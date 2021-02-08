#include "tree-cache.hh"
#include "sqlite.hh"
#include "store-api.hh"
#include "context.hh"

namespace nix::tree_cache {

static const char * schema = R"sql(
create table if not exists Attributes (
    id          integer primary key autoincrement not null,
    parent      integer not null,
    name        text,
    type        integer not null,
    value       text,
    context     text,
    unique      (parent, name)
);

create index if not exists IndexByParent on Attributes(parent, name);
)sql";

struct AttrDb
{
    std::atomic_bool failed{false};

    struct State
    {
        SQLite db;
        SQLiteStmt insertAttribute;
        SQLiteStmt insertAttributeWithContext;
        SQLiteStmt queryAttribute;
        SQLiteStmt queryAttributes;
        std::unique_ptr<SQLiteTxn> txn;
    };

    std::unique_ptr<Sync<State>> _state;

    AttrDb(const Hash & fingerprint)
        : _state(std::make_unique<Sync<State>>())
    {
        auto state(_state->lock());

        Path cacheDir = getCacheDir() + "/nix/eval-cache-v2";
        createDirs(cacheDir);

        Path dbPath = cacheDir + "/" + fingerprint.to_string(Base16, false) + ".sqlite";

        state->db = SQLite(dbPath);
        state->db.isCache();
        state->db.exec(schema);

        state->insertAttribute.create(state->db,
            "insert into Attributes(parent, name, type, value) values (?, ?, ?, ?)");

        state->insertAttributeWithContext.create(state->db,
            "insert into Attributes(parent, name, type, value, context) values (?, ?, ?, ?, ?)");

        state->queryAttribute.create(state->db,
            "select id, type, value, context from Attributes where parent = ? and name = ?");

        state->queryAttributes.create(state->db,
            "select name from Attributes where parent = ?");

        state->txn = std::make_unique<SQLiteTxn>(state->db);
    }

    ~AttrDb()
    {
        try {
            auto state(_state->lock());
            if (!failed)
                state->txn->commit();
            state->txn.reset();
        } catch (...) {
            ignoreException();
        }
    }

    template<typename F>
    AttrId doSQLite(F && fun)
    {
        if (failed) return 0;
        try {
            return fun();
        } catch (SQLiteError &) {
            ignoreException();
            failed = true;
            return 0;
        }
    }

    /**
     * Store a leaf of the tree in the db
     */
    AttrId setLeaf(
        const AttrKey & key,
        const AttrValue & value)
    {
        assert(!std::holds_alternative<std::vector<Symbol>>(value));
        return doSQLite([&]()
        {
            auto state(_state->lock());
            auto rawValue = RawValue::fromVariant(value);

            state->insertAttributeWithContext.use()
                (key.first)
                (key.second)
                (rawValue.type)
                (rawValue.value.value_or(""), rawValue.value.has_value())
                (rawValue.serializeContext())
                .exec();
            AttrId rowId = state->db.getLastInsertedRowId();
            assert(rowId);
            return rowId;
        });
    }

    AttrId setAttrs(
        AttrKey key,
        const std::vector<Symbol> & attrs)
    {
        return doSQLite([&]()
        {
            auto state(_state->lock());

            state->insertAttribute.use()
                (key.first)
                (key.second)
                (AttrType::FullAttrs)
                (0, false).exec();

            AttrId rowId = state->db.getLastInsertedRowId();
            assert(rowId);

            for (auto & attr : attrs)
                state->insertAttribute.use()
                    (rowId)
                    (attr)
                    (AttrType::Placeholder)
                    (0, false).exec();

            return rowId;
        });
    }

    AttrId setValue(
        const AttrKey & key,
        const AttrValue & value)
    {
        if (auto attrs = std::get_if<std::vector<Symbol>>(&value))
            return setAttrs(key, *attrs);
        return setLeaf(key, value);
    }

    AttrId setBool(
        AttrKey key,
        bool b)
    {
        return setLeaf(key, b);
    }

    AttrId setPlaceholder(AttrKey key)
    {
        return setLeaf(key, placeholder_t{});
    }

    AttrId setMissing(AttrKey key)
    {
        return setLeaf(key, missing_t{});
    }

    AttrId setMisc(AttrKey key)
    {
        return setLeaf(key, misc_t{});
    }

    AttrId setFailed(AttrKey key)
    {
        return setLeaf(key, failed_t{});
    }

    std::optional<AttrId> getId(const AttrKey& key)
    {
        auto state(_state->lock());

        auto queryAttribute(state->queryAttribute.use()(key.first)(key.second));
        if (!queryAttribute.next()) return {};

        return (AttrType) queryAttribute.getInt(0);
    }

    AttrId setIfAbsent(const AttrKey& key, const AttrValue& value)
    {
        if (auto existingId = getId(key))
            return *existingId;
        return setValue(key, value);
    }

    std::optional<std::pair<AttrId, AttrValue>> getAttr(
        AttrKey key,
        SymbolTable & symbols)
    {
        auto state(_state->lock());

        auto queryAttribute(state->queryAttribute.use()(key.first)(key.second));
        if (!queryAttribute.next()) return {};

        auto rowId = (AttrType) queryAttribute.getInt(0);
        auto type = (AttrType) queryAttribute.getInt(1);

        switch (type) {
            case AttrType::Placeholder:
                return {{rowId, placeholder_t()}};
            case AttrType::FullAttrs: {
                // FIXME: expensive, should separate this out.
                std::vector<Symbol> attrs;
                auto queryAttributes(state->queryAttributes.use()(rowId));
                while (queryAttributes.next())
                    attrs.push_back(symbols.create(queryAttributes.getStr(0)));
                return {{rowId, attrs}};
            }
            case AttrType::String: {
                std::vector<std::pair<Path, std::string>> context;
                if (!queryAttribute.isNull(3))
                    for (auto & s : tokenizeString<std::vector<std::string>>(queryAttribute.getStr(3), ";"))
                        context.push_back(decodeContext(s));
                return {{rowId, string_t{queryAttribute.getStr(2), context}}};
            }
            case AttrType::Bool:
                return {{rowId, queryAttribute.getInt(2) != 0}};
            case AttrType::Missing:
                return {{rowId, missing_t()}};
            case AttrType::Misc:
                return {{rowId, misc_t()}};
            case AttrType::Failed:
                return {{rowId, failed_t()}};
            default:
                throw Error("unexpected type in evaluation cache");
        }
    }
};

static std::shared_ptr<AttrDb> makeAttrDb(const Hash & fingerprint)
{
    try {
        return std::make_shared<AttrDb>(fingerprint);
    } catch (SQLiteError &) {
        ignoreException();
        return nullptr;
    }
}

Cache::Cache(std::optional<std::reference_wrapper<const Hash>> useCache,
        SymbolTable & symbols)
    : db(useCache ? makeAttrDb(*useCache) : nullptr)
    , symbols(symbols)
    , rootSymbol(symbols.create(""))
{
}

void Cache::commit()
{
    if (db) {
        debug("Saving the cache");
        auto state(db->_state->lock());
        if (state->txn->active) {
            state->txn->commit();
            state->txn.reset();
            state->txn = std::make_unique<SQLiteTxn>(state->db);
        }
    }
}

std::shared_ptr<Cursor> Cache::getRoot()
{
    return std::make_shared<Cursor>(ref(shared_from_this()), std::nullopt, std::vector<Symbol>{});
}

Cursor::Cursor(
    ref<Cache> root,
    Parent parent,
    const AttrValue& value
    )
    : root(root), parent(parent)
    , cachedValue({root->db->setIfAbsent(getKey(), value), value})
{
}

Cursor::Cursor(
    ref<Cache> root,
    Parent parent,
    const AttrId& id,
    const AttrValue& value
    )
    : root(root), parent(parent)
    , cachedValue({id, value})
{
}


AttrKey Cursor::getKey()
{
    if (!parent)
        return {0, root->rootSymbol};
    return {parent->first->cachedValue.first, parent->second};
}

AttrValue Cursor::getCachedValue()
{
    return cachedValue.second;
}

void Cursor::setValue(const AttrValue & v)
{
    debug("Caching the attribute %s", getKey().second);
    cachedValue = {root->db->setValue(getKey(), v), v};
}

std::shared_ptr<Cursor> Cursor::addChild(const Symbol & attrPath, AttrValue & v)
{
    Parent parent = {{shared_from_this(), attrPath}};
    auto childCursor = std::make_shared<Cursor>(
        root,
        parent,
        v
    );
    return childCursor;
}

std::shared_ptr<Cursor> Cursor::maybeGetAttr(const Symbol & name)
{
    auto rawAttr = root->db->getAttr({cachedValue.first, name}, root->symbols);
    if (rawAttr)
        return std::make_shared<Cursor>(root, std::make_pair(shared_from_this(), name), rawAttr->first, rawAttr->second);
    return nullptr;
}

std::shared_ptr<Cursor> Cursor::findAlongAttrPath(const std::vector<Symbol> & attrPath)
{
    auto currentCursor = shared_from_this();
    for (auto & currentAccessor : attrPath) {
        currentCursor = currentCursor->maybeGetAttr(currentAccessor);
        if (!currentCursor)
            break;
    }
    return currentCursor;
}

const RawValue RawValue::fromVariant(const AttrValue & value)
{
    RawValue res;
    std::visit(overloaded{
      [&](std::vector<Symbol> x) { res.type = AttrType::FullAttrs; },
      [&](placeholder_t x) { res.type = AttrType::Placeholder; },
      [&](missing_t x) { res.type = AttrType::Missing; },
      [&](misc_t x) { res.type = AttrType::Misc;  },
      [&](failed_t x) { res.type = AttrType::Failed;  },
      [&](string_t x) {
        res.type = AttrType::String;
        res.value = x.first;
        res.context = x.second;
      },
      [&](bool x) {
        res.type = AttrType::Bool;
        res.value = x ? "0" : "1";
      }
    }, value);
    return res;
}

std::string RawValue::serializeContext() const
{
    std::string res;
    for (auto & elt : context) {
        res.append(encodeContext(elt.first, elt.second));
        res.push_back(' ');
    }
    if (!res.empty())
        res.pop_back(); // Remove the trailing space
    return res;
}

}
