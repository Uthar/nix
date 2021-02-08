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
    AttrId setValue(
        const AttrKey & key,
        const AttrValue & value)
    {
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

    AttrId setBool(
        AttrKey key,
        bool b)
    {
        return setValue(key, b);
    }

    AttrId setPlaceholder(AttrKey key)
    {
        return setValue(key, placeholder_t{});
    }

    AttrId setMissing(AttrKey key)
    {
        return setValue(key, missing_t{});
    }

    AttrId setMisc(AttrKey key)
    {
        return setValue(key, misc_t{});
    }

    AttrId setFailed(AttrKey key)
    {
        return setValue(key, failed_t{});
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
        if (auto existingId = getId(key)) {
            return *existingId;
            debug("cache: spurious write for the attribute %s", key.second);
        }
        debug("cache: miss for the attribute %s", key.second);
        return setValue(key, value);
    }

    std::optional<std::pair<AttrId, AttrValue>> getValue(AttrKey key)
    {
        auto state(_state->lock());

        auto queryAttribute(state->queryAttribute.use()(key.first)(key.second));
        if (!queryAttribute.next()) return {};

        auto rowId = (AttrType) queryAttribute.getInt(0);
        auto type = (AttrType) queryAttribute.getInt(1);

        switch (type) {
            case AttrType::Placeholder:
                return {{rowId, placeholder_t()}};
            case AttrType::Attrs: {
                return {{rowId, attributeSet_t()}};
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

Cache::Cache(const Hash & useCache,
        SymbolTable & symbols)
    : db(std::make_shared<AttrDb>(useCache))
    , symbols(symbols)
    , rootSymbol(symbols.create(""))
{
}

std::shared_ptr<Cache> Cache::tryCreate(const Hash & useCache, SymbolTable & symbols)
{
    try {
        return std::make_shared<Cache>(useCache, symbols);
    } catch (SQLiteError &) {
        ignoreException();
        return nullptr;
    }
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

Cursor::Ref Cache::getRoot()
{
    return new Cursor(ref(shared_from_this()), std::nullopt, attributeSet_t{});
}

Cursor::Cursor(
    ref<Cache> root,
    const Parent & parent,
    const AttrValue& value
    )
    : root(root)
    , parentId(parent ? std::optional{parent->first.cachedValue.first} : std::nullopt)
    , label(parent ? parent->second : root->rootSymbol)
    , cachedValue({root->db->setIfAbsent(getKey(), value), value})
{
}

Cursor::Cursor(
    ref<Cache> root,
    const Parent & parent,
    const AttrId & id,
    const AttrValue & value
    )
    : root(root)
    , parentId(parent ? std::optional{parent->first.cachedValue.first} : std::nullopt)
    , label(parent ? parent->second : root->rootSymbol)
    , cachedValue({id, value})
{
}


AttrKey Cursor::getKey()
{
    if (!parentId)
        return {0, root->rootSymbol};
    return {*parentId, label};
}

AttrValue Cursor::getCachedValue()
{
    return cachedValue.second;
}

void Cursor::setValue(const AttrValue & v)
{
    cachedValue = {root->db->setValue(getKey(), v), v};
}

Cursor::Ref Cursor::addChild(const Symbol & attrPath, AttrValue & v)
{
    Parent parent = {{*this, attrPath}};
    auto childCursor = new Cursor(
        root,
        parent,
        v
    );
    return childCursor;
}

Cursor::Ref Cursor::maybeGetAttr(const Symbol & name)
{
    auto rawAttr = root->db->getValue({cachedValue.first, name});
    if (rawAttr) {
        Parent parent = {{*this, name}};
        debug("cache: hit for the attribute %s", cachedValue.first);
        return new Cursor (
            root, parent, rawAttr->first,
            rawAttr->second);
    }
    return nullptr;
}

Cursor::Ref Cursor::findAlongAttrPath(const std::vector<Symbol> & attrPath)
{
    auto currentCursor = this;
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
      [&](attributeSet_t x) { res.type = AttrType::Attrs; },
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
        res.append(encodeContext(elt.second, elt.first));
        res.push_back(' ');
    }
    if (!res.empty())
        res.pop_back(); // Remove the trailing space
    return res;
}

}
