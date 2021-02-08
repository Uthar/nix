/**
 * caching for a tree-like data structure (like Nix values)
 *
 * The cache is an sqlite db whose rows are the nodes of the tree, with a
 * pointer to their parent (except for the root of course)
 */

#pragma once

#include "sync.hh"
#include "hash.hh"
#include "symbol-table.hh"

#include <functional>
#include <variant>

namespace nix::tree_cache {

struct AttrDb;
class Cursor;

class Cache : public std::enable_shared_from_this<Cache>
{
private:
    friend class Cursor;

    /**
     * The database holding the cache
     */
    std::shared_ptr<AttrDb> db;

    SymbolTable & symbols;

    /**
     * Distinguished symbol indicating the root of the tree
     */
    const Symbol rootSymbol;

public:

    Cache(
        std::optional<std::reference_wrapper<const Hash>> useCache,
        SymbolTable & symbols
    );

    std::shared_ptr<Cursor> getRoot();

    /**
     * Flush the cache to disk
     */
    void commit();
};

enum AttrType {
    Placeholder = 0,
    FullAttrs = 1,
    String = 2,
    Missing = 3,
    Misc = 4,
    Failed = 5,
    Bool = 6,
};

struct placeholder_t {};
struct missing_t {};
struct misc_t {};
struct failed_t {};
typedef uint64_t AttrId;

typedef std::pair<AttrId, Symbol> AttrKey;
typedef std::pair<std::string, std::vector<std::pair<Path, std::string>>> string_t;

typedef std::variant<
    std::vector<Symbol>,
    string_t,
    placeholder_t, // incomplete attribute set?
    missing_t,
    misc_t,
    failed_t,
    bool
> AttrValue;

struct RawValue {
    AttrType type;
    std::optional<std::string> value;
    std::vector<std::pair<Path, std::string>> context;

    std::string serializeContext() const;

    static const RawValue fromVariant(const AttrValue&);
    AttrValue toVariant() const;
};

/**
 * View inside the cache.
 *
 * A `Cursor` represents a node in the cached tree (be it a leaf or not)
 */
class Cursor : public std::enable_shared_from_this<Cursor>
{
    /**
     * The overall cache of which this cursor is a view
     */
    ref<Cache> root;

    typedef std::optional<std::pair<std::shared_ptr<Cursor>, Symbol>> Parent;
    Parent parent;

    std::pair<AttrId, AttrValue> cachedValue;

    /**
     * Get the identifier for this node in the database
     */
    AttrKey getKey();

public:
    // Create a new cache entry
    Cursor(ref<Cache> root, Parent parent, const AttrValue&);
    // Build a cursor from an existing cache entry
    Cursor(ref<Cache> root, Parent parent, const AttrId& id, const AttrValue&);

    AttrValue getCachedValue();

    void setValue(const AttrValue & v);

    std::shared_ptr<Cursor> addChild(const Symbol & attrPath, AttrValue & v);

    std::shared_ptr<Cursor> findAlongAttrPath(const std::vector<Symbol> & attrPath);
    std::shared_ptr<Cursor> maybeGetAttr(const Symbol & attrPath);
};

}
