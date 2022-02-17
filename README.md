# MultiThreadedCaches

`MultiThreadedCache{K,V}()` is a fast-ish, thread-safe cache.

This cache stores k=>v pairs that cache a deterministic computation. The only API into the
cache is `get!()`: you can look up a key, and if it is not available, you can produce a
value which will be added to the cache.

Accesses to the cache will look first in the per-thread cache, and then fall back to the
shared thread-safe cache. Concurrent misses to the same key in the shared cache will
coordinate, so that only one Task will perform the compuatation for that value, and the
other Task(s) will block.

The per-thread caches have very low contention (usually only locked by that single Task), so
a MultiThreadedCache{K,V} scales much better than the naive baseline Dict+ReentrantLock that
you might use instead.

## Alternatives Considered
Some other approaches to concurrent caches include:
- Concurrent Hash Table
- Multithread caches designed for low contention by sharding the **key space**, and keeping
  a separate lock per sub-cache. For example, having an array of e.g. 64 separate caches,
  sharded by a prefix of a key's hash, each with their own lock.
    - This package differs from this design by sharding the hash by Thread ID, rather than
      by the key space.
    - This tradeoff accepts greater data duplication in exchange for hopefully less
      contention.


# Example:
```julia
julia> cache = MultiThreadedCache{Int, Int}(Dict(1=>2, 2=>3))
MultiThreadedCache{Int64, Int64}(Dict(2 => 3, 1 => 2))

julia> init_cache!(cache)
MultiThreadedCache{Int64, Int64}(Dict(2 => 3, 1 => 2))

julia> get!(cache, 2) do
           2+1
       end
3

julia> get!(cache, 5) do  # These accesses are safe from arbitrary threads.
           5+1
       end
6

julia> get!(cache, 5) do
           5+10
       end
6
```
