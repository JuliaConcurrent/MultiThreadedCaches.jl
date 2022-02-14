module MultiThreadedCaches

using Base: @lock

export MultiThreadedCache, init_cache!

"""
    MultiThreadedCache{K,V}()
    MultiThreadedCache{K,V}([initial_kv_values])

`MultiThreadedCache{K,V}()` constructs a fast, thread-safe cache.

This cache stores k=>v pairs that cache a deterministic computation. The only API into the
cache is `get!()`: you can look up a key, and if it is not available, you can produce a
value which will be added to the cache.

Accesses to the cache will look first in the per-thread cache, and then fall back to the
shared thread-safe cache. Concurrent misses to the same key in the shared cache will
coordinate, so that only one Task will perform the compuatation for that value, and the
other Task(s) will block.

# Examples:
```julia
julia> cache = MultiThreadedCache{Int, Int}(Dict(1=>2, 2=>3))
MultiThreadedCache{Int64, Int64}(Dict(2 => 3, 1 => 2))

julia> init_cache!(cache)
MultiThreadedCache{Int64, Int64}(Dict(2 => 3, 1 => 2))

julia> get!(cache, 2) do
           2+1
       end
3

julia> get!(cache, 5) do
           5+1
       end
6

julia> get!(cache, 5) do
           5+10
       end
6
```
"""
struct MultiThreadedCache{K,V}
    thread_caches::Vector{Dict{K,V}}

    base_cache::Dict{K,V}  # Guarded by: base_cache_lock
    base_cache_lock::ReentrantLock
    base_cache_futures::Dict{K,Channel{V}}  # Guarded by: base_cache_lock

    function MultiThreadedCache{K,V}() where {K,V}
        base_cache = Dict{K,V}()

        return MultiThreadedCache{K,V}(base_cache)
    end

    function MultiThreadedCache{K,V}(base_cache::Dict) where {K,V}
        thread_caches = Dict{K,V}[]

        base_cache_lock = ReentrantLock()
        base_cache_futures = Dict{K,Channel{V}}()

        return new(thread_caches, base_cache, base_cache_lock, base_cache_futures)
    end
end

"""
    init_cache!(cache::MultiThreadedCache{K,V})

This function must be called inside a Module's `__init__()` method, to ensure that the
number of threads are set *at runtime, not precompilation time*.

!!! note
NOTE: This function is *not thread safe*, it must not be called concurrently with any other
code that touches the cache. This should only be called once, during module initialization.
"""
function init_cache!(cache::MultiThreadedCache{K,V}) where {K,V}
    append!(empty!(cache.thread_caches),
        # We copy the base cache to all the thread caches, so that any precomputed values
        # can be shared without having to wait for a cache miss.
        Dict{K,V}[copy(cache.base_cache) for _ in 1:Threads.nthreads()])
    return cache
end

function Base.show(io::IO, cache::MultiThreadedCache{K,V}) where {K,V}
    print(io, "$(MultiThreadedCache{K,V})(", cache.base_cache, ")")
end

# Based upon the thread-safe Global RNG implementation in the Random stdlib:
# https://github.com/JuliaLang/julia/blob/e4fcdf5b04fd9751ce48b0afc700330475b42443/stdlib/Random/src/RNGs.jl#L369-L385
# Get or lazily construct the per-thread cache when first requested.
function _thread_cache(cache::MultiThreadedCache)
    length(cache.thread_caches) >= Threads.nthreads() || _thread_cache_length_assert()
    tid = Threads.threadid()
    if @inbounds isassigned(cache.thread_caches, tid)
        @inbounds cache = cache.thread_caches[tid]
    else
        cache = eltype(cache.thread_caches)()
        @inbounds cache.thread_caches[tid] = cache
    end
    return cache
end
@noinline _thread_cache_length_assert() = @assert false "** Must call `init_cache!(cache)` in your Module's __init__()! - length(cache.thread_caches) < Threads.nthreads() "


const CACHE_MISS = :__MultiThreadedCaches_key_not_found__

function Base.get!(func::Base.Callable, cache::MultiThreadedCache{K,V}, key) where {K,V}
    # If the thread-local cache has the value, we can return immediately.
    # If not, we need to check the base cache.
    return get!(_thread_cache(cache), key) do
        # Even though we're using Thread-local caches, we still need to lock during
        # construction to prevent multiple tasks redundantly constructing the same object,
        # and potential thread safety violations due to Tasks migrating threads.
        # NOTE that we only grab the lock if the key doesn't exist, so the mutex contention
        # is not on the critical path for most accessses. :)
        is_first_task = false
        local future  # used only if the base_cache doesn't have the key
        # We lock the mutex, but for only a short, *constant time* duration, to grab the
        # future for this key, or to create the future if it doesn't exist.
        @lock cache.base_cache_lock begin
            value_or_miss = get(cache.base_cache, key, CACHE_MISS)
            if value_or_miss !== CACHE_MISS
                return value_or_miss::V
            end
            future = get!(cache.base_cache_futures, key) do
                is_first_task = true
                Channel{V}(1)
            end
        end
        if is_first_task
            v = func()
            # Finally, lock again for a *constant time* to insert the computed value into
            # the shared cache, so that we can free the Channel and future gets can read
            # from the shared base_cache.
            @lock cache.base_cache_lock begin
                cache.base_cache[key] = v
                # We no longer need the Future, since all future requests will see the key
                # in the base_cache. (Other Tasks may still hold a reference, but it will
                # be GC'd once they have all completed.)
                delete!(cache.base_cache_futures, key)
            end
            # Return v to any other Tasks that were blocking on this key.
            put!(future, v)
            return v
        else
            # Block on the future until the first task that asked for this key finishes
            # computing a value for it.
            return fetch(future)
        end
    end
end


end # module
