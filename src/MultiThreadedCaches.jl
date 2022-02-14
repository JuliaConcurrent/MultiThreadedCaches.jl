module MultiThreadedCaches

using Base: @lock

export MultiThreadedCache, init_cache!

struct MultiThreadedCache{K,V}
    thread_caches::Vector{Dict{K,V}}

    base_cache::Dict{K,V}  # Guarded by: base_cache_lock
    base_cache_lock::ReentrantLock
    base_cache_futures::Dict{K,Channel{V}}  # Guarded by: base_cache_lock

    function MultiThreadedCache{K,V}() where {K,V}
        thread_caches = Dict{K,V}[]

        base_cache = Dict{K,V}()
        base_cache_lock = ReentrantLock()
        base_cache_futures = Dict{K,Channel{V}}()

        return new(thread_caches, base_cache, base_cache_lock, base_cache_futures)
    end
end

"""
    init_cache!(cache::MultiThreadedCache{K,V})

This function must be called inside a Module's `__init__()` method, to ensure that the
number of threads are set *at runtime, not precompilation time*.
"""
function init_cache!(cache::MultiThreadedCache{K,V}) where {K,V}
    append!(empty!(cache.thread_caches),
        Dict{K,V}[Dict{K,V}() for _ in 1:Threads.nthreads()])
    return nothing
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
@noinline _thread_cache_length_assert() = @assert false "length(cache.thread_caches) < Threads.nthreads() - Must call `init_cache!(cache)` in Module __init__()."


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
