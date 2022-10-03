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
    thread_locks::Vector{ReentrantLock}

    base_cache::Dict{K,V}  # Guarded by: base_cache_lock
    base_cache_lock::ReentrantLock
    base_cache_futures::Dict{K,Channel{V}}  # Guarded by: base_cache_lock

    function MultiThreadedCache{K,V}() where {K,V}
        base_cache = Dict{K,V}()

        return MultiThreadedCache{K,V}(base_cache)
    end

    function MultiThreadedCache{K,V}(base_cache::Dict) where {K,V}
        thread_caches = Dict{K,V}[]
        thread_locks = ReentrantLock[]

        base_cache_lock = ReentrantLock()
        base_cache_futures = Dict{K,Channel{V}}()

        return new(thread_caches, thread_locks, base_cache, base_cache_lock, base_cache_futures)
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
    # Statically resize the vector, but wait to lazily create the dictionaries when
    # requested, so that the object will be allocated on the thread that will consume it.
    # (This follows the guidance from Julia Base.)
    resize!(cache.thread_caches, Threads.nthreads())
    resize!(cache.thread_locks, Threads.nthreads())
    return cache
end

# Based upon the thread-safe Global RNG implementation in the Random stdlib:
# https://github.com/JuliaLang/julia/blob/e4fcdf5b04fd9751ce48b0afc700330475b42443/stdlib/Random/src/RNGs.jl#L369-L385
# Get or lazily construct the per-thread cache when first requested.
function _thread_cache(mtcache::MultiThreadedCache, tid)
    length(mtcache.thread_caches) >= Threads.nthreads() || _thread_cache_length_assert()
    if @inbounds isassigned(mtcache.thread_caches, tid)
        @inbounds cache = mtcache.thread_caches[tid]
    else
        # We copy the base cache to all the thread caches, so that any precomputed values
        # can be shared without having to wait for a cache miss.
        cache = Base.@lock mtcache.base_cache_lock begin
            copy(mtcache.base_cache)
        end
        @inbounds mtcache.thread_caches[tid] = cache
    end
    return cache
end
@noinline _thread_cache_length_assert() = @assert false "** Must call `init_cache!(cache)` in your Module's __init__()! - length(cache.thread_caches) < Threads.nthreads() "
function _thread_lock(cache::MultiThreadedCache, tid)
    length(cache.thread_locks) >= Threads.nthreads() || _thread_cache_length_assert()
    if @inbounds isassigned(cache.thread_locks, tid)
        @inbounds lock = cache.thread_locks[tid]
    else
        lock = eltype(cache.thread_locks)()
        @inbounds cache.thread_locks[tid] = lock
    end
    return lock
end


const CACHE_MISS = :__MultiThreadedCaches_key_not_found__

function Base.get!(func, cache::MultiThreadedCache{K,V}, key) where {K,V}
    # If the thread-local cache has the value, we can return immediately.
    # We store tcache in a local variable, so that even if the Task migrates Threads, we are
    # still operating on the same initial cache object.
    tid = Threads.threadid()
    tcache = _thread_cache(cache, tid)
    tlock = _thread_lock(cache, tid)
    # We have to lock during access to the thread-local dict, because it's possible that the
    # Task may migrate to another thread by the end, and we really might be mutating the
    # dict in parallel. But most of the time this lock should have 0 contention, since it's
    # only held during get() and set!().
    Base.@lock tlock begin
        thread_local_cached_value_or_miss = get(tcache, key, CACHE_MISS)
        if thread_local_cached_value_or_miss !== CACHE_MISS
            return thread_local_cached_value_or_miss::V
        end
    end
    # If not, we need to check the base cache.

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
        future_or_miss = get(cache.base_cache_futures, key, CACHE_MISS)
        future = if future_or_miss === CACHE_MISS
            is_first_task = true
            ch = Channel{V}(1)
            cache.base_cache_futures[key] = ch
            ch::Channel{V}
        else
            future_or_miss::Channel{V}
        end
    end
    if is_first_task
        v = try
            func()
        catch e
            # In the case of an exception, we abort the current computation of this
            # key/value pair, and throw the exception onto the future, so that all
            # pending tasks will see the exeption as well.
            #
            # NOTE: we could also cache the exception and throw it from now on, but this
            # would make interactive development difficult, since once you fix the
            # error, you'd have to clear out your cache. So instead, we just rethrow the
            # exception and don't cache anything, so that you can fix the exception and
            # continue on. (This means that throwing exceptions remains expensive.)

            # close(::Channel, ::Exception) requires an Exception object, so if the user
            # threw a non-Exception, we convert it to one, here.
            e isa Exception || (e = ErrorException("Non-exception object thrown during get!(): $e"))
            close(future, e)
            # As below, the future isn't needed after this returns (see below).
            @lock cache.base_cache_lock begin
                delete!(cache.base_cache_futures, key)
            end
            rethrow(e)
        end
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
        # Store the result in this thread-local dictionary.
        _store_result!(tcache, tlock, key, v, test_haskey=false)
        # Return v to any other Tasks that were blocking on this key.
        put!(future, v)
        return v
    else
        # Block on the future until the first task that asked for this key finishes
        # computing a value for it.
        v = fetch(future)
        # Store the result in our original thread-local cache (if another Task hasn't) set
        # it already.
        _store_result!(tcache, tlock, key, v, test_haskey=true)
        return v
    end
end

# Set the value into thread-local cache for the supplied key.
@inline function _store_result!(tcache, tlock, key, v; test_haskey::Bool)
    # Note that we must perform two separate get() and setindex!() calls, for
    # concurrency-safety, in case the dict has been mutated by another task in between.
    # TODO: For 100% concurrency-safety, we maybe want to lock around the get() above
    # and the setindex!() here.. it's probably fine without it, but needs considering.
    # Currently this is relying on get() and setindex!() having no yields.
    Base.@lock tlock begin
        if test_haskey
            if !haskey(tcache, key)
                tcache[key] = v
            end
        else
            tcache[key] = v
        end
    end
end

function Base.show(io::IO, cache::MultiThreadedCache{K,V}) where {K,V}
    # Contention optimization: don't hold the lock while printing, since that could block
    # for an arbitrarily long time. Instead, print the data to an intermediate buffer first.
    # Note that this has the same CPU complexity, since printing is already O(n).
    iobuf = IOBuffer()
    let io = IOContext(iobuf, io)
        Base.@lock cache.base_cache_lock begin
            _oneline_show(io, cache)
        end
    end
    # Now print the data without holding the lock.
    seekstart(iobuf)
    write(io, read(iobuf))
    return nothing
end
_oneline_show(io::IO, cache::MultiThreadedCache{K,V}) where {K,V} =
    print(io, "$(MultiThreadedCache{K,V})(", cache.base_cache, ")")

function Base.show(io::IO, mime::MIME"text/plain", cache::MultiThreadedCache{K,V}) where {K,V}
    # Contention optimization: don't hold the lock while printing. See above for more info.
    iobuf = IOBuffer()
    let io = IOContext(iobuf, io)
        Base.@lock cache.base_cache_lock begin
            if isempty(cache.base_cache)
                _oneline_show(io, cache)
            else
                print(io, "$(MultiThreadedCache): ")
                Base.show(io, mime, cache.base_cache)
            end
        end
    end
    # Now print the data without holding the lock.
    seekstart(iobuf)
    write(io, read(iobuf))
    return nothing
end

end # module
