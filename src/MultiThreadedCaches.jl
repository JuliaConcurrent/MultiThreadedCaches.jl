module MultiThreadedCaches

using Base: @lock

export MultiThreadedCache, init_cache!

struct MultiThreadedCache{K,V}
    thread_caches::Vector{Dict{K,V}}
    base_cache::Dict{K,V}

    function MultiThreadedCache{K,V}() where {K,V}
        base_cache = Dict{K,V}()
        return new(Dict{K,V}[], base_cache)
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
@noinline _thread_cache_length_assert() = @assert false "length(cache.thread_caches) < Threads.nthreads() - Must call `init!(cache)` in Module __init__()."


end # module
