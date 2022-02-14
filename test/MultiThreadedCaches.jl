using MultiThreadedCaches

using Test

@info "Testing with:" Threads.nthreads()

@testset "basics" begin
    cache = MultiThreadedCache{Int,Int}()
    init_cache!(cache)

    @test length(cache.thread_caches) == Threads.nthreads()

    get!(cache, 1) do
        return 10
    end

    # The second get!() shouldn't have any effect, and the result should still be 10.
    @test get!(cache, 1) do
        return 100
    end == 10

    # Even on different threads, the cache should still have a value for key `1`.
    @sync for i in 1:100
        Threads.@spawn begin
            @test get!(cache, 1) do
                return 100
            end == 10
        end
    end

    # Internals test:
    @test length(cache.base_cache_futures) == 0
end

# Helper function for stress-test: returns true if all elements in iterable `v` are equal.
function all_equal(v)
    length(v) < 2 && return true
    e1 = first(v)
    i = 2
    @inbounds for i=2:length(v)
        v[i] == e1 || return false
    end
    return true
end

@testset "stress-test" begin
    cache = MultiThreadedCache{Int,Int}()
    init_cache!(cache)

    len = 100
    outputs = [Channel{Int}(Inf) for _ in 1:len]

    @sync for i in 1:100_000
        Threads.@spawn begin
            key = rand(1:len)
            value = get!(cache, len) do
                # Randomize a value for (key,value) the *first time* it's requested
                rand(Int)
            end
            # Every subsequent get!() on the cache should see the exact same value,
            # even if requested in parallel from multiple threads.
            put!(outputs[key], value)
        end
    end

    for ch in outputs
        close(ch)
        @test all_equal(collect(ch))
    end

    # Internals test:
    @test length(cache.base_cache_futures) == 0
end

@testset "constructors" begin
    # Constructing with precomputed cache key/value pairs:
    cache = MultiThreadedCache{Int64, Int64}(Dict(2 => 3, 1 => 2))
    init_cache!(cache)

    # Existing keys use the cached value:
    @test get!(()->10, cache, 1) == 2
    @test get!(()->10, cache, 2) == 3
    # New keys use the newly provided value:
    @test get!(()->10, cache, 3) == 10
end



