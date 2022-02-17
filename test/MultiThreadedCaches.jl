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
    @sync for _ in 1:100
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

    @sync for _ in 1:100_000
        Threads.@spawn begin
            key = rand(1:len)
            value = get!(cache, key) do
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

@testset "exceptions" begin
    cache = MultiThreadedCache{Int64, Int64}()
    init_cache!(cache)

    # Test that if the construction Task throws an exception, all Tasks see that exception:
    exceptions = Channel(Inf)
    @sync for i in 1:1000
        Threads.@spawn begin
            try
                get!(cache, 1) do
                    sleep(1)  # sleep, to give all Tasks time to spin up
                    throw(ArgumentError("$i"))
                end
            catch e
                @test e isa ArgumentError
                put!(exceptions, e)
            end
        end
    end

    # Test that all Tasks saw the same exception thrown.
    close(exceptions)
    @test all_equal(collect(exceptions))

    # Test that after throwing an exception during a get! function, the cache is still
    # robust and working as intended.

    @test get!(()->10, cache, 1) == 10
    @test get!(()->20, cache, 1) == 10

    # Internals test:
    # (despite the exception, all futures are closed, and the base cache only contains
    # expected values.)
    @test length(cache.base_cache_futures) == 0
    @test cache.base_cache == Dict(1=>10)
end

@testset "show" begin
    cache = MultiThreadedCache{Int64, Int64}()
    # Exercise both show functions
    show(cache)
    display(cache)
end







# ---------------------------------------------------------------
# What follows is a benchmark, testing that scaling the number of threads (increasing
# contention on the cache) doesn't negatively affect performance:

@testset "perf-benchmark" begin
    cache = MultiThreadedCache{Int,Int}()
    init_cache!(cache)

    len = 100
    outputs = [Channel{Int}(Inf) for _ in 1:len]

    run_serial(N) = @sync for _ in 1:N
        begin
            key = rand(1:len)
            value = get!(cache, key) do
                # Randomize a value for (key,value) the *first time* it's requested
                rand(Int)
            end
            # Every subsequent get!() on the cache should see the exact same value,
            # even if requested in parallel from multiple threads.
            put!(outputs[key], value)
        end
    end

    run_parallel(N) = @sync for _ in 1:N
        Threads.@spawn begin
            key = rand(1:len)
            value = get!(cache, key) do
                # Randomize a value for (key,value) the *first time* it's requested
                rand(Int)
            end
            # Every subsequent get!() on the cache should see the exact same value,
            # even if requested in parallel from multiple threads.
            put!(outputs[key], value)
        end
    end

    # Measure a baseline against a dict with a lock
    mutex = ReentrantLock()
    dict = Dict{Int,Int}()
    baseline_outputs = [Channel{Int}(Inf) for _ in 1:len]
    run_baseline(N) = @sync for _ in 1:N
        Threads.@spawn begin
            key = rand(1:len)
            Base.@lock mutex begin
                value = get!(dict, key) do
                    rand(Int)
                end
                put!(baseline_outputs[key], value)
            end
        end
    end

    # warmup
    run_serial(10); run_parallel(10); run_baseline(10)

    # measurement
    n = 100_000
    time_serial = @elapsed run_serial(n)
    time_parallel = @elapsed run_parallel(n)
    time_baseline = @elapsed run_baseline(n)

    for ch in outputs
        close(ch)
        @test all_equal(collect(ch))
    end
    for ch in baseline_outputs
        close(ch)
        @test all_equal(collect(ch))
    end

    # Performance test:
    @info "benchmark results" Threads.nthreads() time_serial time_parallel time_baseline
end


