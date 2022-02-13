using MultiThreadedCaches

using Test

@testset "basics" begin

    cache = MultiThreadedCache{Int,Int}()
    init_cache!(cache)

    @test length(cache.thread_caches) == Threads.nthreads()



end

