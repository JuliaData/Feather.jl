using Feather, Test, CategoricalArrays
using DataFrames, Arrow, Tables
using Random, Dates

const ≅ = isequal

# whether or not to do python round-trip tests
# currently does not work in travis
const DO_PYTHON_ROUNDTRIP = false
const PYTHON_EXEC = "python3"

const testdir = joinpath(dirname(pathof(Feather)), "../test/data")
files = map(x -> joinpath(testdir, x), readdir(testdir))

temps = []

@testset "ReadWrite" for f in files
    @info("testing $f...")
    source = Feather.Source(f)
    df = DataFrame(source)
    temp = tempname()
    push!(temps, temp)
    sink = Feather.write(temp, df)
    df2 = Feather.read(temp)

    for (c1,c2) ∈ zip(getfield(df, :columns), getfield(df, :columns))
        for i = 1:length(c1)
            @test isequal(c1[i], c2[i])
        end
    end

    source2 = Feather.Source(sink)
    @test source.ctable.description == source2.ctable.description
    @test source.ctable.num_rows == source2.ctable.num_rows
    @test source.ctable.metadata == source2.ctable.metadata
    for (col1,col2) in zip(source.ctable.columns,source2.ctable.columns)
        @test col1.name == col2.name
        @test col1.metadata_type == col2.metadata_type
        @test typeof(col1.metadata) == typeof(col2.metadata)
        @test col1.user_metadata == col2.user_metadata

        v1 = col1.values; v2 = col2.values
        @test v1.dtype == v2.dtype
        @test v1.encoding == v2.encoding
        # @test v1.offset == v2.offset # currently not python/R compatible due to wesm/feather#182
        @test v1.length == v2.length
        @test v1.null_count == v2.null_count
        # @test v1.total_bytes == v2.total_bytes
    end
end

GC.gc(); GC.gc()
for t in temps
    rm(t)
end

const Nrows = 10

@testset "Buffer" begin
    io = IOBuffer()
    df = DataFrame(A=rand(Nrows), B=map(randstring, 1:Nrows))
    Feather.write(io, df)
    seekstart(io)
    df2 = Feather.read(io)
    for col ∈ names(df)
        @test df[col] == df2[col]
    end
end

include("arrowtests.jl")

GC.gc(); GC.gc()

@testset "issue#34" begin
    data = DataFrame(A=Union{Missing, String}[randstring(10) for i ∈ 1:100], B=rand(100))
    data[2, :A] = missing
    Feather.write("testfile.feather", data)
    dfo = Feather.read("testfile.feather")
    @test size(dfo) == (100, 2)
    GC.gc();
    rm("testfile.feather")
end

@testset "issue#124" begin
    df = DataFrame(A=rand(5), B=missings(5))
    @test_throws ArgumentError Feather.write("test124.feather", df)
    isfile("test124.feather") && rm("test124.feather")
end

if DO_PYTHON_ROUNDTRIP

@testset "PythonRoundtrip" begin
    try
        @info("Generating test.feather file from python...")
        run(`$PYTHON_EXEC runtests.py`)

        @info("Reading test.feather into Julia...")
        df = Feather.read("test.feather")
    
        dts = [Dates.DateTime(2016,1,1), Dates.DateTime(2016,1,2), Dates.DateTime(2016,1,3)]

        @test df[:Autf8][:] == ["hey","there","sailor"]
        @test df[:Abool][:] == [true, true, false]
        @test df[:Acat][:] == categorical(["a","b","c"])  # these violate Arrow standard by using Int8!!
        @test df[:Acatordered][:] == categorical(["d","e","f"])  # these violate Arrow standard by using Int8!!
        @test convert(Vector{Dates.DateTime}, df[:Adatetime][:]) == dts
        @test isequal(df[:Afloat32][:], [1.0, missing, 0.0])
        @test df[:Afloat64][:] == [Inf,1.0,0.0]
    
        df_ = Feather.read("test.feather"; use_mmap=false)
    
        @info("Writing test2.feather from Julia...")
        Feather.write("test2.feather", df)
        df2 = Feather.read("test2.feather")
    
        @test df2[:Autf8][:] == ["hey","there","sailor"]
        @test df2[:Abool][:] == [true, true, false]
        @test df2[:Acat][:] == categorical(["a","b","c"])  # these violate Arrow standard by using Int8!!
        @test df2[:Acatordered][:] == categorical(["d","e","f"])  # these violate Arrow standard by using Int8!!
        @test convert(Vector{Dates.DateTime}, df2[:Adatetime][:]) == dts
        @test isequal(df2[:Afloat32][:], [1.0, missing, 0.0])
        @test df2[:Afloat64][:] == [Inf,1.0,0.0]

        @info("Read test2.feather into Python...")
        @test (run(`$PYTHON_EXEC runtests2.py`); true)
    finally
        isfile("test.feather") && rm("test.feather")
        isfile("test2.feather") && rm("test2.feather")
    end
end

end
