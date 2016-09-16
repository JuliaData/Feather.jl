using Feather, DataFrames, Base.Test, NullableArrays

if VERSION < v"0.5.0-dev+4267"
    @eval is_windows() = $(OS_NAME == :Windows)
end

testdir = joinpath(dirname(@__FILE__), "data")
# testdir = joinpath(Pkg.dir("Feather"), "test/data")
files = map(x -> joinpath(testdir, x), readdir(testdir))

testnull{T}(v1::T, v2::T) = v1 == v2
testnull{T}(v1::Nullable{T}, v2::Nullable{T}) = (isnull(v1) && isnull(v2)) || (!isnull(v1) && !isnull(v2) && get(v1) == get(v2))
testnull{T}(v1::T, v2::Nullable{T}) = !isnull(v2) && get(v2) == v1
testnull{T}(v1::Nullable{T}, v2::T) = !isnull(v1) && get(v1) == v2

for f in files
    source = Feather.Source(f)
    df = Data.stream!(source, DataFrame)
    temp = tempname()
    sink = Feather.Sink(temp)
    Feather.Data.stream!(df, sink)
    df2 = Feather.read(temp)

    for (c1,c2) in zip(df.columns,df2.columns)
        for i = 1:length(c1)
            @test testnull(c1[i], c2[i])
        end
    end
    @test Data.header(source) == Data.header(sink) && Data.types(source) == Data.types(sink)
    @test source.ctable.description == sink.ctable.description
    @test source.ctable.num_rows == sink.ctable.num_rows
    @test source.ctable.metadata == sink.ctable.metadata
    for (col1,col2) in zip(source.ctable.columns,sink.ctable.columns)
        @test col1.name == col2.name
        @test col1.metadata_type == col2.metadata_type
        @test typeof(col1.metadata) == typeof(col2.metadata)
        @test col1.user_metadata == col2.user_metadata

        v1 = col1.values; v2 = col2.values
        @test v1.type_ == v2.type_
        @test v1.encoding == v2.encoding
        # @test v1.offset == v2.offset # currently not python/R compatible due to wesm/feather#182
        @test v1.length == v2.length
        @test v1.null_count == v2.null_count
        # @test v1.total_bytes == v2.total_bytes
    end
    rm(temp)
end

# DataStreams interface
source_file = joinpath(testdir, "test_utf8.feather")
sink_file = joinpath(testdir, "test_utf8_new.feather")

ds = Feather.read(source_file)
@test size(ds) == (3,3)
Feather.read(source_file, Feather.Sink, sink_file)
@test isequal(ds, Feather.read(sink_file))

sink = Feather.Sink(joinpath(testdir, "test_utf8_new.feather"))
Feather.read(source_file, sink)
@test isequal(ds, Feather.read(sink_file))

source = Feather.Source(source_file)
ds = Feather.read(source)
@test size(ds) == (3,3)
source = Feather.Source(source_file)
Feather.read(source, Feather.Sink, sink_file)
@test isequal(ds, Feather.read(sink_file))

sink = Feather.Sink(joinpath(testdir, "test_utf8_new.feather"))
source = Feather.Source(source_file)
Feather.read(source, sink)
@test isequal(ds, Feather.read(sink_file))

si = Feather.write(sink_file, Feather.Source, source_file)
@test isequal(ds, Feather.read(sink_file))

source = Feather.Source(source_file)
Feather.write(sink_file, source)
@test isequal(ds, Feather.read(sink_file))

sink = Feather.Sink(joinpath(testdir, "test_utf8_new.feather"))
Feather.write(sink, Feather.Source, source_file)
@test isequal(ds, Feather.read(sink_file))

source = Feather.Source(source_file)
sink = Feather.Sink(joinpath(testdir, "test_utf8_new.feather"))
Feather.write(sink, source)
@test isequal(ds, Feather.read(sink_file))
rm(sink_file)

# check if valid, non-sudo docker is available
dockercheck = false
try
    dockercheck = success(`docker images`)
catch
    println("It seems that `docker` is not installed or has to be run with sudo, skipping python roundtrip tests")
end

if dockercheck
println("Running python round-trip tests...")

println("Pulling feather docker image...")
run(`docker pull quinnj/feather:0.1`)

println("Create docker container from feather image...")
run(`docker run -it -d --name feathertest quinnj/feather:0.1 /bin/sh`)

println("Generate a test.feather file from python...")
run(`docker cp runtests.py feathertest:/home/runtests.py`)
run(`docker exec feathertest python /home/runtests.py`)

println("Read test.feather into julia...")
run(`docker cp feathertest:/home/test.feather test.feather`)
df = Feather.read("test.feather")

@test df[:Autf8] == ["hey","there","sailor"]
@test df[:Abool] == [true, true, false]
@test df[:Acat] == CategoricalArrays.NominalArray(["a","b","c"])
@test df[:Acatordered] == CategoricalArrays.OrdinalArray(["d","e","f"])
@test df[:Adatetime] == [DateTime(2016,1,1), DateTime(2016,1,2), DateTime(2016,1,3)]
@test isequal(df[:Afloat32], NullableArray(Float32[1.0, 0.0, 0.0], [false, true, false]))
@test df[:Afloat64] == [Inf,1.0,0.0]

println("Writing test2.feather from julia...")
Feather.write("test2.feather", df)
df2 = Feather.read("test2.feather")

@test df2[:Autf8] == ["hey","there","sailor"]
@test df2[:Abool] == [true, true, false]
@test df2[:Acat] == CategoricalArrays.NominalArray(["a","b","c"])
@test df2[:Acatordered] == CategoricalArrays.OrdinalArray(["d","e","f"])
@test df2[:Adatetime] == [DateTime(2016,1,1), DateTime(2016,1,2), DateTime(2016,1,3)]
@test isequal(df2[:Afloat32], NullableArray(Float32[1.0, 0.0, 0.0], [false, true, false]))
@test df2[:Afloat64] == [Inf,1.0,0.0]

println("Read test2.feather into python...")
run(`docker cp test2.feather feathertest:/home/test2.feather`)
run(`docker cp runtests2.py feathertest:/home/runtests2.py`)
run(`docker exec feathertest python /home/runtests2.py`)

run(`docker stop feathertest`)
run(`docker rm feathertest`)
rm("test.feather")
rm("test2.feather")

end
