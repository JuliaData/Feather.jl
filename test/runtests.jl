using Feather, DataFrames, Base.Test, DataArrays, NullableArrays, WeakRefStrings

testdir = joinpath(dirname(@__FILE__), "data")
testdir2 = joinpath(dirname(@__FILE__), "newdata")
# testdir = joinpath(Pkg.dir("Feather"), "test/data")
# testdir2 = joinpath(Pkg.dir("Feather"), "test/newdata")
files = map(x -> joinpath(testdir, x), readdir(testdir))
append!(files, map(x -> joinpath(testdir2, x), readdir(testdir2)))

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
    Data.close!(sink)
    df2 = Feather.read(temp)

    for (c1,c2) in zip(df.columns,df2.columns)
        for i = 1:length(c1)
            @test testnull(c1[i], c2[i])
        end
    end
    # @test Data.header(source) == Data.header(sink) && Data.types(source) == Data.types(sink)
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

# check if valid, non-sudo docker is available
dockercheck = false
try
    dockercheck = success(`docker images`)
catch
    println("It seems that `docker` is not installed or has to be run with sudo, skipping python roundtrip tests")
end

if dockercheck

DOCKERTAG = "0.1"

println("Running python round-trip tests...")

println("Pulling feather docker image...")
run(`docker pull quinnj/feather:$DOCKERTAG`)

println("Create docker container from feather image...")
run(`docker run -it -d --name feathertest quinnj/feather:$DOCKERTAG /bin/sh`)

println("Generate a test.feather file from python...")
run(`docker cp runtests.py feathertest:/home/runtests.py`)
run(`docker exec feathertest python /home/runtests.py`)

println("Read test.feather into julia...")
run(`docker cp feathertest:/home/test.feather test.feather`)
df = Feather.read("test.feather")

@test isequal(df[:Autf8], NullableArray(["hey","there","sailor"]))
@test isequal(df[:Abool], NullableArray([true, true, false]))
@test isequal(df[:Acat], CategoricalArrays.NullableCategoricalArray(["a","b","c"]))
@test isequal(df[:Acatordered], CategoricalArrays.NullableCategoricalArray(["d","e","f"]))
@test isequal(df[:Adatetime], NullableArray([DateTime(2016,1,1), DateTime(2016,1,2), DateTime(2016,1,3)]))
@test isequal(df[:Afloat32], NullableArray(Float32[1.0, 0.0, 0.0], [false, true, false]))
@test isequal(df[:Afloat64], NullableArray([Inf,1.0,0.0]))

df_ = Feather.read("test.feather"; nullable=false, use_mmap=false)

println("Writing test2.feather from julia...")
Feather.write("test2.feather", df)
df2 = Feather.read("test2.feather")

@test isequal(df2[:Autf8], NullableArray(["hey","there","sailor"]))
@test isequal(df2[:Abool], NullableArray([true, true, false]))
@test isequal(df2[:Acat], CategoricalArrays.NullableCategoricalArray(["a","b","c"]))
@test isequal(df2[:Acatordered], CategoricalArrays.NullableCategoricalArray(["d","e","f"]))
@test isequal(df2[:Adatetime], NullableArray([DateTime(2016,1,1), DateTime(2016,1,2), DateTime(2016,1,3)]))
@test isequal(df2[:Afloat32], NullableArray(Float32[1.0, 0.0, 0.0], [false, true, false]))
@test isequal(df2[:Afloat64], NullableArray([Inf,1.0,0.0]))

println("Read test2.feather into python...")
run(`docker cp test2.feather feathertest:/home/test2.feather`)
run(`docker cp runtests2.py feathertest:/home/runtests2.py`)
run(`docker exec feathertest python /home/runtests2.py`)

run(`docker stop feathertest`)
run(`docker rm feathertest`)
rm("test.feather")
rm("test2.feather")

end

installed = Pkg.installed()
haskey(installed, "DataStreamsIntegrationTests") || Pkg.clone("https://github.com/JuliaData/DataStreamsIntegrationTests")
using DataStreamsIntegrationTests

# test Data.Field-based streaming
FFILE = joinpath(DSTESTDIR, "randoms_small.feather")
source = Feather.Source(FFILE)
sch = Data.schema(source, Data.Field)
df = DataFrame(sch, Data.Field, false, Data.reference(source))
Data.stream!(source, Data.Field, df, sch, sch, [identity, identity, identity, identity, identity, identity, identity])
DataStreamsIntegrationTests.check(df, 99)

# test DataArray DataFrame
for i = 1:size(df, 2)
    if !(typeof(df.columns[i]) <: DataArray)
        df.columns[i] = DataArray(df.columns[i].values, df.columns[i].isnull)
    end
end
temp = tempname()
Feather.write(temp, df)
df = Feather.read(temp)
DataStreamsIntegrationTests.check(df, 99)
rm(temp)

# needed until #265 is resolved
workspace()

include("datastreams.jl")
