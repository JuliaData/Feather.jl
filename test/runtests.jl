using Feather, DataFrames
using Base.Test

testdir = joinpath(dirname(@__FILE__), "data")
# testdir = joinpath("/Users/jacobquinn/.julia/v0.5/Feather/test","data")
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
    @test source.ctable.version == sink.ctable.version
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
        @test v1.total_bytes == v2.total_bytes
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
