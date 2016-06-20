using Feather, DataFrames
using Base.Test

testdir = joinpath(Pkg.dir("Feather"), "test/data")
files = map(x -> joinpath(testdir, x), readdir(testdir))
for f in files
    source = Feather.Source(f)
    df = Data.stream!(source, DataFrame)
    temp = tempname()
    sink = Feather.Sink(temp)
    Feather.Data.stream!(df, sink)
    df2 = Feather.read(temp)

    for (c1,c2) in zip(df.columns,df2.columns)
        for i = 1:length(c1)
            @test isnull(c1[i]) == isnull(c2[i])
            if !isnull(c1[i])
                @test get(c1[i]) === get(c2[i])
            end
        end
    end
    @test source.schema == sink.schema
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
