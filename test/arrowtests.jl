
const SEED = 999
const NROWS = 128
const N_IDX_TESTS = 16

arrow_tempname = tempname()

if VERSION < v"0.7-"
    srand(SEED)
else
    Random.seed!(SEED)
end

randdate() = Date(rand(0:4000), rand(1:12), rand(1:27))
randtime() = Dates.Time(rand(0:23), rand(0:59), rand(0:59))
randdatetime() = randdate() + randtime()

randstrings() = String[[randstring(rand(0:20)) for i ∈ 1:(NROWS-1)]; "a"]
function randstrings(::Missing)
    Union{String,Missing}[[rand(Bool) ? missing : randstring(rand(0:20)) for i ∈ 1:(NROWS-1)]; "a"]
end

convstring(str::AbstractString) = String(str)
convstring(::Missing) = missing

@testset "ArrowTests" begin
df = DataFrame(ints=rand(Int32,NROWS),
               floats=rand(Float64,NROWS),
               dates=Date[randdate() for i ∈ 1:NROWS],
               datetimes=DateTime[randdatetime() for i ∈ 1:NROWS],
               times=Dates.Time[randtime() for i ∈ 1:NROWS],
               missingints=Union{Int64,Missing}[rand(Bool) ? missing : rand(Int64) for i ∈ 1:NROWS],
               strings=randstrings(),
               missingstrings=randstrings(missing),
               catstrings=categorical(randstrings()),
               catstringsmissing=categorical(randstrings(missing))
              )

Feather.write(arrow_tempname, df)

ndf = Feather.read(arrow_tempname)

@test typeof(ndf[:ints]) == Primitive{Int32}
@test typeof(ndf[:floats]) == Primitive{Float64}
@test typeof(ndf[:dates]) == Primitive{Arrow.Datestamp}
@test typeof(ndf[:datetimes]) == Primitive{Arrow.Timestamp{Dates.Millisecond}}
@test typeof(ndf[:times]) == Primitive{Arrow.TimeOfDay{Dates.Nanosecond,Int64}}
@test typeof(ndf[:missingints]) == NullablePrimitive{Int64}
@test typeof(ndf[:strings]) == List{String,Arrow.DefaultOffset,Primitive{UInt8}}
@test typeof(ndf[:missingstrings]) == NullableList{String,Arrow.DefaultOffset,Primitive{UInt8}}
@test typeof(ndf[:catstrings]) == DictEncoding{String,Primitive{Int32},
                                               List{String,Arrow.DefaultOffset,Primitive{UInt8}}}
@test typeof(ndf[:catstringsmissing]) ==
        DictEncoding{Union{String,Missing},NullablePrimitive{Int32},List{String,Arrow.DefaultOffset,
                                                                         Primitive{UInt8}}}

for j ∈ 1:N_IDX_TESTS
    i = rand(1:NROWS)
    @test df[i, :ints] == ndf[i, :ints]
    @test df[i, :floats] == ndf[i, :floats]
    @test df[i, :dates] == convert(Date, ndf[i, :dates])
    @test df[i, :datetimes] == convert(DateTime, ndf[i, :datetimes])
    @test df[i, :times] == convert(Dates.Time, ndf[i, :times])
    @test df[i, :missingints] ≅ ndf[i, :missingints]
    @test df[i, :strings] == ndf[i, :strings]
    @test df[i, :missingstrings] ≅ ndf[i, :missingstrings]
    @test df[i, :catstrings] == String(ndf[i, :catstrings])
    @test df[i, :catstringsmissing] ≅ convstring(ndf[i, :catstringsmissing])
end
for j ∈ 1:N_IDX_TESTS
    a, b = extrema(rand(1:NROWS, 2))
    i = a:b
    @test df[i, :ints] == ndf[i, :ints]
    @test df[i, :floats] == ndf[i, :floats]
    @test df[i, :dates] == convert.(Date, ndf[i, :dates])
    @test df[i, :datetimes] == convert.(DateTime, ndf[i, :datetimes])
    @test df[i, :times] == convert.(Dates.Time, ndf[i, :times])
    @test df[i, :missingints] ≅ ndf[i, :missingints]
    @test df[i, :strings] == ndf[i, :strings]
    @test df[i, :missingstrings] ≅ ndf[i, :missingstrings]
    @test df[i, :catstrings] == String.(ndf[i, :catstrings])
    @test df[i, :catstringsmissing] ≅ convstring.(ndf[i, :catstringsmissing])
end
for j ∈ 1:N_IDX_TESTS
    i = rand(1:NROWS, rand(1:4))
    @test df[i, :ints] == ndf[i, :ints]
    @test df[i, :floats] == ndf[i, :floats]
    @test df[i, :dates] == convert.(Date, ndf[i, :dates])
    @test df[i, :datetimes] == convert.(DateTime, ndf[i, :datetimes])
    @test df[i, :times] == convert.(Dates.Time, ndf[i, :times])
    @test df[i, :missingints] ≅ ndf[i, :missingints]
    @test df[i, :strings] == ndf[i, :strings]
    @test df[i, :missingstrings] ≅ ndf[i, :missingstrings]
    @test df[i, :catstrings] == String.(ndf[i, :catstrings])
    @test df[i, :catstringsmissing] ≅ convstring.(ndf[i, :catstringsmissing])
end
end

ndf = nothing;
GC.gc()

rm(arrow_tempname)
