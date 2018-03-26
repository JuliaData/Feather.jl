

const NROWS = 128
const ARROW_FILENAME = "arrowtest1.feather"

randdate() = Date(rand(0:4000), rand(1:12), rand(0:27))
randtime() = Dates.Time(rand(0:23), rand(0:59), rand(0:59))
randdatetime() = randdate() + randtime()

randstrings() = [[randstring(rand(0:20)) for i ∈ 1:(NROWS-1)]; "a"]
randstrings(::Missing) = [[rand(Bool) ? missing : randstring(rand(0:20)) for i ∈ 1:(NROWS-1)]; "a"]

@testset "ArrowTests" begin
    df = DataFrame(ints=rand(Int32,NROWS),
                   floats=rand(Float64,NROWS),
                   dates=[randdate() for i ∈ 1:NROWS],
                   datetimes=[randdatetime() for i ∈ 1:NROWS],
                   times=[randtime() for i ∈ 1:NROWS],
                   missingints=[rand(Bool) ? missing : rand(Int64) for i ∈ 1:NROWS],
                   strings=randstrings(),
                   missingstrings=randstrings(missing),
                   catstrings=categorical(randstrings()),
                   catstringsmissing=categorical(randstrings(missing))
                  )
    
    Feather.write(ARROW_FILENAME, df)

    ndf = Feather.read(ARROW_FILENAME)
end
