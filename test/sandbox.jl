using DataFrames
using DataStreams
using Arrow
using Feather


# const filename = "dicttest.feather"
# const filename = "datestest.feather"
# const filename = "basictest.feather"

# s = Feather.Source(filename)

# df = DataFrame(s)

v = CategoricalArray(["a", "b", "a", "c"])

df = DataFrame(A=v)

# df = DataFrame(A=[2,3,5,7], B=[2.0, missing, 5.0, 7.0], C=["a", "ab", "abc", "abcd"],
#                D=["a", missing, "ab", "abc"], E=[Date()+Dates.Day(i) for i ∈ 1:4],
#                F=vcat([missing], [DateTime()+Dates.Second(i) for i ∈ 1:3]), G=G)


sink = Feather.Sink("output.feather", df)


info("streaming...")
Data.stream!(df, sink)
info("writing...")
Data.close!(sink)
info("done.")


src = Feather.Source("output.feather")
odf = DataFrame(src)

src2 = Feather.Source("dicttest.feather")
odf2 = DataFrame(src2)
