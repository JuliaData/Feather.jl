using DataFrames
using DataStreams
using Arrow
using Feather


# const filename = "dicttest.feather"
const filename = "datestest.feather"
# const filename = "basictest.feather"


s = Feather.Source(filename)

# v = DictEncoding(String, pointer(s.data), s.ctable.columns[1])

df = DataFrame(s)

