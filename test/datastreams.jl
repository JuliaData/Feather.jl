using Feather, DataFrames, Base.Test, DataArrays, NullableArrays, WeakRefStrings, DataStreamsIntegrationTests

# DataFrames
FILE = joinpath(DSTESTDIR, "randoms_small.csv")
DF = readtable(FILE)
strings = DF[2]
strings2 = DF[3]
if typeof(DF[:hiredate]) <: NullableVector
    DF[:hiredate] = NullableArray(Date[isnull(x) ? Date() : Date(get(x)) for x in DF[:hiredate]], [isnull(x) for x in DF[:hiredate]])
    DF[:lastclockin] = NullableArray(DateTime[isnull(x) ? DateTime() : DateTime(get(x)) for x in DF[:lastclockin]], [isnull(x) for x in DF[:lastclockin]])
    stringdata = join(String[get(x) for x in strings])
    stringdata2 = join(String[get(x) for x in strings2])
    DF.columns[2] = NullableArray{WeakRefString{UInt8},1}(Array(WeakRefString{UInt8}, size(DF, 1)), ones(Bool, size(DF, 1)), Vector{UInt8}(stringdata))
    DF.columns[3] = NullableArray{WeakRefString{UInt8},1}(Array(WeakRefString{UInt8}, size(DF, 1)), ones(Bool, size(DF, 1)), Vector{UInt8}(stringdata2))
    ind = ind2 = 1
    for i = 1:size(DF, 1)
        DF.columns[2][i] = Nullable(WeakRefString(pointer(stringdata, ind), length(get(strings[i])), ind))
        DF.columns[3][i] = Nullable(WeakRefString(pointer(stringdata2, ind2), length(get(strings2[i])), ind2))
        ind += length(get(strings[i]))
        ind2 += length(get(strings2[i]))
    end
else
    for i = 1:5
        T = eltype(DF.columns[i])
        DF.columns[i] = NullableArray(T[isna(x) ? (T <: String ? "" : zero(T)) : x for x in DF.columns[i]], [isna(x) for x in DF.columns[i]])
    end
    DF.columns[6] = NullableArray(Date[isna(x) ? Date() : Date(x) for x in DF[:hiredate]], [isna(x) for x in DF[:hiredate]])
    DF.columns[7] = NullableArray(DateTime[isna(x) ? DateTime() : DateTime(x) for x in DF[:lastclockin]], [isna(x) for x in DF[:lastclockin]])
    stringdata = join(String[isna(x) ? "" : x for x in strings])
    stringdata2 = join(String[isna(x) ? "" : x for x in strings2])
    DF.columns[2] = NullableArray{WeakRefString{UInt8},1}(Array{WeakRefString{UInt8}}(size(DF, 1)), ones(Bool, size(DF, 1)), Vector{UInt8}(stringdata))
    DF.columns[3] = NullableArray{WeakRefString{UInt8},1}(Array{WeakRefString{UInt8}}(size(DF, 1)), ones(Bool, size(DF, 1)), Vector{UInt8}(stringdata2))
    ind = ind2 = 1
    for i = 1:size(DF, 1)
        DF.columns[2][i] = Nullable(WeakRefString(pointer(stringdata, ind), length(strings[i]), ind))
        DF.columns[3][i] = Nullable(WeakRefString(pointer(stringdata2, ind2), length(strings2[i]), ind2))
        ind += length(strings[i])
        ind2 += length(strings2[i])
    end
end
DF2 = deepcopy(DF)
dfsource = Tester("DataFrame", x->x, false, DataFrame, (:DF,), scalartransforms, vectortransforms, x->x, x->nothing)
dfsink = Tester("DataFrame", x->x, false, DataFrame, (:DF2,), scalartransforms, vectortransforms, x->x, x->nothing)
function DataFrames.DataFrame(sym::Symbol; append::Bool=false)
    return @eval $sym
end
function DataFrames.DataFrame(sch::Data.Schema, ::Type{Data.Column}, append::Bool, ref::Vector{UInt8}, sym::Symbol)
    return DataFrame(DataFrame(sym), sch, Data.Column, append, ref)
end
function DataFrames.DataFrame(sink, sch::Data.Schema, ::Type{Data.Column}, append::Bool, ref::Vector{UInt8})
    rows, cols = size(sch)
    append ? (sch.rows += size(sink, 1)) : foreach(empty!, sink.columns)
    if append
        types = Data.types(sch)
        for (i, T) in enumerate(Data.types(sink, Data.Column))
            if T <: NullableVector{WeakRefString{UInt8}}
                if types[i] <: NullableVector{String}
                    sink.columns[i] = NullableArray(String[string(get(x, "")) for x in sink.columns[i]])
                else
                    sink.columns[i] = Feather.renullify(sink.columns[i])
                end
            end
        end
    end
    return sink
end
function Feather.Sink{T<:Data.StreamType}(file::AbstractString, schema::Data.Schema=Data.Schema(), ::Type{T}=Data.Column;
              description::AbstractString="", metadata::AbstractString="", append::Bool=false)
    if append && isfile(file)
        df = Feather.read(file)
        types = Data.types(schema)
        for i = 1:size(df, 2)
            if eltype(df.columns[i]) <: Nullable{WeakRefString{UInt8}}
                if !isempty(types) && types[i] <: NullableVector{String}
                    df.columns[i] = NullableArray(String[string(get(x, "")) for x in df.columns[i]])
                else
                    df.columns[i] = Feather.renullify(df.columns[i])
                end
            end
        end
        schema.rows += size(df, 1)
    else
        df = DataFrame(schema, T)
    end
    io = IOBuffer()
    Feather.writepadded(io, Feather.FEATHER_MAGIC_BYTES)
    return Feather.Sink(Feather.Metadata.CTable("", 0, Feather.Metadata.Column[], Feather.VERSION, ""), file, io, description, metadata, df)
end
function Feather.Sink{T}(sink, sch::Data.Schema, ::Type{T}, append::Bool, ref::Vector{UInt8})
    if !append
        for col in sink.df.columns
            empty!(col)
        end
        sink.df = DataFrame(sch, T)
    else
        types = Data.types(sch)
        for i = 1:size(sink.df, 2)
            if eltype(sink.df.columns[i]) <: Nullable{WeakRefString{UInt8}}
                if !isempty(types) && types[i] <: NullableVector{String}
                    sink.df.columns[i] = NullableArray(String[string(get(x, "")) for x in sink.df.columns[i]])
                else
                    sink.df.columns[i] = Feather.renullify(sink.df.columns[i])
                end
            end
        end
        sch.rows += size(sink.df, 1)
    end
    return sink
end


# Feather
FFILE = joinpath(DSTESTDIR, "randoms_small.feather")
FFILE2 = joinpath(DSTESTDIR, "randoms2_small.feather")
feathersource = Tester("Feather.Source", Feather.read, true, Feather.Source, (FFILE,), scalartransforms, vectortransforms, x->x, ()->nothing)
feathersink = Tester("Feather.Sink", Feather.write, true, Feather.Sink, (FFILE2,), scalartransforms, vectortransforms, x->Feather.read(FFILE2), x->rm(FFILE2))

DataStreamsIntegrationTests.teststream([dfsource, feathersource], [dfsink, feathersink]; rows=99)
