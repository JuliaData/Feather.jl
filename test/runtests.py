import feather, pandas, numpy as np, datetime

Abool = np.array([True,True,False])
Aint8 = np.array([1,1,0], dtype=np.int8)
Aint16 = np.array([1,1,0], dtype=np.int16)
Aint32 = np.array([1,1,0], dtype=np.int32)
Aint64 = np.array([1,1,0], dtype=np.int64)
Auint8 = np.array([1,1,0], dtype=np.uint8)
Auint16 = np.array([1,1,0], dtype=np.uint16)
Auint32 = np.array([1,1,0], dtype=np.uint32)
Auint64 = np.array([1,1,0], dtype=np.uint64)

Afloat32 = np.array([1.0, "NaN", 0.0], dtype=np.float32)
Afloat64 = np.array(["Inf", 1.0, 0.0], dtype=np.float64)

Autf8 = ["hey","there","sailor"]
Abinary = [b"hey",b"there",b"sailor"]

# Adate = [datetime.datetime(2016,1,1).date(),datetime.datetime(2016,1,2).date(),datetime.datetime(2016,1,3).date()]
Adatetime = [datetime.datetime(2016,1,1),datetime.datetime(2016,1,2),datetime.datetime(2016,1,3)]

Acat = pandas.Categorical(["a","b","c"], categories=["a","b","c","d"],ordered=False)  # don't conform to Arrow!
Acatordered = pandas.Categorical(["d","e","f"], categories=["d","e","f"],ordered=True)  # don't conform to Arrow!

df = pandas.DataFrame({"Abool": Abool,"Aint8": Aint8,"Aint16": Aint16,"Aint32": Aint32,"Aint64": Aint64,"Auint8": Auint8,"Auint16": Auint16,"Auint32": Auint32,"Auint64": Auint64,"Afloat32": Afloat32,"Afloat64": Afloat64,"Autf8": Autf8,"Abinary": Abinary,"Adatetime": Adatetime, "Acat": Acat,"Acatordered":Acatordered})

feather.write_dataframe(df,  "/home/test.feather")
