import numpy as np
import numpy.typing as npt

NDArray = npt.NDArray

StringArray = StrArray = npt.NDArray[np.str_]
BytesArray = npt.NDArray[np.bytes_]
Float32Array = FloatArray = npt.NDArray[np.float32]
Float64Array = DoubleArray = npt.NDArray[np.float64]
IntArray = npt.NDArray[np.int_]
Int8Array = npt.NDArray[np.int8]
Int16Array = npt.NDArray[np.int16]
Int32Array = npt.NDArray[np.int32]
Int64Array = npt.NDArray[np.int64]
UInt8Array = npt.NDArray[np.uint8]
UInt16Array = npt.NDArray[np.uint16]
UInt32Array = npt.NDArray[np.uint32]
UInt64Array = npt.NDArray[np.uint64]
DateTimeArray = npt.NDArray[np.datetime64]
TimeDeltaArray = npt.NDArray[np.timedelta64]
