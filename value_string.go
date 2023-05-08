package godis

const (
	StringEncodingRaw     uint8 = 0
	StringEncodingString  uint8 = 1
	StringEncodingInteger uint8 = 2
)

// 0 raw, 1 string, 2 integer
func StringEncoding(bytes Bytes) uint8 {
	slice := Slice[byte](bytes)
	return slice[0]
}

func StringSetString(bytes Bytes, value []byte) {
	Slice[byte](bytes)[0] = StringEncodingString
	bytes.ptr++
	bytes.len--
	bytes.cap--
	buf := Slice[byte](bytes)
	copy(buf, value)
}

func StringGetBytes(bytes Bytes) []byte {
	bytes.ptr++
	bytes.len--
	bytes.cap--
	return Slice[byte](bytes)
}

func StringSetInt(bytes Bytes, value int64) {
	Slice[byte](bytes)[0] = StringEncodingInteger
	bytes.ptr++
	bytes.len--
	bytes.cap--
	Slice[int64](bytes)[0] = value
}

func StringGetInt(bytes Bytes) int64 {
	bytes.ptr++
	bytes.len--
	bytes.cap--
	return Slice[int64](bytes)[0]
}

func StringIncr(bytes Bytes, delta int64) int64 {
	bytes.ptr++
	bytes.len--
	bytes.cap--
	slice := Slice[int64](bytes)
	slice[0] += delta
	return slice[0]
}
