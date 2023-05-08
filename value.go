package godis

import (
	"reflect"
	"unsafe"
)

const (
	TypeString int8 = 1
)

type Value struct {
	Bytes

	typ int8

	// todo lfu
	timestamp int16
	visited   int8

	padding int16
}

type Bytes struct {
	ptr uintptr
	len int
	cap int
}

func Slice[T byte | int8 | int16 | int32 | int64](bytes Bytes) []T {
	var t T
	sizeT := int(unsafe.Sizeof(t))
	if bytes.len < sizeT || bytes.cap < sizeT {
		panic("wtf")
	}
	header := reflect.SliceHeader{
		Data: bytes.ptr,
		Len:  bytes.len / sizeT,
		Cap:  bytes.cap / sizeT,
	}
	return *(*[]T)(unsafe.Pointer(&header))
}
