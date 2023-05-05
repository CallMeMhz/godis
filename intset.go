package godis

import (
	"math"
)

const (
	IntsetEncodingInt16 = 1 << 1 // 2 Bytes
	IntsetEncodingInt32 = 1 << 2 // 4 Bytes
	IntsetEncodingInt64 = 1 << 3 // 8 Bytes
)

const MaxIntSetItems = 512

func createIntset(value int64) any {
	enc := intsetValueEncoding(value)
	switch enc {
	case IntsetEncodingInt16:
		return []int16{int16(value)}
	case IntsetEncodingInt32:
		return []int32{int32(value)}
	case IntsetEncodingInt64:
		return []int64{value}
	default:
		panic("invalid encoding of value")
	}
}

func intsetAdd(is any, value int64) any {
	enc := encodingOfIntset(is)
	size := sizeOfIntset(is)
	if newEnc := intsetValueEncoding(value); newEnc > enc {
		// upgrade
		switch newEnc {
		case IntsetEncodingInt32:
			is32 := make([]int32, size, size+1)
			migrateIntset[int16, int32](is.([]int16), is32)
			is32 = append(is32, int32(value))
			is = is32
		case IntsetEncodingInt64:
			is64 := make([]int64, size, size+1)
			switch enc {
			case IntsetEncodingInt16:
				migrateIntset[int16, int64](is.([]int16), is64)
			case IntsetEncodingInt32:
				migrateIntset[int32, int64](is.([]int32), is64)
			}
			is64 = append(is64, value)
			is = is64
		}
		return is
	}
	if intsetHasValue(is, value) {
		return is
	}
	switch enc {
	case IntsetEncodingInt16:
		return appendIntset[int16](is.([]int16), value)
	case IntsetEncodingInt32:
		return appendIntset[int32](is.([]int32), value)
	case IntsetEncodingInt64:
		return appendIntset[int64](is.([]int64), value)
	default:
		panic("unexpected encoding of intset")
	}
}

func intsetDel(is any, value int64) any {
	switch v := is.(type) {
	case []int16:
		return intsetRemove[int16](v, value)
	case []int32:
		return intsetRemove[int32](v, value)
	case []int64:
		return intsetRemove[int64](v, value)
	default:
		panic("invalid intset type")
	}
}

func intsetHasValue(is any, value int64) bool {
	if intsetValueEncoding(value) > encodingOfIntset(is) {
		return false
	}
	switch t := is.(type) {
	case []int16:
		return intsetContains[int16](t, value)
	case []int32:
		return intsetContains[int32](t, value)
	case []int64:
		return intsetContains[int64](t, value)
	default:
		panic("invalid intset type")
	}
}

func sizeOfIntset(is any) int {
	switch t := is.(type) {
	case []int16:
		return len(t)
	case []int32:
		return len(t)
	case []int64:
		return len(t)
	default:
		panic("invalid intset type")
	}
}

func encodingOfIntset(is any) int {
	switch is.(type) {
	case []int16:
		return IntsetEncodingInt16
	case []int32:
		return IntsetEncodingInt32
	case []int64:
		return IntsetEncodingInt64
	default:
		return 0
	}
}

func migrateIntset[T1 int16 | int32, T2 int32 | int64](is1 []T1, is2 []T2) {
	for i := range is1 {
		is2[i] = T2(is1[i])
	}
}

func appendIntset[T int16 | int32 | int64](is []T, value int64) []T {
	return append(is, T(value))
}

func intsetContains[T int16 | int32 | int64](is []T, value int64) bool {
	for _, v := range is {
		if v == T(value) {
			return true
		}
	}
	return false
}

func intsetRemove[T int16 | int32 | int64](is []T, value int64) []T {
	var idx int
	for idx < len(is) {
		if is[idx] == T(value) {
			break
		}
		idx++
	}
	is[idx] = is[len(is)-1]
	return is[:len(is)-1]
}

func intsetValueEncoding(value int64) int {
	if value < math.MinInt32 || value > math.MaxInt32 {
		return IntsetEncodingInt64
	} else if value < math.MinInt16 || value > math.MaxInt16 {
		return IntsetEncodingInt32
	} else {
		return IntsetEncodingInt16
	}
}
