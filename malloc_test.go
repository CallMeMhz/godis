package godis

import "testing"

func TestStringIncr(t *testing.T) {
	str := String{
		bytes: Bytes{
			ptr: malloc(1),
			len: 1,
			cap: 1,
		},
		encoding: 2,
	}
	t.Logf("%+v\n", str)
	t.Log(str.Int())
	str.Incr(10)
	t.Log(str.Int())
}
