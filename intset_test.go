package godis

import (
	"testing"
)

func TestIntset(t *testing.T) {
	var is any
	var init []int16
	is = init

	is = intsetAdd(is, 1)
	is = intsetAdd(is, 2)
	is = intsetAdd(is, 3)
	if sizeOfIntset(is) != 3 {
		t.Fatal("size is not equals to 3")
	}
	is = intsetAdd(is, 2)
	if sizeOfIntset(is) != 3 {
		t.Fatal("size is not equals to 3")
	}
	is = intsetDel(is, 2)
	if sizeOfIntset(is) != 2 {
		t.Fatal("size is not equals to 2")
	}

	if _, ok := is.([]int16); !ok {
		t.Fatal("type(intset) is not []int16")
	}
	is = intsetAdd(is, 1<<16+1)
	if sizeOfIntset(is) != 3 {
		t.Fatal("size is not equals to 3")
	}
	if _, ok := is.([]int32); !ok {
		t.Fatal("type(intset) is not []int32")
	}

}
