package godis

import (
	"bytes"
	"testing"
)

func TestQuickList(t *testing.T) {
	items := [][]byte{
		[]byte("1"),
		[]byte("22"),
		[]byte("3"),
		[]byte("4"),
		[]byte("5"),
		[]byte("6"),
	}
	ql := new(QuickList)
	for _, item := range items {
		ql.Push(item)
	}

	values := ql.GetAll()
	for i, value := range values {
		if bytes.Compare(value, items[i]) != 0 {
			t.Fatal(i)
		}
	}
}
