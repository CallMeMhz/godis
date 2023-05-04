package godis

import (
	"strconv"
	"testing"
)

func BenchmarkQuickList_Push(b *testing.B) {
	ql := new(QuickList)
	for i := 0; i < b.N; i++ {
		ql.Push([]byte(strconv.Itoa(i)))
	}
}

func BenchmarkLinkedList_Push(b *testing.B) {
	ll := new(LinkedList)
	for i := 0; i < b.N; i++ {
		ll.Push([]byte(strconv.Itoa(i)))
	}
}

func BenchmarkQuickList_GetAll(b *testing.B) {
	ql := new(QuickList)
	for i := 0; i < 10000; i++ {
		ql.Push([]byte(strconv.Itoa(i)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ql.GetAll()
	}
}

func BenchmarkLinkedList_GetAll(b *testing.B) {
	ll := new(LinkedList)
	for i := 0; i < 10000; i++ {
		ll.Push([]byte(strconv.Itoa(i)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ll.GetAll()
	}
}
