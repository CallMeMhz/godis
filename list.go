package godis

type LinkedList struct {
	head, tail *LinkedListElem
	length     int
}

type LinkedListElem struct {
	value []byte
	prev  *LinkedListElem
	next  *LinkedListElem
}

func (ll *LinkedList) Push(value []byte) int {
	elem := &LinkedListElem{
		value: value,
		prev:  ll.tail,
		next:  nil,
	}
	if ll.head == nil {
		ll.head = elem
		ll.tail = elem
	} else {
		ll.tail.next = elem
		ll.tail = elem
	}
	ll.length++
	return ll.length
}

func (ll *LinkedList) Pop() ([]byte, int) {
	if ll.length == 0 {
		return nil, -1
	}
	ll.length--
	if ll.head == ll.tail {
		value := ll.tail.value
		ll.head = nil
		ll.tail = nil
		return value, ll.length
	} else {
		value := ll.tail.value
		ll.tail = ll.tail.prev
		ll.tail.next = nil
		return value, ll.length
	}
}
