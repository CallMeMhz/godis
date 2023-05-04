package godis

type List interface {
	GetAll() [][]byte
	Size() int
	Push(value []byte)
	Pop() []byte
}

type LinkedList struct {
	head, tail *LinkedListElem
	length     int
}

type LinkedListElem struct {
	value []byte
	prev  *LinkedListElem
	next  *LinkedListElem
}

func (ll *LinkedList) GetAll() [][]byte {
	values := make([][]byte, ll.length)
	for i, elem := 0, ll.head; elem != nil; i, elem = i+1, elem.next {
		value := make([]byte, len(elem.value))
		copy(value, elem.value)
		values[i] = value
	}
	return values
}

func (ll *LinkedList) Size() int { return ll.length }

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

func (ll *LinkedList) Pop() []byte {
	if ll.length == 0 {
		return nil
	}
	ll.length--
	if ll.head == ll.tail {
		value := ll.tail.value
		ll.head = nil
		ll.tail = nil
		return value
	} else {
		value := ll.tail.value
		ll.tail = ll.tail.prev
		ll.tail.next = nil
		return value
	}
}
