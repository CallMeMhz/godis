package godis

import (
	"encoding/binary"
)

const ListMaxZiplistSize = 5

type QuickList struct {
	head, tail *QuickListNode
	count      int // count of elems in quick list
	len        int // count of nodes in quick list
}

func (list *QuickList) GetAll() (values [][]byte) {
	for node := list.head; node != nil; node = node.next {
		for i := 0; i < node.zl.count; i++ {
			offset := i * node.zl.len
			size, n := binary.Uvarint(node.zl.buf[offset : offset+node.zl.len])
			value := make([]byte, size)
			copy(value, node.zl.buf[offset+n:offset+node.zl.len])
			values = append(values, value)
		}
	}
	return
}

func (list *QuickList) Size() int { return list.count }

func (list *QuickList) Push(value []byte) {
	if list.tail != nil && list.tail.zl.count < ListMaxZiplistSize {
		list.tail.zl.pushTail(value)
	} else {
		node := new(QuickListNode)
		node.zl = new(ZiplistNode)
		node.zl.pushTail(value)
		if list.tail == nil {
			list.head = node
			list.tail = node
		} else {
			node.prev = list.tail
			list.tail.next = node
			list.tail = node
		}
		list.len++
	}
	list.count++
}

func (list *QuickList) Pop() []byte {
	if list.tail == nil {
		return nil
	}
	value := list.tail.zl.popTail()
	if list.tail.zl.count == 0 {
		if list.tail == list.head {
			list.head = nil
			list.tail = nil
		} else {
			list.tail = list.tail.prev
			list.tail.next = nil
		}
		list.len--
	}
	list.count--
	return value
}

type ZiplistNode struct {
	buf   []byte
	len   int
	count int
}

type QuickListNode struct {
	prev *QuickListNode
	next *QuickListNode
	zl   *ZiplistNode
}

func (node *ZiplistNode) pushTail(value []byte) {
	if node.count >= ListMaxZiplistSize {
		panic("ziplist overflow")
	}
	// update item length if exceed
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, uint64(len(value)))
	if newLen := n + len(value); newLen > node.len {
		newBuf := make([]byte, newLen*ListMaxZiplistSize)
		for i := 0; i < node.count; i++ {
			offset := i * node.len
			size, n1 := binary.Uvarint(node.buf[offset : offset+node.len])
			n2 := binary.PutUvarint(newBuf[i*newLen:], size)
			copy(newBuf[i*newLen+n2:], node.buf[offset+n1:offset+node.len])
		}
		node.buf = newBuf
		node.len = newLen
	}
	offset := node.count * node.len
	n = binary.PutUvarint(node.buf[offset:], uint64(len(value)))
	copy(node.buf[offset+n:], value)
	node.count++
}

func (node *ZiplistNode) popTail() []byte {
	if node.count == 0 {
		panic("pop from empty ziplist")
	}
	offset := (node.count - 1) * node.len
	size, n := binary.Uvarint(node.buf[offset : offset+node.len])
	value := make([]byte, size)
	copy(value, node.buf[offset+n:offset+node.len])
	node.count--
	return value
}
