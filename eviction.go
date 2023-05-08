package godis

import "fmt"

const (
	NoEviction = iota
	VolatileRandom
	VolatileTTL
	VolatileLRU
	VolatileLFU
	AllKeysRandom
	AllKeysLRU
	AllKeysLFU
)

func (server *Server) tryEvict() {
	switch server.Options.Eviction.Policy {
	case NoEviction:
		return
	case VolatileRandom:
		if server.Options.Eviction.MaxOffHeapSize <= 0 {
			return
		}

		const factor = 0.9
		if float64(offHeapSize) < float64(server.Options.Eviction.MaxOffHeapSize)*factor {
			return
		}

		size := offHeapSize
		keysToDelete := make(map[string]Value)
		for key, value := range server.dict {
			if float64(size) >= float64(server.Options.Eviction.MaxOffHeapSize)*factor {
				keysToDelete[key] = value
				size -= value.cap
			}
		}
		fmt.Println("eviction ...")
		for key, value := range keysToDelete {
			fmt.Printf("key <%s> is evicted\n", key)
			delete(server.dict, key)
			delete(server.expire, key)
			Free(value.Bytes)
		}
	}
}
