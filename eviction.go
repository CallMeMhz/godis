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
		keysToDelete := make([]map[string]Value, len(server.dicts))
		for i := 0; i < len(server.dicts); i++ {
			keysToDelete[i] = make(map[string]Value)
		}
		for shardID, dict := range server.dicts {
			for key, value := range dict {
				if float64(size) >= float64(server.Options.Eviction.MaxOffHeapSize)*factor {
					keysToDelete[shardID][key] = value
					size -= value.cap
				}
			}
		}

		fmt.Println("eviction ...")
		for i, keys := range keysToDelete {
			shardID := uint64(i)
			for key, value := range keys {
				fmt.Printf("key <%s> is evicted\n", key)
				server.delKey(shardID, key, value)
			}
		}
	}
}
