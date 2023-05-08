package godis

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

type Server struct {
	lis    net.Listener
	dicts  []map[string]Value
	latchs []*sync.Mutex
	expire []map[string]int64
	cmds   chan Command

	Options struct {
		Eviction struct {
			Policy         int
			MaxOffHeapSize int
		}
	}
}

func NewServer(addr string, shards int) (*Server, error) {
	if shards <= 0 {
		panic("invalid shard count")
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	server := new(Server)
	server.lis = listener
	server.dicts = make([]map[string]Value, shards)
	server.expire = make([]map[string]int64, shards)
	server.latchs = make([]*sync.Mutex, shards)
	for i := range server.dicts {
		server.dicts[i] = make(map[string]Value)
		server.expire[i] = make(map[string]int64)
		server.latchs[i] = new(sync.Mutex)
	}
	server.cmds = make(chan Command)
	return server, nil
}

func (server *Server) Serve() error {
	go server.mainLoop()
	for {
		conn, err := server.lis.Accept()
		if err != nil {
			return err
		}
		fmt.Println("client connected", conn.RemoteAddr())
		go server.eventLoop(conn)
	}
}

func (server *Server) Close() error {
	return server.lis.Close()
}

func (server *Server) eventLoop(conn net.Conn) {
	defer conn.Close()
	for {
		buf := make([]byte, 1024)
		_, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				fmt.Println("client disconnected", conn.RemoteAddr())
				return
			}
			fmt.Println("err:", err)
			return
		}
		buf = buf[:bytes.LastIndexByte(buf, '\n')]
		args := bytes.Split(buf, []byte{' '})
		server.cmds <- Command{args, conn}
	}
}

type Command struct {
	args [][]byte
	conn io.Writer
}

func (server *Server) mainLoop() {
	ticker := time.NewTicker(time.Second * 15)
	defer ticker.Stop()
	for {
		select {
		case cmd := <-server.cmds:
			server.processCommand(cmd)
			server.tryEvict()
		case <-ticker.C:
			server.pruneExpiredKeys()
		}
	}
}

func (server *Server) partitioner(key string) int {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	i := int(hash.Sum32()) % len(server.dicts)
	return i
}

func (server *Server) getKey(shardId int, key string) (Value, bool) {
	value, ok := server.dicts[shardId][key]
	if !ok {
		return Value{}, false
	}
	// lazy prune
	if ddl, ok := server.expire[shardId][key]; ok && time.Now().UnixNano() >= ddl {
		server.delKey(shardId, key, value)
		return Value{}, false
	}
	value.last = int32(time.Now().Unix())
	value.count++
	return value, true
}

func (server *Server) setKey(shardId int, key string, value Value) {
	server.dicts[shardId][key] = value
}

func (server *Server) delKey(shardId int, key string, value Value) {
	delete(server.dicts[shardId], key)
	delete(server.expire[shardId], key)
	Free(value.Bytes)
}

// fixme check arguments length is valid
// todo reuse objects that will be deleted
func (server *Server) processCommand(cmd Command) {
	args := cmd.args
	conn := cmd.conn
	switch string(args[0]) {
	case "set":
		key := string(args[1])
		shardId := server.partitioner(key)
		server.latchs[shardId].Lock()
		defer server.latchs[shardId].Unlock()

		value, exists := server.getKey(shardId, key)
		if i64, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
			if exists && value.typ == TypeString && StringEncoding(value.Bytes) == StringEncodingInteger {
				StringSetInt(value.Bytes, i64)
			} else {
				if exists {
					Free(value.Bytes)
				}
				size := 1 + 8
				value := Value{
					Bytes:   Malloc(size),
					typ:     TypeString,
					last:    0,
					count:   0,
					padding: 0,
				}
				server.setKey(shardId, key, value)
				StringSetInt(value.Bytes, i64)
			}
		} else {
			if exists {
				Free(value.Bytes)
			}
			size := 1 + len(args[2])
			value := Value{
				Bytes:   Malloc(size),
				typ:     TypeString,
				last:    0,
				count:   0,
				padding: 0,
			}
			server.setKey(shardId, key, value)
			StringSetString(value.Bytes, args[2])
		}
		fmt.Fprintln(conn, "OK")
	case "get":
		key := string(args[1])
		shardId := server.partitioner(key)
		server.latchs[shardId].Lock()
		defer server.latchs[shardId].Unlock()

		fmt.Println("get", key)
		value, ok := server.getKey(shardId, key)
		if !ok {
			fmt.Fprintln(conn, "key not found")
			return
		}
		switch value.typ {
		case TypeString:
			switch StringEncoding(value.Bytes) {
			case StringEncodingRaw:
				fmt.Fprintln(conn, StringGetBytes(value.Bytes))
			case StringEncodingString:
				fmt.Fprintln(conn, string(StringGetBytes(value.Bytes)))
			case StringEncodingInteger:
				fmt.Fprintf(conn, "%d (integer)\n", StringGetInt(value.Bytes))
			}
		}
	case "del":
		key := string(args[1])
		shardId := server.partitioner(key)
		server.latchs[shardId].Lock()
		defer server.latchs[shardId].Unlock()

		// todo victim
		value, ok := server.getKey(shardId, key)
		if !ok {
			fmt.Fprintln(conn, "key not found")
			return
		}
		server.delKey(shardId, key, value)
		fmt.Fprintln(conn, "OK")
	case "expire":
		key := string(args[1])
		shardId := server.partitioner(key)
		server.latchs[shardId].Lock()
		defer server.latchs[shardId].Unlock()

		delay, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil || delay < 0 {
			fmt.Fprintln(conn, "invalid delay ms")
			return
		}
		_, ok := server.getKey(shardId, key)
		if !ok {
			fmt.Fprintln(conn, "key not found")
			return
		}
		if delay > 0 {
			ttd := time.Now().Add(time.Duration(delay) * time.Millisecond)
			server.expire[shardId][key] = ttd.UnixNano()
			fmt.Fprintf(conn, "(TTD: %v)\n", ttd)
		} else {
			delete(server.expire[shardId], key)
			fmt.Fprintln(conn, "(TTD: never forever)")
		}
	case "ttl":
		key := string(args[1])
		shardId := server.partitioner(key)
		server.latchs[shardId].Lock()
		defer server.latchs[shardId].Unlock()

		if _, ok := server.getKey(shardId, key); !ok {
			fmt.Fprintln(conn, "key not found")
			return
		}
		nano := server.expire[shardId][key]
		ttl := time.Unix(0, nano).Sub(time.Now()).Truncate(time.Millisecond)
		fmt.Fprintf(conn, "%v\n", ttl)

	case "incr":
		key := string(args[1])
		shardId := server.partitioner(key)
		server.latchs[shardId].Lock()
		defer server.latchs[shardId].Unlock()

		delta, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			fmt.Fprintln(conn, "invalid delta")
			return
		}

		value, ok := server.getKey(shardId, key)
		if !ok {
			fmt.Fprintln(conn, "key not found")
			return
		}
		if value.typ != TypeString || StringEncoding(value.Bytes) != StringEncodingInteger {
			fmt.Fprintln(conn, "key is not integer")
			return
		}

		i := StringIncr(value.Bytes, delta)
		fmt.Fprintf(conn, "%d (integer)\n", i)
	case "status":
		fmt.Fprintf(conn, "Options:%+v\n", server.Options)
		fmt.Fprintf(conn, "OffHeapSize: %d\n", offHeapSize)
	default:
		fmt.Fprintln(conn, "invalid command")
	}
}

func (server *Server) pruneExpiredKeys() {
	fmt.Println("pruning expired keys ...")
	keysToDelete := make([]map[string]Value, len(server.dicts))
	for i := 0; i < len(server.dicts); i++ {
		keysToDelete[i] = make(map[string]Value)
	}

	var counter int
	for shardId, dict := range server.dicts {
		expire := server.expire[shardId]
		for key, value := range dict {
			if counter++; counter >= 20 {
				break
			}
			if ddl, ok := expire[key]; ok && time.Now().UnixMilli() >= ddl {
				keysToDelete[shardId][key] = value
			}
		}
	}

	counter = 0
	for shardId, keys := range keysToDelete {
		for key, value := range keys {
			fmt.Printf("key %s is pruned\n", key)
			server.delKey(shardId, key, value)
			counter++
		}
	}

	if counter >= 5 {
		server.pruneExpiredKeys()
	}
}
