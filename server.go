package godis

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

type Server struct {
	lis    net.Listener
	dict   map[string]Value
	expire map[string]int64
	cmds   chan Command

	Options struct {
		Eviction struct {
			Policy         int
			MaxOffHeapSize int
		}
	}
}

func NewServer(addr string) (*Server, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	server := new(Server)
	server.lis = listener
	server.dict = make(map[string]Value)
	server.expire = make(map[string]int64)
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
	conn net.Conn
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

func (server *Server) getKey(key string) (Value, bool) {
	value, ok := server.dict[key]
	if !ok {
		return Value{}, false
	}
	// lazy prune
	if ddl, ok := server.expire[key]; ok && time.Now().UnixMilli() >= ddl {
		delete(server.dict, key)
		delete(server.expire, key)
		return Value{}, false
	}
	value.last = int32(time.Now().Unix())
	value.count++
	return value, true
}

// fixme check arguments length is valid
// todo reuse objects that will be deleted
func (server *Server) processCommand(cmd Command) {
	args := cmd.args
	conn := cmd.conn
	switch string(args[0]) {
	case "set":
		key := string(args[1])
		value, exists := server.dict[key]
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
				server.dict[key] = value
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
			server.dict[key] = value
			StringSetString(value.Bytes, args[2])
		}
		fmt.Fprintln(conn, "OK")
	case "get":
		key := string(args[1])
		fmt.Println("get", key)
		value, ok := server.getKey(key)
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
		// todo victim
		value, ok := server.getKey(key)
		if !ok {
			fmt.Fprintln(conn, "key not found")
			return
		}
		delete(server.dict, key)
		delete(server.expire, key)
		Free(value.Bytes)
		fmt.Fprintln(conn, "OK")
	case "expire":
		key := string(args[1])
		delay, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil || delay < 0 {
			fmt.Fprintln(conn, "invalid delay ms")
			return
		}
		_, ok := server.getKey(key)
		if !ok {
			fmt.Fprintln(conn, "key not found")
			return
		}
		if delay > 0 {
			ttd := time.Now().Add(time.Duration(delay) * time.Millisecond)
			server.expire[key] = ttd.UnixMilli()
			fmt.Fprintf(conn, "(TTD: %v)\n", ttd)
		} else {
			delete(server.expire, key)
			fmt.Fprintln(conn, "(TTD: never forever)")
		}
	case "incr":
		key := string(args[1])
		delta, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			fmt.Fprintln(conn, "invalid delta")
			return
		}

		value, ok := server.getKey(key)
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
	var deletedKeys []string
	var counter int
	for key := range server.dict {
		if counter++; counter >= 20 {
			break
		}
		if ddl, ok := server.expire[key]; ok && time.Now().UnixMilli() >= ddl {
			deletedKeys = append(deletedKeys, key)
		}
	}
	for _, key := range deletedKeys {
		fmt.Printf("key %s is pruned\n", key)
		delete(server.dict, key)
		delete(server.expire, key)
	}
	if len(deletedKeys) >= 5 {
		server.pruneExpiredKeys()
	}
}
