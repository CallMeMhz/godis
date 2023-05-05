package godis

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
)

type Server struct {
	lis  net.Listener
	dict map[string]any
	cmds chan Command
}

func NewServer(addr string) (*Server, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	server := new(Server)
	server.lis = listener
	server.dict = make(map[string]any)
	server.cmds = make(chan Command)
	return server, nil
}

func (server *Server) Serve() error {
	go server.processCommand()
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

// fixme check arguments length is valid
// todo reuse objects that will be deleted
func (server *Server) processCommand() {
	for cmd := range server.cmds {
		args := cmd.args
		conn := cmd.conn
		switch string(args[0]) {
		case "set":
			key, value := string(args[1]), args[2]
			fmt.Println("set", key, value)
			if i64, err := strconv.ParseInt(string(value), 10, 64); err == nil {
				server.dict[key] = i64
			} else {
				server.dict[key] = value
			}
			fmt.Fprintln(conn, "OK")
		case "get":
			key := string(args[1])
			fmt.Println("get", key)
			if value, ok := server.dict[key]; ok {
				switch v := value.(type) {
				case int64:
					fmt.Fprintln(conn, v, "(integer)")
				case string:
					fmt.Fprintln(conn, v)
				case []byte:
					fmt.Fprintln(conn, string(v))
				case List:
					values := v.GetAll()
					fmt.Fprintf(conn, "[")
					for i, value := range values {
						if i > 0 {
							fmt.Fprint(conn, ", ")
						}
						fmt.Fprint(conn, string(value))
					}
					fmt.Fprint(conn, "]\n")
				case []int16:
					fmt.Fprintf(conn, "[")
					for i, value := range v {
						if i > 0 {
							fmt.Fprint(conn, ", ")
						}
						fmt.Fprint(conn, value)
					}
					fmt.Fprintf(conn, "]\n(%d)\n", len(v))
				case []int32:
					fmt.Fprintf(conn, "[")
					for i, value := range v {
						if i > 0 {
							fmt.Fprint(conn, ", ")
						}
						fmt.Fprint(conn, value)
					}
					fmt.Fprintf(conn, "]\n(%d)\n", len(v))
				case []int64:
					fmt.Fprintf(conn, "[")
					for i, value := range v {
						if i > 0 {
							fmt.Fprint(conn, ", ")
						}
						fmt.Fprint(conn, value)
					}
					fmt.Fprintf(conn, "]\n(%d)\n", len(v))
				default:
					fmt.Fprintln(conn, v)
				}
			} else {
				fmt.Fprintln(conn, "key not found")
			}
		case "del":
			key := string(args[1])
			if _, ok := server.dict[key]; !ok {
				fmt.Fprintln(conn, 0)
				continue
			}
			delete(server.dict, key)
			fmt.Fprintln(conn, 1)
		case "incr":
			key := string(args[1])
			delta, err := strconv.ParseInt(string(args[2]), 10, 64)
			if err != nil {
				fmt.Fprintln(conn, "invalid delta")
				continue
			}
			v, ok := server.dict[key]
			if !ok {
				fmt.Fprintln(conn, "key not found")
				continue
			}
			i64, ok := v.(int64)
			if !ok {
				fmt.Println(conn, "key is not integer")
				continue
			}
			server.dict[key] = i64 + delta
			fmt.Fprintln(conn, "OK")
		case "push", "rpush":
			key, value := string(args[1]), args[2]
			var l List
			v, ok := server.dict[key]
			if !ok {
				l = new(QuickList)
				server.dict[key] = l
			} else if l, ok = v.(List); !ok {
				fmt.Fprintln(conn, "key is not a list")
				continue
			}
			l.Push(value)
			fmt.Fprintf(conn, "(%d)\n", l.Size())
		case "pop", "rpop":
			key := string(args[1])
			v, ok := server.dict[key]
			if !ok {
				fmt.Fprintln(conn, "key not found")
				continue
			}
			ll, ok := v.(List)
			if !ok {
				fmt.Fprintln(conn, "key is not linked list")
				continue
			}
			value := ll.Pop()
			if value == nil {
				fmt.Fprintln(conn, "empty linked list")
			} else {
				fmt.Fprintf(conn, "%s\n(%d)\n", string(value), ll.Size())
			}
		case "len":
			key := string(args[1])
			v, ok := server.dict[key]
			if !ok {
				fmt.Fprintln(conn, "key not found")
				continue
			}
			ll, ok := v.(List)
			if !ok {
				fmt.Fprintln(conn, "key is not linked list")
				continue
			}
			fmt.Fprintln(conn, ll.Size())
		case "sadd":
			key, value := string(args[1]), args[2]
			if len(args) > 8 {
				fmt.Fprintln(conn, "value is too big")
				continue
			}
			i64, err := strconv.ParseInt(string(value), 10, 64)
			if err != nil {
				padding := 8 - len(value)
				value = append(make([]byte, padding), value...)
				i64 = int64(binary.BigEndian.Uint64(value))
			}
			fmt.Println("sadd", key, i64)
			is, ok := server.dict[key]
			if !ok {
				server.dict[key] = createIntset(i64)
				fmt.Fprintln(conn, "(1)")
				continue
			}
			switch is.(type) {
			case []int16, []int32, []int64:
				is = intsetAdd(is, i64)
				server.dict[key] = is
				fmt.Fprintf(conn, "(%d)\n", sizeOfIntset(is))
			default:
				fmt.Fprintf(conn, "key is not a set")
			}
		case "sdel":
			key, value := string(args[1]), args[2]
			if len(args) > 8 {
				fmt.Fprintln(conn, "value is too big")
				continue
			}
			i64, err := strconv.ParseInt(string(value), 10, 64)
			if err != nil {
				padding := 8 - len(value)
				value = append(make([]byte, padding), value...)
				i64 = int64(binary.BigEndian.Uint64(value))
			}
			is, ok := server.dict[key]
			if !ok {
				fmt.Fprintln(conn, "key not found")
				continue
			}
			switch is.(type) {
			case []int16, []int32, []int64:
				is = intsetDel(is, i64)
				server.dict[key] = is
				fmt.Fprintf(conn, "(%d)\n", sizeOfIntset(is))
			default:
				fmt.Fprintf(conn, "key is not a set")
			}

		case "slen":
			key := string(args[1])
			is, ok := server.dict[key]
			if !ok {
				fmt.Fprintln(conn, "key not found")
				continue
			}
			if encodingOfIntset(is) == 0 {
				fmt.Fprintln(conn, "key is not set")
				continue
			}
			size := sizeOfIntset(is)
			fmt.Fprintf(conn, "(%d)\n", size)
		default:
			fmt.Fprintln(conn, "invalid command")
		}
	}
}
