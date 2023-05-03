package godis

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
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
				case *LinkedList:
					values := make([]string, v.length)
					for i, elem := 0, v.head; elem != nil; i, elem = i+1, elem.next {
						values[i] = string(elem.value)
					}
					fmt.Fprintln(conn, "[", strings.Join(values, ", "), "]")
				default:
					fmt.Fprintln(conn, v)
				}
			} else {
				fmt.Fprintln(conn, "key not found")
			}
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
		case "lpush":
			key, value := string(args[1]), args[2]
			var ll *LinkedList
			v, ok := server.dict[key]
			if !ok {
				ll = new(LinkedList)
				server.dict[key] = ll
			} else if ll, ok = v.(*LinkedList); !ok {
				fmt.Fprintln(conn, "key is not linked list")
				continue
			}
			n := ll.Push(value)
			fmt.Fprintln(conn, "(", n, ")")
		case "lpop":
			key := string(args[1])
			v, ok := server.dict[key]
			if !ok {
				fmt.Fprintln(conn, "key not found")
				continue
			}
			ll, ok := v.(*LinkedList)
			if !ok {
				fmt.Fprintln(conn, "key is not linked list")
				continue
			}
			value, n := ll.Pop()
			if n < 0 {
				fmt.Fprintln(conn, "empty linked list")
			} else {
				fmt.Fprintln(conn, string(value), "(", n, ")")
			}
		default:
			fmt.Fprintln(conn, "invalid command")
		}
	}
}
