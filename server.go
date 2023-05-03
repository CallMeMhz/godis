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
		args := strings.Split(string(buf), " ")
		server.cmds <- Command{args, conn}
	}
}

type Command struct {
	args []string
	conn net.Conn
}

func (server *Server) processCommand() {
	for cmd := range server.cmds {
		args := cmd.args
		conn := cmd.conn
		switch args[0] {
		case "set":
			key, value := args[1], args[2]
			fmt.Println("set", key, value)
			if i64, err := strconv.ParseInt(value, 10, 64); err == nil {
				server.dict[key] = i64
			} else {
				server.dict[key] = value
			}
			fmt.Fprintln(conn, "OK")
		case "get":
			key := args[1]
			fmt.Println("get", key)
			if value, ok := server.dict[key]; ok {
				if _, ok := value.(int64); ok {
					fmt.Fprintln(conn, value, "(integer)")
				} else {
					fmt.Fprintln(conn, value)
				}
			} else {
				fmt.Fprintln(conn, "key not found")
			}
		case "incr":
			key := args[1]
			delta, err := strconv.ParseInt(args[2], 10, 64)
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

		default:
			fmt.Fprintln(conn, "invalid command")
		}
	}
}
