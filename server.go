package godis

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
)

type Server struct {
	lis  net.Listener
	dict map[string]string
	cmds chan Command
}

func NewServer(addr string) (*Server, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	server := new(Server)
	server.lis = listener
	server.dict = make(map[string]string)
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
			server.dict[key] = value
			fmt.Println("set", key, value)
			fmt.Fprintln(conn, "OK")
		case "get":
			key := args[1]
			fmt.Println("get", key)
			if value, ok := server.dict[key]; ok {
				fmt.Fprintln(conn, value)
			} else {
				fmt.Fprintln(conn, "key not found")
			}
		default:
			fmt.Fprintln(conn, "invalid command")
		}
	}
}
