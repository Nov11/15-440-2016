// Implementation of a KeyValueServer. Students should write their code in this file.

package p0

import (
	"net"
	"os"
	"fmt"
	"bufio"
	"strings"
)

type keyValueServer struct {
	port    int
	close   chan int
	clients chan int
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	return &keyValueServer{clients: make(chan int), close: make(chan int)}
}

func (kvs *keyValueServer) Start(port int) error {
	kvs.port = port
	listener, err := net.Listen("tcp", ":"+string(port))
	if err != nil {
		return err
	}
	go func(acceptor net.Listener) {
		defer acceptor.Close()
		clientSet := make(map[net.Conn]chan int)
		for {
			select {
			case <-kvs.close:
				//shutdown children
				for _, v := range clientSet {
					v <- 1
					<-v
				}
			case conn, err := acceptor.Accept():
				if err != nil {
					fmt.Fprint(os.Stderr, "Accept connection error: %s", err.Error())
				} else {
					clientSet[conn] = make(chan int)
					go
				}
			case <-kvs.clients:
				kvs.clients <- len(clientSet)
			}
		}
	}(listener)
	return nil
}

func (kvs *keyValueServer) Close() {
	kvs.close <- 1
}

func (kvs *keyValueServer) Count() int {
	kvs.clients <- 1
	return <-kvs.clients
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

type ringbuffer struct{
	buffer []string
}
func worker(conn net.Conn, msgChannel chan string) {
	defer func() { msgChannel <- "" }()
	defer conn.Close()
	rb := ringbuffer{buffer:make([]string, 0)}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	for {
		select {
		case msg := <-msgChannel:
			if msg == ""{
				return
			}
			rw.WriteString(msg)
		case line, _, err := rw.ReadLine():
			if err != nil{
				return
			}
			msg := string(line)
			cmds := strings.Split(msg, ",")
			switch cmds[0]{
			case "put":
			case "get":
			}
		}
	}

}
