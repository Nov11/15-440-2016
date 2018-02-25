// Implementation of a KeyValueServer. Students should write their code in this file.

package p0

import (
	"net"
	"os"
	"fmt"
	"bufio"
	"strings"
	"strconv"
)

type keyValueServer struct {
	port      int
	close     chan int
	clients   chan int
	kvPut     chan string
	kvGet     chan string
	broadcast chan string
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	return &keyValueServer{clients: make(chan int)		, close: make(chan int)		, kvPut: make(chan string)		, kvGet: make(chan string), broadcast: make(chan string)}
}

func (kvs *keyValueServer) Start(port int) error {
	kvs.port = port
	tmp := ":" + strconv.Itoa(port)
	listener, err := net.Listen("tcp", tmp)
	if err != nil {
		return err
	}
	go func(acceptor net.Listener) {

		clientSet := make(map[net.Conn]chan string)
		exitChan := make(chan net.Conn)
		connected := make(chan net.Conn)
		go func(acceptor net.Listener) {
			defer acceptor.Close()
			for {
				conn, err := acceptor.Accept()
				if err != nil {
					fmt.Fprintf(os.Stderr, "Accept connection error: %s", err.Error())
				} else {
					connected <- conn
				}
			}

		}(acceptor)
		for {
			select {
			case <-kvs.close:
				//shutdown children
				for _, v := range clientSet {
					select {
					case v <- "close":
					default:
					}
				}
				return
			case conn := <-connected:
				clientSet[conn] = make(chan string)
				go worker(conn, clientSet[conn], kvs.kvPut, kvs.kvGet, exitChan)
			case <-kvs.clients:
				kvs.clients <- len(clientSet)
			case c := <-exitChan:
				delete(clientSet, c)
			case msg := <-kvs.broadcast:
				for _, v := range clientSet {
					select {
					case v <- msg:
					default:
					}
				}
			}
		}
	}(listener)

	go func(putMsg chan string, getMsg chan string) {
		init_db()
		for {
			select {
			case k := <-getMsg:
				v := get(k)
				sv := string(v)
				ret := k + "," + sv

				if len(ret) == len(k) {
					fmt.Printf("%v", kvstore)
				}
				fmt.Printf("get k: %v v :%v ret : %v\n", k, v, ret)
				kvs.broadcast <- ret
			case msg := <-putMsg:
				cmds := strings.Split(msg, ",")
				k := cmds[0]
				v := cmds[1]
				put(k, []byte(v))
				fmt.Printf("put k: %v v :%v\n", k, string(get(k)))
			}
		}
	}(kvs.kvPut, kvs.kvGet)
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

func worker(conn net.Conn, msgChannel chan string, kvPut chan string, kvGet chan string, exitChan chan net.Conn) {
	defer conn.Close()
	defer func() { exitChan <- conn }()
	rb := make(chan string, 500)
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	line := make(chan string)
	go func(reader *bufio.ReadWriter) {
		for {
			msg, _, err := rw.ReadLine()

			line <- string(msg)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s", err.Error())
				return
			}
		}
	}(rw)
	for {
		select {
		case msg := <-msgChannel:
			if msg == "close" {
				return
			}
			rb <- msg
		case v := <-rb:
			fmt.Printf("worker write out :%v", v)
			rw.WriteString(v)
		case msg := <-line:
			if msg == "" {
				return
			}
			cmds := strings.Split(msg, ",")
			switch {
			case cmds[0] == "put":
				kvPut <- cmds[1] + "," + cmds[2]
			case cmds[0] == "get":
				kvGet <- cmds[1]
				//out := <-kvGet
				//fmt.Printf("reply get: [%v]\n", out)
				//out = out + "\n"
				//_, writeErr := rw.Write([]byte(out))
				//rw.Flush()
				//if writeErr != nil {
				//	return
				//}
			}
		}
	}
}
