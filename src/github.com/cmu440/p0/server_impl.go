// Implementation of a KeyValueServer. Students should write their code in this file.

package p0

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
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
	return &keyValueServer{clients: make(chan int), close: make(chan int), kvPut: make(chan string), kvGet: make(chan string), broadcast: make(chan string, 100)}
}

func (kvs *keyValueServer) Start(port int) error {
	kvs.port = port
	tmp := ":" + strconv.Itoa(port)
	listener, err := net.Listen("tcp", tmp)
	if err != nil {
		return err
	}
	go func(acceptor net.Listener) {
		//fmt.Println("main starter")
		clientSet := make(map[net.Conn]chan string)
		exitChan := make(chan net.Conn)
		connected := make(chan net.Conn)
		go func(acceptor net.Listener) {
			//fmt.Println("acceptor")
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
		cnt := 0
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
				clientSet[conn] = make(chan string, 500)
				go worker(conn, clientSet[conn], kvs.kvPut, kvs.kvGet, exitChan)
			case <-kvs.clients:
				kvs.clients <- len(clientSet)
			case c := <-exitChan:
				delete(clientSet, c)
			case msg := <-kvs.broadcast:
				//fmt.Printf("bc %d\n", cnt)

				for _, v := range clientSet {
					//prev := len(v)
					select {
					case v <- msg:
						//fmt.Printf("passed msg %d  prevlen: %d cur len : %d\n", cnt, prev, len(v))
					default:
						//fmt.Printf("!!!!!!!!!!!!!drop msg cur len:%d\n", len(v))
					}
				}
				cnt++
			}
		}
	}(listener)

	go func(putMsg chan string, getMsg chan string) {
		//fmt.Println("db access")
		init_db()
		cnt := 0
		for {
			select {
			case k := <-getMsg:
				v := get(k)
				sv := string(v)
				ret := k + "," + sv

				if len(ret) == len(k) {
					fmt.Printf("%v*********************\n", kvstore)
				}
				kvs.broadcast <- ret
				//fmt.Printf("broadcast : %d\n", cnt)
				cnt++
			case msg := <-putMsg:
				cmds := strings.Split(msg, ",")
				k := cmds[0]
				v := cmds[1]
				put(k, []byte(v))
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
	//fmt.Println("worker")
	defer conn.Close()
	defer func() { exitChan <- conn }()

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
			//bulk read
			var message []string
			message = append(message, msg)
			for msg != "" {
				select {
				case msg := <-msgChannel:
					message = append(message, msg)
				default:
					msg = ""
				}
			}
			for _, v := range message {
				rw.WriteString(v + "\n")
			}
			rw.Flush()
		case msg := <-line:
			//fmt.Printf("incomming : %v \n", msg)
			if msg == "" {
				return
			}
			cmds := strings.Split(msg, ",")
			switch {
			case cmds[0] == "put":
				kvPut <- cmds[1] + "," + cmds[2]
			case cmds[0] == "get":
				kvGet <- cmds[1]
			}
		}
	}
}
