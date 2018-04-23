package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/cmu440-project2/lsp"
	"github.com/cmu440-project2/bitcoin"
	"math"
	"time"
)

type server struct {
	lspServer      lsp.Server
	minerList      []int
	client2Request map[int]bitcoin.Message
	task           map[uint64]int
	currentRequest int
	c              coin
}

type coin struct {
	hash   uint64
	nounce uint64
}

type readMsg struct {
	connId int
	binary []byte
	err    error
}

func startServer(port int) (*server, error) {
	s, err := lsp.NewServer(port, lsp.NewParams())
	return &server{lspServer: s, client2Request: make(map[int]bitcoin.Message), task: make(map[uint64]int)}, err
}

var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "log.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	// Usage: LOGF.Println() or LOGF.Printf()

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <port>", os.Args[0])
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be a number:", err)
		return
	}

	srv, err := startServer(port)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Server listening on port", port)

	defer srv.lspServer.Close()

	msgChannel := make(chan *readMsg)
	go read(msgChannel, srv.lspServer)
	for true {
		timer := time.After(time.Second * 1000)
		select {
		case incomingMsg := <-msgChannel:
			//deal with error
			if incomingMsg.err != nil {
				//client close, remove that client
				//1.is this a miner?
				if b, _ := remove(srv.minerList, incomingMsg.connId); !b {
					//2.is this a client?
					delete(srv.client2Request, incomingMsg.connId)
					if srv.currentRequest == incomingMsg.connId {
						srv.currentRequest = 0
						srv.task = nil
						srv.c = coin{}
					}
				}
				continue
			}

			//no error
			var msg bitcoin.Message
			bitcoin.Decode(incomingMsg.binary, &msg)

			if msg.Type == bitcoin.Join {
				srv.minerList = append(srv.minerList, incomingMsg.connId)
			} else if msg.Type == bitcoin.Request {
				srv.client2Request[incomingMsg.connId] = msg
			} else {
				if _, ok := srv.client2Request[srv.currentRequest]; ok {
					if srv.c.hash > msg.Hash {
						srv.c.hash = msg.Hash
						srv.c.nounce = msg.Nonce
					}
					delete(srv.task, msg.Nonce)
					if len(srv.task) == 0 {
						//write result to client
						p := bitcoin.NewResult(srv.c.hash, srv.c.nounce)
						srv.lspServer.Write(srv.currentRequest, bitcoin.Encode(p))
						srv.currentRequest = 0
						srv.task = nil
						srv.c = coin{}
						timer = time.After(100 * time.Second)

					}
				}
			}

			if srv.currentRequest == 0 {
				next := 0
				for k := range srv.client2Request {
					next = k
					break
				}
				if next == 0 {
					continue
				}
				srv.currentRequest = next
				srv.task = make(map[uint64]int)
				m := srv.client2Request[next]
				for i := m.Lower; i <= m.Upper; i++ {
					srv.task[i] = 0
				}
				srv.c = coin{hash: math.MaxUint64, nounce: math.MaxUint64}

				keyList := keys(srv.task)
				sendMsg(srv.minerList, keyList, srv.lspServer, srv.client2Request[srv.currentRequest].Data)
				timer = time.After(time.Millisecond * 500)
			}

		case <-timer:
			timer = time.After(time.Millisecond * 500)
			keyList := keys(srv.task)
			sendMsg(srv.minerList, keyList, srv.lspServer, srv.client2Request[srv.currentRequest].Data)
		}

	}
}

func keys(m map[uint64]int) []uint64 {
	var result [] uint64
	for k := range m {
		result = append(result, k)
	}
	return result
}

func remove(array [] int, v int) (bool, int) {
	for i := 0; i < len(array); i++ {
		if array[i] == v {
			array = append(array[:i], array[i+1:]...)
			return true, i
		}
	}
	return false, -1
}

func read(msgChannel chan *readMsg, server lsp.Server) {
	for true {
		select {
		default:
			connId, binary, err := server.Read()
			msgChannel <- &readMsg{connId: connId, binary: binary, err: err}
		}
	}
}

func sendMsg(miner []int, task []uint64, server lsp.Server, data string) {
	if len(miner) == 0 {
		return
	}
	idx := 0
	for i := 0; i < len(task); i++ {
		err := server.Write(miner[idx], bitcoin.Encode(bitcoin.NewRequest(data, task[i], task[i])))
		if err != nil {
			i --
			remove(miner, miner[idx])
		} else {
			idx++
			idx = idx % len(task)
		}
	}
}
