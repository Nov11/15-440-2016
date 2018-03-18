// Contains the implementation of a LSP server.

package lsp

import (
	"errors"
	"github.com/cmu440-project2/lspnet"
	"strconv"
	"fmt"
	"sync"
)

type server struct {
	address                          *lspnet.UDPAddr
	connection                       *lspnet.UDPConn
	nextConnectionId                 int
	connectIdList                    map[int]*client
	address2ConnectionId             map[lspnet.UDPAddr]int
	closing                          bool
	signalReaderClosed               chan error
	dataIncomingPacket               chan *Packet
	mtx                              sync.Mutex
	cmdGetClient                     chan int
	dataClient                       chan *client
	cmdGetMsg                        chan int
	receivedDataPacket               []*Packet
	reqNewPacket                     bool
	dataGetMsg                       chan *Message
	clientReceivedDataIncomingPacket chan *Packet
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {

	address, err := lspnet.ResolveUDPAddr("udp", "localhost:"+strconv.Itoa(port))
	checkError(err)

	conn, err := lspnet.ListenUDP("udp", address)
	if err != nil {
		return nil, err
	}
	ret := server{
		address:                          address,
		connection:                       conn,
		nextConnectionId:                 1,
		connectIdList:                    make(map[int]*client),
		address2ConnectionId:             make(map[lspnet.UDPAddr]int),
		closing:                          false,
		signalReaderClosed:               make(chan error),
		dataIncomingPacket:               make(chan *Packet),
		cmdGetMsg:                        make(chan int),
		dataGetMsg:                       make(chan *Message),
		clientReceivedDataIncomingPacket: make(chan *Packet),
	}

	go readSocketWithAddress(ret.connection, ret.dataIncomingPacket, ret.signalReaderClosed)
	go func() {
		for {
			select {
			case packet := <-ret.dataIncomingPacket:
				msg := packet.msg
				addr := packet.addr
				if msg.Type == MsgConnect {
					if _, exist := ret.address2ConnectionId[*addr]; !exist {
						id := ret.nextConnectionId
						ret.nextConnectionId++
						ret.address2ConnectionId[*addr] = id
						ret.connectIdList[id] = createNewClient(id, params, address, conn, nil, nil, ret.clientReceivedDataIncomingPacket)
					}
					id := ret.address2ConnectionId[*addr]
					c := ret.connectIdList[id]
					c.Write(encode(NewAck(id, 0)))
				} else {
					c, ok := ret.connectIdList[msg.ConnID]
					if !ok {
						//ignore
						fmt.Printf("ignore packet %v as there's no related worker", packet)
						continue
					}
					c.appendPacket(packet)
				}
			case connId := <-ret.cmdGetClient:
				c, ok := ret.connectIdList[connId]
				if !ok {
					ret.dataClient <- nil
				}
				ret.dataClient <- c
			case <-ret.cmdGetMsg:
				ret.reqNewPacket = true
				if len(ret.receivedDataPacket) > 0 {
					ret.reqNewPacket = false
					p := ret.receivedDataPacket[0]
					ret.receivedDataPacket = ret.receivedDataPacket[1:]
					ret.dataGetMsg <- p.msg
				}
			case p := <-ret.clientReceivedDataIncomingPacket:
				msg := p.msg
				if ret.reqNewPacket {
					ret.reqNewPacket = false
					ret.dataGetMsg <- msg
				} else {
					ret.receivedDataPacket = append(ret.receivedDataPacket, p)
				}
			}

		}
	}()
	return &ret, nil
}

func (s *server) Read() (int, []byte, error) {
	if s.closing {
		return 0, nil, errors.New("server closed")
	}
	s.cmdGetMsg <- 1
	msg := <-s.dataGetMsg
	if msg.Type == -1 {
		var err error
		decodeInterface(msg.Payload, &err)
		return msg.ConnID, nil, err
	}
	return msg.ConnID, msg.Payload, nil
}

func (s *server) Write(connID int, payload []byte) error {
	s.cmdGetClient <- connID
	c := <-s.dataClient
	return c.Write(payload)
}

func (s *server) CloseConn(connID int) error {
	s.cmdGetClient <- connID
	c := <-s.dataClient
	return c.Close()
}

func (s *server) Close() error {
	return errors.New("not yet implemented")
}
