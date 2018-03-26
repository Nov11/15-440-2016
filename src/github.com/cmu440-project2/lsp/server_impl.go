// Contains the implementation of a LSP server.

package lsp

import (
	"errors"
	"github.com/cmu440-project2/lspnet"
	"strconv"
	"fmt"
	"sync"
	"os"
)

type server struct {
	address                          *lspnet.UDPAddr
	connection                       *lspnet.UDPConn
	nextConnectionId                 int
	connectIdList                    map[int]*client
	address2ConnectionId             map[string]int
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
	clientExit                       chan int
	clientNumber                     int
	name                             string
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {

	e13 := errors.New("1234567")
	txt := encodeError(&e13)
	e23 := errors.New("")
	decodeError(txt, &e23)
	fmt.Printf("%v\n", e23)

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
		address2ConnectionId:             make(map[string]int),
		closing:                          false,
		signalReaderClosed:               make(chan error),
		dataIncomingPacket:               make(chan *Packet, 1000),
		cmdGetMsg:                        make(chan int),
		dataGetMsg:                       make(chan *Message, 1000),
		clientReceivedDataIncomingPacket: make(chan *Packet, 1000),
		cmdGetClient:                     make(chan int),
		dataClient:                       make(chan *client),
		clientExit:                       make(chan int),
		name:                             "server",
	}

	go readSocketWithAddress(ret.connection, ret.dataIncomingPacket, ret.signalReaderClosed, ret.name)
	go func() {
		defer func() { fmt.Println("!!!!! server main loop exit") }()
		for {
			select {
			case packet := <-ret.dataIncomingPacket:
				var localPacketList []*Packet
				localPacketList = append(localPacketList, packet)
				localPacketList = append(localPacketList, readAllPackets(ret.dataIncomingPacket)...)
				fmt.Println(ret.name + "batch read " + strconv.Itoa(len(localPacketList)) + " packets:dataIncomingPacket")
				for _, packet = range localPacketList {
					fmt.Printf("%s received packet:%v\n", ret.name, packet)
					msg := packet.msg
					addr := packet.addr
					if msg.Type == MsgConnect {
						if _, exist := ret.address2ConnectionId[addr.String()]; !exist {
							id := ret.nextConnectionId
							ret.nextConnectionId++
							ret.address2ConnectionId[addr.String()] = id
							ret.connectIdList[id] = createNewClient(id, params, addr, conn, nil, nil, ret.clientReceivedDataIncomingPacket, true, ret.clientExit, ret.name+strconv.Itoa(ret.clientNumber))
							ret.clientNumber++
						}
						id := ret.address2ConnectionId[addr.String()]
						c := ret.connectIdList[id]
						c.WriteImpl(nil, MsgAck)
					} else {
						c, ok := ret.connectIdList[msg.ConnID]
						if !ok {
							//ignore
							fmt.Printf("ignore packet %v as there's no related worker", packet)
							continue
						}
						c.appendPacket(packet)
					}
				}

			case connId := <-ret.cmdGetClient:
				c, ok := ret.connectIdList[connId]
				if !ok {
					ret.dataClient <- nil
				}
				ret.dataClient <- c
			case <-ret.cmdGetMsg:
				if len(ret.dataGetMsg) > 0 {
					continue
				}
				ret.reqNewPacket = true
			ENDGETMSG:
				for len(ret.receivedDataPacket) > 0 {
					ret.reqNewPacket = false
					p := ret.receivedDataPacket[0]
					ret.receivedDataPacket = ret.receivedDataPacket[1:]
					select {
					case ret.dataGetMsg <- p.msg:
					default:
						break ENDGETMSG
					}

				}
			case p := <-ret.clientReceivedDataIncomingPacket:
				localPacketList := []*Packet{p}
				localPacketList = append(localPacketList, readAllPackets(ret.clientReceivedDataIncomingPacket)...)
				fmt.Println("batch read " + strconv.Itoa(len(localPacketList)) + " packets : clientReceivedIncomingPacket")
				for _, p = range localPacketList {
					msg := p.msg
					fmt.Println(ret.name + " " + msg.String() + " message : clientReceivedIncomingPacket")
					if ret.reqNewPacket {
						ret.reqNewPacket = false
						ret.dataGetMsg <- msg
					} else {
						ret.receivedDataPacket = append(ret.receivedDataPacket, p)
					}
				}

			case no := <-ret.clientExit:
				c, ok := ret.connectIdList[no]
				if !ok {
					fmt.Println("closing unexisted client!")
				}
				addr := c.remoteAddress
				delete(ret.address2ConnectionId, addr.String())
				delete(ret.connectIdList, no)
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
	if msg.Type != MsgData {
		fmt.Printf("%v\n", msg)
		os.Exit(1)
	}

	if msg.Type == -1 {
		str := string("")
		decodeString(msg.Payload, &str)
		return msg.ConnID, nil, errors.New(str)
	}
	breakThis := false
	if (len(msg.Payload) == 0) {
		breakThis = true
	}
	fmt.Printf("%s read %v payload len 0:%v\n", s.name, msg, breakThis)
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
