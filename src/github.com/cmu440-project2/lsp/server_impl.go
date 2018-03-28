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

var SERVERCHANNELLENGTH int = 5000

type server struct {
	address                          *lspnet.UDPAddr
	connection                       *lspnet.UDPConn
	nextConnectionId                 int
	connectIdList                    map[int]*client
	address2ConnectionId             map[string]int
	closing                          bool
	signalReaderClosed               chan error
	dataIncomingPacket               chan *Packet
	mtx                              sync.RWMutex
	receivedDataPacket               []*Packet
	reqNewPacket                     bool
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
		dataIncomingPacket:               make(chan *Packet, SERVERCHANNELLENGTH),
		clientReceivedDataIncomingPacket: make(chan *Packet, SERVERCHANNELLENGTH),
		clientExit:                       make(chan int),
		name:                             "server",
	}

	go readSocketWithAddress(ret.connection, ret.dataIncomingPacket, ret.signalReaderClosed, ret.name)
	go func() {
		defer func() { fmt.Println("!!!!! server main loop exit") }()
		for {
			select {
			case packet := <-ret.dataIncomingPacket:
				//var localPacketList []*Packet
				//localPacketList = append(localPacketList, packet)
				//localPacketList = append(localPacketList, readAllPackets(ret.dataIncomingPacket)...)
				//fmt.Println(ret.name + " batch read " + strconv.Itoa(len(localPacketList)) + " packets:dataIncomingPacket")
				//for _, item := range localPacketList {
				//	go func(packet *Packet) {
				fmt.Printf("%s received packet:%v\n", ret.name, packet)
				msg := packet.msg
				addr := packet.addr

				if msg.Type == MsgConnect {
					ret.mtx.Lock()
					if _, exist := ret.address2ConnectionId[addr.String()]; !exist {
						id := ret.nextConnectionId
						ret.nextConnectionId++
						ret.address2ConnectionId[addr.String()] = id
						ret.connectIdList[id] = createNewClient(id, params, addr, conn, nil, nil, ret.clientReceivedDataIncomingPacket, true, ret.clientExit, ret.name+strconv.Itoa(ret.clientNumber), nil)
						ret.clientNumber++
					}
					id := ret.address2ConnectionId[addr.String()]
					c := ret.connectIdList[id]
					c.WriteImpl(nil, MsgAck)
					ret.mtx.Unlock()
				} else {
					ret.mtx.RLock()
					c, ok := ret.connectIdList[msg.ConnID]
					ret.mtx.RUnlock()
					if !ok {
						//ignore
						fmt.Printf("ignore packet %v as there's no related worker", packet)
						return
					}
					c.appendPacket(packet)
				}
				//}(item)
				//}

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
	fmt.Println("server [Read] called")
	if s.closing {
		return 0, nil, errors.New("server closed")
	}
	p := <-s.clientReceivedDataIncomingPacket
	msg := p.msg
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
	fmt.Printf("[read interface]%s read %v payload len 0:%v msg pending:%v\n", s.name, msg, breakThis, len(s.clientReceivedDataIncomingPacket))
	return msg.ConnID, msg.Payload, nil
}

func (s *server) Write(connID int, payload []byte) error {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	c, ok := s.connectIdList[connID]
	if !ok {
		return errors.New("connection not exists : id :" + strconv.Itoa(connID))
	}
	go func() { c.Write(payload) }()
	return nil
}

func (s *server) CloseConn(connID int) error {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	c, ok := s.connectIdList[connID]
	if !ok {
		return errors.New("connection not exists : id :" + strconv.Itoa(connID))
	}
	go func() { c.Close() }()
	return nil
}

func (s *server) Close() error {
	return errors.New("not yet implemented")
}
