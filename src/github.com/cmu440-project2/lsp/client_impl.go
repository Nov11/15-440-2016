// Contains the implementation of a LSP client.

package lsp

import (
	"errors"
	"fmt"
	"github.com/cmu440-project2/lspnet"
	"sync"
	"time"
	"strconv"
)

type client struct {
	connectionId                int
	connection                  *lspnet.UDPConn
	remoteAddress               *lspnet.UDPAddr
	params                      *Params
	mtx                         sync.Mutex
	cmdClientClose              chan CloseCmd
	clientHasFullyClosedDown    chan error
	dataIncomingPacket          chan *Packet
	receiveMessageQueue         []*Message
	readTimerReset              chan int
	writer                      *writerWithWindow
	closed                      bool
	closeReason                 string
	dataNewestMessage           chan *Message
	previousSeqNumReturnedToApp int
	signalWriterClosed          chan error
	signalReaderClosed          chan error
	ackForLastReceivedMessage   *Message
	result                      chan error
	dataPacketSideWay           chan *Packet
	shareSocket                 bool
	clientExit                  chan int
	name                        string
	dataMessageInThisEpoch      int
}

func (c *client) closeChannels() {
	close(c.cmdClientClose)
	close(c.clientHasFullyClosedDown)
	close(c.dataIncomingPacket)
	close(c.readTimerReset)
	close(c.dataNewestMessage)
	close(c.signalWriterClosed)
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params, name string) (*client, error) {
	address, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	//establish UDP connection to server
	conn, err := lspnet.DialUDP("udp", nil, address)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	//prepare protocol connection message
	msg := encode(NewConnect())
	//fire up read routine
	dataIncomingPacket := make(chan *Packet, 1000)
	signalReaderClosed := make(chan error)
	go readSocketWithAddress(conn, dataIncomingPacket, signalReaderClosed, name)
	//send connection message and wait for server's reply
	id, err := protocolConnect(msg, conn, params, dataIncomingPacket)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	fmt.Println("connected to server")
	ret := createNewClient(id, params, nil, conn, dataIncomingPacket, signalReaderClosed, nil, false, nil, name)
	return ret, nil
}

//maintain message in seq order. cause application needs message to be read in serial order
func (c *client) appendNewReceivedMessage(msg *Message) bool {
	if msg.SeqNum <= c.previousSeqNumReturnedToApp {
		return false
	}
	for i := 0; i < len(c.receiveMessageQueue); i++ {
		if c.receiveMessageQueue[i].SeqNum == msg.SeqNum {
			fmt.Println(c.name + " appendNewReceivedMessage ignore duplicate message with same seq num :" + strconv.Itoa(msg.SeqNum))
			return false
		} else if c.receiveMessageQueue[i].SeqNum > msg.SeqNum {
			fmt.Println(c.name + " appendNewReceivedMessage prepend seq num :" + strconv.Itoa(msg.SeqNum))
			tmp := c.receiveMessageQueue[i:]
			c.receiveMessageQueue = append(c.receiveMessageQueue[:i], msg)
			c.receiveMessageQueue = append(c.receiveMessageQueue, tmp...)
			return true
		}
	}
	fmt.Println(c.name + " appendNewReceivedMessage append seq num :" + strconv.Itoa(msg.SeqNum))
	c.receiveMessageQueue = append(c.receiveMessageQueue, msg)
	return true
}

//return next message if already received that message, or return nil
func (c *client) getNextMessage() *Message {
	if len(c.receiveMessageQueue) == 0 {
		return nil
	}
	for i := 0; i < len(c.receiveMessageQueue) && c.receiveMessageQueue[i].SeqNum <= c.previousSeqNumReturnedToApp+1; i++ {
		if c.receiveMessageQueue[i].SeqNum == c.previousSeqNumReturnedToApp+1 {
			ret := c.receiveMessageQueue[i]
			c.receiveMessageQueue = c.receiveMessageQueue[i+1:]
			c.previousSeqNumReturnedToApp++
			return ret
		}
	}

	return nil
}

func verify(msg *Message, c *client) bool {
	if msg.Type != MsgConnect && msg.Type != MsgData && msg.Type != MsgAck {
		fmt.Println("msg type incorrecct : " + string(msg.Type))
		return false
	}

	if c != nil && c.connectionId != 0 && msg.ConnID != c.connectionId {
		fmt.Println("msg connectionId invalid")
		return false
	}

	if msg.Size > len(msg.Payload) {
		fmt.Println("msg length > payload length")
		return false
	}

	if msg.Size < len(msg.Payload) {
		fmt.Println("msg length smaller than payload lenght, truncate payload")
		msg.Payload = msg.Payload[:msg.Size]
	}

	return true
}

func (c *client) ConnID() int {
	return c.connectionId
}

//how to read message while bg routine is closing or just shutdown ?
//at that time there's no routine exists to wait on cmd channel
//close all cmd channel before
func (c *client) Read() ([]byte, error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("cmd channel closed already")
		}
	}()
	msg, ok := <-c.dataNewestMessage
	if ok == true {
		return msg.Payload, nil
	}
	return nil, errors.New(c.closeReason)
}

func (c *client) Write(payload []byte) error {
	return c.WriteImpl(payload, MsgData)
}

func (c *client) WriteImpl(payload []byte, msgType MsgType) error {
	//fmt.Printf("write called with param %v\n", payload)
	c.mtx.Lock()
	defer c.mtx.Unlock()
	closed := c.closed
	if closed {
		return errors.New("client is closing. refuse sending new packets")
	}

	var msg *Message
	if msgType == MsgAck {
		msg = NewAck(0, 0)
	} else {
		msg = NewData(0, 0, len(payload), payload)
	}
	c.writer.asyncWrite(msg)
	return nil
}

func (c *client) Close() error {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("cmd channel closed already")
		}
	}()
	cmd := CloseCmd{reason: ""}
	c.cmdClientClose <- cmd
	<-c.clientHasFullyClosedDown
	return nil
}

func (c *client) closeConnectionIfOwning() {
	//if reader routine is still running, it will wake up and die.
	if !c.shareSocket {
		c.connection.Close()
	}
	//
}

func protocolConnect(message []byte, conn *lspnet.UDPConn, params *Params, dataIncomingPacket chan *Packet) (int, error) {
	//repeat several times to send connection message until time out or receive an ack from server
	//c.connection.WriteToUDP(message, c.remoteAddress)
	conn.Write(message)
DONE:
	for i := 0; i < params.EpochLimit; i++ {
		timeOut := time.After(time.Duration(params.EpochMillis) * time.Millisecond)
		select {
		case <-timeOut:
			//resend connection msg
			if i+1 == params.EpochLimit {
				break DONE
			}
			conn.Write(message)
		case p := <-dataIncomingPacket:
			msg := p.msg
			if verify(msg, nil) && msg.Type == MsgAck {
				return msg.ConnID, nil
			}
		}
	}
	return 0, errors.New("connection abort : time out : wait server's ack for connection message")
}

func (c *client) setupAckToLastReceivedMsg(seqNum int) {
	c.ackForLastReceivedMessage = NewAck(c.connectionId, seqNum)
}

func createNewClient(connectionId int,
	params *Params,
	address *lspnet.UDPAddr,
	conn *lspnet.UDPConn,
	dataIncomingPacket chan *Packet,
	signalReaderClosed chan error,
	sendDataPacket chan *Packet,
	shareSocket bool,
	clientExit chan int,
	name string) *client {
	ret := &client{
		connectionId:                connectionId,
		params:                      params,
		remoteAddress:               address,
		connection:                  conn,
		readTimerReset:              make(chan int),
		cmdClientClose:              make(chan CloseCmd),
		clientHasFullyClosedDown:    make(chan error),
		dataIncomingPacket:          dataIncomingPacket,
		receiveMessageQueue:         nil,
		dataNewestMessage:           make(chan *Message, 1000),
		previousSeqNumReturnedToApp: 0,
		signalWriterClosed:          make(chan error, 1),
		signalReaderClosed:          signalReaderClosed,
		dataPacketSideWay:           sendDataPacket,
		shareSocket:                 shareSocket,
		clientExit:                  clientExit,
		name:                        name,
		dataMessageInThisEpoch:      0,
	}
	if dataIncomingPacket == nil {
		ret.dataIncomingPacket = make(chan *Packet, 1000)
	}
	if signalReaderClosed == nil {
		ret.signalReaderClosed = make(chan error)
	}
	writer :=
		newWriterWithWindow(params.WindowSize,
			conn,
			address,
			ret.signalWriterClosed,
			ret.name+"'s www",
			connectionId)
	writer.start()
	ret.writer = writer
	go func() {
		defer func() {
			msg := &Message{Type: -1, Payload: encodeString(&ret.closeReason)}
			p := &Packet{msg: msg}
			ret.dataPacketSideWay <- p
			ret.closeChannels()
			if ret.clientExit != nil {
				ret.clientExit <- ret.connectionId
			}
			fmt.Println("!!!!!main loop exited " + ret.name)
		}()
		timeOutCntLeft := ret.params.EpochLimit
		timeOut := time.After(time.Duration(ret.params.EpochMillis) * time.Millisecond)

		for {
			select {
			case cmd := <-ret.cmdClientClose:
				ret.mtx.Lock()
				if ret.closed == false {
					ret.closeReason = cmd.reason
					ret.closed = true
					if ret.closeReason == "" {
						ret.closeReason = "explicit close"
						writer.close()
					} else {
						ret.closeConnectionIfOwning()
						ret.mtx.Unlock()
						return
					}
				}
				ret.mtx.Unlock()
			case p := <-ret.dataIncomingPacket:
				var list []*Packet
				list = append(list, p)
				list = append(list, readAllPackets(ret.dataIncomingPacket)...)
				for _, packet := range list {
					//go func(packet *Packet) {
					receiveMsg := packet.msg
					fmt.Printf(ret.name+"read msg from socket: %v\n", receiveMsg)
					//validate
					if !verify(receiveMsg, ret) {
						fmt.Printf("msg : %v mal formatted", receiveMsg)
						continue
					}

					if receiveMsg.Type == MsgConnect {
						fmt.Println("received connection message. ignore")
						continue
					} else if receiveMsg.Type == MsgAck {
						writer.getAck(receiveMsg.SeqNum)
						continue
					}

					//update epoch timeout count
					ret.dataMessageInThisEpoch++
					//push to receiveMessage queue
					isNewMsg := ret.appendNewReceivedMessage(receiveMsg)
					//send ack for this data message
					ack := NewAck(ret.connectionId, receiveMsg.SeqNum)
					writer.asyncWrite(ack)
					go func(ptrCopy *Packet) {
						if isNewMsg && ret.dataPacketSideWay != nil {
							//fmt.Printf("push into server thread : %v\n", ptrCopy)
							ret.dataPacketSideWay <- ptrCopy
						}
					}(packet)

					//if ret.reqReadMsg == true {
					nextMsg := ret.getNextMessage()
					if nextMsg != nil {
						//ret.reqReadMsg = false
						fmt.Println("returning newest message")
						ret.dataNewestMessage <- nextMsg
					}
					//}
					//}(p)
				}
			case <-timeOut:
				ret.mtx.Lock()
				fmt.Println(ret.name + " TIME OUT");
				if ret.dataMessageInThisEpoch != 0 {
					timeOutCntLeft = ret.params.EpochLimit
				} else {
					timeOutCntLeft--
					if timeOutCntLeft == 0 {
						fmt.Println("client closing : no data message after epoch limit exceeded : ")
						cmd := CloseCmd{reason: "no data message received after epoch limit exceeded"}
						go func() {
							ret.cmdClientClose <- cmd
						}()
					}
					//resend ack <de>of the last received data message</del>
					zeroAck := NewAck(ret.connectionId, 0)
					fmt.Println("send keep alive")
					writer.asyncWrite(zeroAck)
					//writer.asyncWrite()
				}
				// if there is msg that hasn't receive ack
				// resend that packet
				writer.resend()
				//reset epoch counters
				timeOut = time.After(time.Duration(ret.params.EpochMillis) * time.Millisecond)
				ret.dataMessageInThisEpoch = 0
				ret.mtx.Unlock()
			case err, ok := <-ret.signalWriterClosed:
				fmt.Println("writer closed")
				//I never write to this, how can it wake up ?
				if ok != true {

				}
				ret.mtx.Lock()
				closed := ret.closed
				ret.mtx.Unlock()
				if closed == false {
					go func() { ret.cmdClientClose <- CloseCmd{reason: err.Error()} }()
				} else {
					ret.closeConnectionIfOwning()
					fmt.Println("waiting for reader to exit")
					<-ret.signalReaderClosed
					ret.clientHasFullyClosedDown <- err
					return
				}
			}
		}
	}()
	return ret
}

func (c *client) appendPacket(p *Packet) {
	c.dataIncomingPacket <- p
}
