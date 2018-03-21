// Contains the implementation of a LSP client.

package lsp

import (
	"errors"
	"fmt"
	"github.com/cmu440-project2/lspnet"
	"sync"
	"time"
	"strconv"
	"os"
)

type client struct {
	connectionId                int
	connection                  *lspnet.UDPConn
	remoteAddress               *lspnet.UDPAddr
	nextSequenceNumber          int
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
	cmdReadNewestMessage        chan int
	dataNewestMessage           chan *Message
	previousSeqNumReturnedToApp int
	signalWriterClosed          chan error
	signalReaderClosed          chan error
	ackForLastReceivedMessage   *Message
	result                      chan error
	dataPacketSideWay           chan *Packet
	shareSocket                 bool
	clientExit                  chan int
}

func (c *client) closeChannels() {
	close(c.cmdClientClose)
	close(c.clientHasFullyClosedDown)
	close(c.dataIncomingPacket)
	close(c.readTimerReset)
	close(c.cmdReadNewestMessage)
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
func NewClient(hostport string, params *Params) (*client, error) {
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
	dataIncomingPacket := make(chan *Packet)
	signalReaderClosed := make(chan error)
	go readSocketWithAddress(conn, dataIncomingPacket, signalReaderClosed)
	//send connection message and wait for server's reply
	id, err := protocolConnect(msg, conn, params, dataIncomingPacket)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	fmt.Println("connected to server")
	ret := createNewClient(id, params, address, conn, dataIncomingPacket, signalReaderClosed, nil, false, nil)
	return ret, nil
}

//maintain message in seq order. cause application needs message to be read in serial order
func (c *client) appendNewReceivedMessage(msg *Message) {
	for i := 0; i < len(c.receiveMessageQueue); i++ {
		if c.receiveMessageQueue[i].SeqNum == msg.SeqNum {
			fmt.Println("appendNewReceivedMessage ignore duplicate message with same seq num :" + strconv.Itoa(msg.SeqNum))
			return
		} else if c.receiveMessageQueue[i].SeqNum > msg.SeqNum {
			fmt.Println("appendNewReceivedMessage prepend seq num :" + strconv.Itoa(msg.SeqNum))
			tmp := c.receiveMessageQueue[i:]
			c.receiveMessageQueue = append(c.receiveMessageQueue[:i], msg)
			c.receiveMessageQueue = append(c.receiveMessageQueue, tmp...)
			return
		}
	}
	fmt.Println("appendNewReceivedMessage append seq num :" + strconv.Itoa(msg.SeqNum))
	c.receiveMessageQueue = append(c.receiveMessageQueue, msg)
}

//return next message if already received that message, or return nil
func (c *client) getNextMessage() *Message {
	if len(c.receiveMessageQueue) == 0 {
		return nil
	}
	for i := 0; i < len(c.receiveMessageQueue); i++ {
		if c.receiveMessageQueue[i].SeqNum == c.previousSeqNumReturnedToApp+1 {
			ret := c.receiveMessageQueue[i]
			c.receiveMessageQueue = c.receiveMessageQueue[i:]
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
	c.mtx.Lock()
	defer c.mtx.Unlock()
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("cmd channel closed already")
		}
	}()
	c.cmdReadNewestMessage <- 1
	msg, ok := <-c.dataNewestMessage
	if ok == true {
		return encode(msg), nil
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
	connId := c.connectionId

	var msg *Message
	if msgType == MsgAck {
		msg = NewAck(connId, 0)
	} else {
		seq := c.nextSequenceNumber
		c.nextSequenceNumber++
		msg = NewData(connId, seq, len(payload), payload)
	}
	if msg.Size != len(payload) {
		os.Exit(1)
	}
	c.writer.add(msg)
	return nil
}

func (c *client) Close() error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
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

//func (c *client) writeSocket(addr *lspnet.UDPAddr, sendMsg chan *Message, explicitClose chan bool) {
//	for {
//		select {
//		case msg := <-sendMsg:
//			bytes := encode(msg)
//			_, err := c.connection.Write(bytes)
//			if err != nil {
//				return
//			}
//		case <-explicitClose:
//			return
//		}
//	}
//}

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
	clientExit chan int) *client {
	ret := &client{
		connectionId:                connectionId,
		nextSequenceNumber:          1,
		params:                      params,
		remoteAddress:               address,
		connection:                  conn,
		readTimerReset:              make(chan int),
		cmdClientClose:              make(chan CloseCmd),
		clientHasFullyClosedDown:    make(chan error),
		dataIncomingPacket:          dataIncomingPacket,
		receiveMessageQueue:         nil,
		cmdReadNewestMessage:        make(chan int),
		dataNewestMessage:           make(chan *Message),
		previousSeqNumReturnedToApp: 0,
		signalWriterClosed:          make(chan error, 1),
		signalReaderClosed:          signalReaderClosed,
		dataPacketSideWay:           sendDataPacket,
		shareSocket:                 shareSocket,
		clientExit:                  clientExit,
	}
	if dataIncomingPacket == nil {
		ret.dataIncomingPacket = make(chan *Packet)
	}
	if signalReaderClosed == nil {
		ret.signalReaderClosed = make(chan error)
	}
	writer :=
		newWriterWithWindow(params.WindowSize, conn, address, ret.signalWriterClosed)
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
			fmt.Println("!!!!!main loop exited")
		}()
		timeOutCntLeft := ret.params.EpochLimit
		timeOut := time.After(time.Duration(ret.params.EpochMillis) * time.Millisecond)
		dataMessageInThisEpoch := 0
		reqReadMsg := false
		for {
			select {
			case cmd := <-ret.cmdClientClose:
				if ret.closed == false {
					ret.closeReason = cmd.reason
					ret.closed = true
					if ret.closeReason == "" {
						ret.closeReason = "explicit close"
						writer.close()
					} else {
						ret.closeConnectionIfOwning()
						return
					}
				}
			case p := <-ret.dataIncomingPacket:
				receiveMsg := p.msg
				fmt.Printf("read msg from socket: %v\n", receiveMsg)
				//validate
				if !verify(receiveMsg, ret) {
					fmt.Printf("msg : %v mal formatted", receiveMsg)
					continue
				}

				if receiveMsg.Type == MsgConnect {
					fmt.Println("received connection message. ignore")
				} else if receiveMsg.Type == MsgAck {
					writer.getAck(receiveMsg.SeqNum)
				} else {
					//update epoch timeout count
					dataMessageInThisEpoch++
					//push to receiveMessage queue
					ret.appendNewReceivedMessage(receiveMsg)
					//send ack for this data message
					ack := NewAck(ret.connectionId, receiveMsg.SeqNum)
					writer.add(ack)
					go func() {
						if ret.dataPacketSideWay != nil {
							ret.dataPacketSideWay <- p
						}
					}()
				}

				if reqReadMsg == true {
					nextMsg := ret.getNextMessage()
					if nextMsg != nil {
						reqReadMsg = false
						fmt.Println("returning newest message")
						ret.dataNewestMessage <- nextMsg
					}
				}
			case <-timeOut:
				if dataMessageInThisEpoch != 0 {
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
					writer.add(zeroAck)
					//writer.add()
				}
				// if there is msg that hasn't receive ack
				// resend that packet
				writer.resend()
				//reset epoch counters
				timeOut = time.After(time.Duration(ret.params.EpochMillis) * time.Millisecond)
				dataMessageInThisEpoch = 0
			case err, ok := <-ret.signalWriterClosed:
				fmt.Println("writer closed")
				//I never write to this, how can it wake up ?
				if ok != true {

				}
				if ret.closed == false {
					go func() { ret.cmdClientClose <- CloseCmd{reason: err.Error()} }()
				} else {
					ret.closeConnectionIfOwning()
					fmt.Println("waiting for reader to exit")
					<-ret.signalReaderClosed
					ret.clientHasFullyClosedDown <- err
					return
				}
			case <-ret.cmdReadNewestMessage:
				fmt.Println("requesting new message")
				reqReadMsg = true
				msg := ret.getNextMessage()
				if msg != nil {
					reqReadMsg = false
					fmt.Println("returning newest message")
					ret.dataNewestMessage <- msg
				}
			}
		}
	}()
	return ret
}

func (c *client) appendPacket(p *Packet) {
	c.dataIncomingPacket <- p
}
