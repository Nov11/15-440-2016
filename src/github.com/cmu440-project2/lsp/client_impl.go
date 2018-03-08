// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440-project2/lspnet"
	"os"
	"sync"
	"time"
)

type client struct {
	connectionId             int
	connection               lspnet.UDPConn
	remoteAddress            lspnet.UDPAddr
	nextSequenceNumber       int
	remoteNextSequenceNumber int
	remoteHost               string
	params                   Params
	mtx                      sync.Mutex
	cmdClientClose           chan int
	clientHasFullyClosedDown chan error
	dataIncomingMsg          chan Message
	receiveMessageQueue      []Message
	unAckMessage             map[Message]int
	readTimerReset           chan int
	writer                   writerWithWindow
	closing                  bool
	closed                   bool
	closeReason              string
	cmdReadNewestMessage     chan int
	dataNewestMessage        chan *Message
	//cmdQuitReadRoutine          chan int
	previousSeqNumReturnedToApp int
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
func NewClient(hostport string, params *Params) (Client, error) {
	ret := client{
		connectionId:             0,
		nextSequenceNumber:       0,
		remoteNextSequenceNumber: 0,
		remoteHost:               hostport,
		params:                   *params,
		readTimerReset:           make(chan int),
		cmdClientClose:           make(chan int, 2),
		clientHasFullyClosedDown: make(chan error),
		dataIncomingMsg:          make(chan Message),
		receiveMessageQueue:      nil,
		unAckMessage:             make(map[Message]int),
		cmdReadNewestMessage:     make(chan int),
		dataNewestMessage:        make(chan *Message),
		//cmdQuitReadRoutine:          make(chan int),
		previousSeqNumReturnedToApp: -1,
	}
	//establish UDP connection to server
	conn, err := lspnet.DialUDP(hostport, nil, &ret.remoteAddress)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	//prepare protocol connection message
	msg, err := json.Marshal(*NewConnect())
	checkError(err)
	//fire up read routine
	go ret.readSocket()
	//send connection message and wait for server's reply
	err = protocolConnect(msg, &ret)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	writerClosed := make(chan error)
	writer := newWriterWithWindow(params.WindowSize, conn, &ret.remoteAddress, writerClosed)
	ret.writer = writer
	go func() {
		timeOutCntLeft := ret.params.EpochLimit
		timeOut := time.After(time.Duration(ret.params.EpochMillis) * time.Millisecond)
		receivedMessageCountInThisEpoch := 0
		reqReadMsg := false
		for {
			select {
			case <-ret.cmdClientClose:
				if ret.closing == false {
					ret.closeReason = "explicitly close connection"
					ret.closing = true
					writer.close()
				}
			case receiveMsg := <-ret.dataIncomingMsg:
				//validate
				if !ret.verify(receiveMsg) {
					fmt.Printf("msg : %v mal formatted", receiveMsg)
					continue
				}
				//update epoch timeout count
				receivedMessageCountInThisEpoch++
				//push to receiveMessage queue
				ret.appendNewReceivedMessage(&receiveMsg)
				if receiveMsg.Type == MsgAck {
					writer.getAck(receiveMsg.SeqNum)
				} else if receiveMsg.Type == MsgData {
					ack := NewAck(ret.connectionId, ret.nextSequenceNumber)
					ret.nextSequenceNumber++
					writer.add(ack)
				}
				if reqReadMsg {
					nextMsg := ret.getNextMessage()
					if nextMsg != nil {
						reqReadMsg = false
						ret.dataNewestMessage <- nextMsg
					}
				}
			case <-timeOut:
				if receivedMessageCountInThisEpoch != 0 {
					timeOutCntLeft = ret.params.EpochLimit
				} else {
					timeOutCntLeft--
					if timeOutCntLeft == 0 {
						fmt.Println("client closing : no data message after epoch limit exceeded : ")
						ret.cmdClientClose <- 1
						ret.closing = true
						ret.closeReason = "close due to time out"
					}
				}
				if receivedMessageCountInThisEpoch == 0 {
					//resend ack
					zeroAck := NewAck(ret.connectionId, 0)
					writer.add(zeroAck)
				}
				// if there is msg that hasn't receive ack
				// resend that packet
				writer.resend()
			case err := <-writerClosed:
				ret.clientHasFullyClosedDown <- err
				ret.closed = true
				return
			case <-ret.cmdReadNewestMessage:
				reqReadMsg = true
				msg := ret.getNextMessage()
				if msg != nil {
					reqReadMsg = false
					ret.dataNewestMessage <- msg
				}
			}
		}
	}()
	return &ret, nil
}

//maintain message in seq order. cause application needs message to be read in serial order
func (c *client) appendNewReceivedMessage(msg *Message) {
	for i := 0; i < len(c.receiveMessageQueue); i++ {
		if c.receiveMessageQueue[i].SeqNum == msg.SeqNum {
			return
		} else if c.receiveMessageQueue[i].SeqNum > msg.SeqNum {
			tmp := c.receiveMessageQueue[i:]
			c.receiveMessageQueue = append(c.receiveMessageQueue[:i], *msg)
			c.receiveMessageQueue = append(c.receiveMessageQueue, tmp...)
			return
		}
	}
	c.receiveMessageQueue = append(c.receiveMessageQueue, *msg)
}

//return next message if already received that message, or return nil
func (c *client) getNextMessage() *Message {
	if len(c.receiveMessageQueue) == 0 {
		return nil
	}
	if c.receiveMessageQueue[0].SeqNum == c.previousSeqNumReturnedToApp+1 {
		ret := c.receiveMessageQueue[0]
		c.receiveMessageQueue = c.receiveMessageQueue[1:]
		c.previousSeqNumReturnedToApp++
		return &ret
	}
	return nil
}

//func (c *client) doWithEpoch(work func(chan error)) error {
//	for tried := 0; tried < c.params.EpochLimit; tried++ {
//		timeOut := time.After(time.Duration(c.params.EpochMillis) * time.Millisecond)
//		channel := make(chan error)
//		go func() {
//			work(channel)
//		}()
//		select {
//		case <-timeOut:
//		case err := <-channel:
//			return err
//		}
//	}
//	return errors.New("time out")
//}

func (c *client) verify(msg Message) bool {
	if msg.Type != MsgConnect && msg.Type != MsgData && msg.Type != MsgAck {
		return false
	}

	if c.connectionId != 0 && msg.ConnID != c.connectionId {
		return false
	}

	if msg.Size > len(msg.Payload) {
		return false
	}

	if msg.Size < len(msg.Payload) {
		msg.Payload = msg.Payload[:msg.Size]
	}

	return true
}

func (c *client) ConnID() int {
	return c.connectionId
}

//how to read message while bg routine is closing or just shutdown ?
//at that time there's no routine exists to wait on cmd channel
func (c *client) Read() ([]byte, error) {
	c.cmdReadNewestMessage <- 1
	msg := <-c.dataNewestMessage
	if msg != nil {
		return encode(*msg), nil
	}
	return nil, errors.New(c.closeReason)
}

func (c *client) Write(payload []byte) error {
	if c.closing {
		return errors.New("client is closing. refuse sending new packets")
	}
	connId := c.connectionId
	seq := c.nextSequenceNumber
	c.nextSequenceNumber++
	msg := NewData(connId, seq, len(payload), payload)
	c.writer.add(msg)
	return nil
}

func (c *client) Close() error {
	c.cmdClientClose <- 1
	return <-c.clientHasFullyClosedDown
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

//this is for reading
//this should be a typical routine / building block as there is no async/no blocking io in golang
//select case/default is the only way to express non blocking io.
//this routine is shutdown by client on conn.Close()
func (c *client) readSocket() {
	conn := c.connection
	for {
		//select {
		////case <-c.cmdQuitReadRoutine:
		////	return
		//default:
		buffer := make([]byte, 1024)
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		msg := Message{}
		decode(buffer[:n], &msg)
		c.dataIncomingMsg <- msg
		//}
	}
}

func (c *client) stopReadingSocket() {
	c.connection.Close()
	//c.cmdQuitReadRoutine <- 1
}

func writeSocket(conn lspnet.UDPConn, addr *lspnet.UDPAddr, sendMsg chan Message, explicitClose chan bool) {
	for {
		select {
		case msg := <-sendMsg:
			bytes := encode(msg)
			conn.WriteToUDP(bytes, addr)
		case <-explicitClose:
			return
		}
	}
}

func (www *writerWithWindow) writeMessage(message Message) {
	www.conn.WriteToUDP(encode(message), www.address)
}

type writerWithWindow struct {
	pendingMessage []Message
	needAck        int
	windowSize     int
	shutdown       chan int
	newMessage     chan Message
	conn           *lspnet.UDPConn
	address        *lspnet.UDPAddr
	returnChannel  chan error
}

func newWriterWithWindow(windowSize int, conn *lspnet.UDPConn, addr *lspnet.UDPAddr, result chan error) writerWithWindow {
	ret := writerWithWindow{
		windowSize:    windowSize,
		shutdown:      make(chan int),
		newMessage:    make(chan Message),
		conn:          conn,
		address:       addr,
		returnChannel: result,
	}
	return ret
}

func (www *writerWithWindow) start(toMsg chan Message) {
	go func() {
		stop := false
		for !stop && len(www.pendingMessage) == 0 {
			select {
			case <-www.shutdown:
				stop = true
				www.windowSize = 99999
			case msg := <-www.newMessage:
				www.pendingMessage = append(www.pendingMessage, msg)
			default:
				for len(www.pendingMessage) > 0 && www.needAck < www.windowSize {
					toMsg <- www.pendingMessage[www.needAck]
					www.needAck++
				}
			}
		}
		www.returnChannel <- nil
	}()
}

func (www *writerWithWindow) add(msg *Message) {
	www.pendingMessage = append(www.pendingMessage, *msg)
}

func (www *writerWithWindow) getAck(number int) {
	www.needAck--
	for i := 0; i < www.windowSize; i++ {
		if www.pendingMessage[i].SeqNum == number {
			www.pendingMessage = append(www.pendingMessage[:i], www.pendingMessage[i+1:]...)
			break
		}
	}
}

func (www *writerWithWindow) resend() {
	for i := 0; i < www.needAck; i++ {
		www.writeMessage(www.pendingMessage[i])
	}
}

func (www *writerWithWindow) close() {
	www.shutdown <- 1
}

func (www *writerWithWindow) resultChannel() chan error {
	return www.returnChannel
}

func decode(raw []byte, to interface{}) {
	err := json.Unmarshal(raw, to)
	checkError(err)
}

func encode(from interface{}) []byte {
	ret, err := json.Marshal(from)
	checkError(err)
	return ret
}

func protocolConnect(message []byte, c *client) error {
	//repeat several times to send connection message until time out or receive an ack from server
	c.connection.WriteToUDP(message, &c.remoteAddress)
	for i := 0; i < c.params.EpochLimit; i++ {
		timeOut := time.After(time.Duration(c.params.EpochMillis) * time.Millisecond)
		select {
		case <-timeOut:
			//resend connection msg
			if i+1 == c.params.EpochLimit {
				break
			}
			c.connection.WriteToUDP(message, &c.remoteAddress)
		case msg := <-c.dataIncomingMsg:
			if c.verify(msg) && msg.Type == MsgAck {
				c.connectionId = msg.ConnID
				return nil
			}
		}
	}
	return errors.New("connection abort : time out : wait server's ack for connection message")
}
