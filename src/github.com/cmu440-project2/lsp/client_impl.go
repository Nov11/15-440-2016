// Contains the implementation of a LSP client.

package lsp

import (
	"errors"
	"fmt"
	"github.com/cmu440-project2/lspnet"
	"sync"
	"time"
	"io"
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
	cmdClientClose           chan CloseCmd
	clientHasFullyClosedDown chan error
	dataIncomingMsg          chan Message
	receiveMessageQueue      []Message
	unAckMessage             map[Message]int
	readTimerReset           chan int
	writer                   writerWithWindow
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
		cmdClientClose:           make(chan CloseCmd),
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
	msg := encode(*NewConnect())
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
	writer.start()
	ret.writer = writer
	go func() {
		timeOutCntLeft := ret.params.EpochLimit
		timeOut := time.After(time.Duration(ret.params.EpochMillis) * time.Millisecond)
		receivedMessageCountInThisEpoch := 0
		reqReadMsg := false
		for {
			select {
			case cmd := <-ret.cmdClientClose:
				if ret.closed == false {
					ret.closeReason = cmd.reason
					ret.closed = true
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
						cmd := CloseCmd{reason: "no data message received after epoch limit exceeded"}
						go func() {
							ret.cmdClientClose <- cmd
						}()
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
				if ret.closed == false {
					go func() { ret.cmdClientClose <- CloseCmd{reason: err.Error()} }()
				}
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
//close all cmd channel before
func (c *client) Read() ([]byte, error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("cmd channel closed already")
		}
	}()
	c.cmdReadNewestMessage <- 1
	msg, ok := <-c.dataNewestMessage
	if ok == true {
		return encode(*msg), nil
	}
	return nil, errors.New(c.closeReason)
}

func (c *client) Write(payload []byte) error {
	c.mtx.Lock()
	closed := c.closed
	c.mtx.Unlock()
	if closed {
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
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("cmd channel closed already")
		}
	}()
	cmd := CloseCmd{reason: "user explicitly close client"}
	c.cmdClientClose <- cmd
	_, err := <-c.clientHasFullyClosedDown
	if err {
		return err
	}
	return nil
}

func (c *client) readSocket() {
	conn := c.connection
	for {
		buffer := make([]byte, 1024)
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println(err.Error())
			if err != io.EOF {
				//connection is lost. client should close

			} else {
				//peer will never send any more messages include lsp' ack
				//any thing client send will be in vain
				//client should shutdown itself

			}
			//shutdown client without flushing pending messages

		}
		msg := Message{}
		decode(buffer[:n], &msg)
		c.dataIncomingMsg <- msg
	}
}

func (c *client) cleanUp() {
	//if reader routine is still running, it will wake up and die.
	c.connection.Close()
	//
}

func (c *client) writeSocket(addr *lspnet.UDPAddr, sendMsg chan Message, explicitClose chan bool) {
	for {
		select {
		case msg := <-sendMsg:
			bytes := encode(msg)
			_, err := c.connection.WriteToUDP(bytes, addr)
			if err != nil {
				return
			}
		case <-explicitClose:
			return
		}
	}
}

func protocolConnect(message []byte, c *client) error {
	//repeat several times to send connection message until time out or receive an ack from server
	c.connection.WriteToUDP(message, &c.remoteAddress)
DONE:
	for i := 0; i < c.params.EpochLimit; i++ {
		timeOut := time.After(time.Duration(c.params.EpochMillis) * time.Millisecond)
		select {
		case <-timeOut:
			//resend connection msg
			if i+1 == c.params.EpochLimit {
				break DONE
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
