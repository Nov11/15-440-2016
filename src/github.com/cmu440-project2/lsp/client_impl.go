// Contains the implementation of a LSP client.

package lsp

import (
	"errors"
	"fmt"
	"github.com/cmu440-project2/lspnet"
	"sync"
	"time"
	"io"
	"strconv"
	"os"
)

type client struct {
	connectionId                int
	connection                  *lspnet.UDPConn
	remoteAddress               *lspnet.UDPAddr
	nextSequenceNumber          int
	params                      Params
	mtx                         sync.Mutex
	cmdClientClose              chan CloseCmd
	clientHasFullyClosedDown    chan error
	dataIncomingMsg             chan *Message
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
}

func (c *client) closeChannels() {
	close(c.cmdClientClose)
	close(c.clientHasFullyClosedDown)
	close(c.dataIncomingMsg)
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
func NewClient(hostport string, params *Params) (Client, error) {
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
	ret := client{
		connectionId:                0,
		nextSequenceNumber:          1,
		params:                      *params,
		remoteAddress:               address,
		connection:                  conn,
		readTimerReset:              make(chan int),
		cmdClientClose:              make(chan CloseCmd),
		clientHasFullyClosedDown:    make(chan error),
		dataIncomingMsg:             make(chan *Message),
		receiveMessageQueue:         nil,
		cmdReadNewestMessage:        make(chan int),
		dataNewestMessage:           make(chan *Message),
		previousSeqNumReturnedToApp: 0,
		signalWriterClosed:          make(chan error, 1),
		signalReaderClosed:          make(chan error, 1),
	}
	//prepare protocol connection message
	msg := encode(NewConnect())
	//fire up read routine
	go ret.readSocket()
	//send connection message and wait for server's reply
	err = protocolConnect(msg, &ret)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	fmt.Println("connected to server")
	writer := newWriterWithWindow(params.WindowSize, conn, ret.remoteAddress, ret.signalWriterClosed)
	writer.start()
	ret.writer = writer
	go func() {
		defer func() { fmt.Println("!!!!!main loop exited") }()
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
						writer.close()
					} else {
						ret.connection.Close()
						ret.closeChannels()
						return
					}
				}
			case receiveMsg := <-ret.dataIncomingMsg:
				fmt.Printf("read msg from socket: %v\n", receiveMsg)
				//validate
				if !ret.verify(receiveMsg) {
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
					//resend ack
					zeroAck := NewAck(ret.connectionId, 0)
					fmt.Println("send keep alive")
					writer.add(zeroAck)
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
					ret.connection.Close()
					fmt.Println("waiting for reader to exit")
					<-ret.signalReaderClosed
					ret.clientHasFullyClosedDown <- err
					ret.closeChannels()
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
	return &ret, nil
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

func (c *client) verify(msg *Message) bool {
	if msg.Type != MsgConnect && msg.Type != MsgData && msg.Type != MsgAck {
		fmt.Println("msg type incorrecct : " + string(msg.Type))
		return false
	}

	if c.connectionId != 0 && msg.ConnID != c.connectionId {
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
	c.cmdReadNewestMessage <- 1
	msg, ok := <-c.dataNewestMessage
	if ok == true {
		return encode(msg), nil
	}
	return nil, errors.New(c.closeReason)
}

func (c *client) Write(payload []byte) error {
	fmt.Printf("write called with param %v\n", payload)
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
	if msg.Size != len(payload) {
		os.Exit(1)
	}
	c.writer.add(msg)
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

func (c *client) readSocket() {
	var globalError error
	defer func() {
		fmt.Println("!!!!! reader exit")
		c.signalReaderClosed <- globalError
	}()
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
			globalError = errors.New("read from socket returns " + err.Error())
			return
		}
		msg := Message{}
		decode(buffer[:n], &msg)
		c.dataIncomingMsg <- &msg
	}
}

func (c *client) cleanUp() {
	//if reader routine is still running, it will wake up and die.
	c.connection.Close()
	//
}

func (c *client) writeSocket(addr *lspnet.UDPAddr, sendMsg chan *Message, explicitClose chan bool) {
	for {
		select {
		case msg := <-sendMsg:
			bytes := encode(msg)
			_, err := c.connection.Write(bytes)
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
	//c.connection.WriteToUDP(message, c.remoteAddress)
	c.connection.Write(message)
DONE:
	for i := 0; i < c.params.EpochLimit; i++ {
		timeOut := time.After(time.Duration(c.params.EpochMillis) * time.Millisecond)
		select {
		case <-timeOut:
			//resend connection msg
			if i+1 == c.params.EpochLimit {
				break DONE
			}
			c.connection.Write(message)
		case msg := <-c.dataIncomingMsg:
			if c.verify(msg) && msg.Type == MsgAck {
				c.connectionId = msg.ConnID
				return nil
			}
		}
	}
	return errors.New("connection abort : time out : wait server's ack for connection message")
}
