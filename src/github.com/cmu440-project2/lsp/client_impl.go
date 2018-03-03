// Contains the implementation of a LSP client.

package lsp

import (
	"errors"
	"github.com/cmu440-project2/lspnet"
	"fmt"
	"bufio"
	"encoding/json"
	"os"
	"time"
	"sync"
	"net"
)

type client struct {
	connectionId             int
	nextSequenceNumber       int
	remoteNextSequenceNumber int
	closed                   bool
	remoteHost               string
	params                   Params
	mtx                      sync.Mutex
	readTimerReset           chan int
	clientClose              chan int
	receiveMsg               chan Message
	writeMsg                 chan Message
	writeClose               chan int
	writeWindow              chan []byte

	receiveMessageQueue []Message
	unAckedMessage      map[Message]int
	connection          net.Conn
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
		closed:                   false,
		remoteHost:               hostport,
		params:                   *params,
		readTimerReset:           make(chan int),
		clientClose:              make(chan int),
		receiveMsg:               make(chan Message),
		writeMsg:                 make(chan Message),
		writeClose:               make(chan int),
		writeWindow:              make(chan []byte, params.WindowSize),
		receiveMessageQueue:      nil,
		unAckedMessage:           make(map[Message]int),
	}
	conn, err := lspnet.DialUDP(hostport, nil, nil)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	msg, err := json.Marshal(*NewConnect())
	checkError(err)

	err = ret.doWithEpoch(func(result chan error) {
		_, err = bufio.NewWriter(conn).Write(msg)
		if err != nil {
			result <- err
			return
		}
		buff := make([]byte, 1024)
		readCnt := 0
		readCnt, err := bufio.NewReader(conn).Read(buff)
		checkError(err)
		ack := Message{}
		json.Unmarshal(buff[:readCnt], &ack)
		if !ret.verify(ack) || ack.Type != MsgAck {
			result <- errors.New("invalid ack message")
			return
		}
		ret.mtx.Lock()
		//in case of n connection msg sent and receive n ack simultaneously
		//server will assign only one connection id to one port
		//the later connection msg receive from the same port will be ignored and return ack with -1(I picked this value)
		//the client may receive acks of previous sent connection requests at the same time with many connectId == -1
		//the only one above zero will be the real connection id
		//if there is a bit flip in incoming msg, this should be detected in other mechanism
		//mutex should serves as synchronization point. inside this section the current co routine should see the changes
		//of others made to the connection id field. an atomic field may be better than this mutex approach. in case of
		//heavy contention this will be a significant bottle neck, I think.
		//co routines making connection will die eventually when return from this procedure without dangling or doing damage.
		if ret.connectionId == 0 && ack.ConnID != 0 {
			ret.connectionId = ack.ConnID
		}
		ret.mtx.Unlock()
		result <- nil
		return
	})
	if err != nil {
		return nil, err
	}
	//this is the main event loop of a client. further abstraction may be applied.
	//the basic incoming input are connection read, write and time out.
	//and also close from client driver application

	//this is for writing
	go func() {
		writer := bufio.NewWriter(conn)
		for {
			select {
			case <-ret.writeClose:
				//write all the remaining message(from window & queue)
				//and wait until all the packets get acknowledged
				for len(ret.writeWindow) > 0 {
					msg := <-ret.writeWindow
					writer.Write(msg)
				}
				for len(ret.writeMsg) > 0 {
					msg := <-ret.writeMsg
					byteMsg, err := json.Marshal(msg)
					checkError(err)
					writer.Write(byteMsg)
				}
				return
			case msg := <-ret.writeWindow:
				writer.Write(msg)
				writer.Flush()
			}
		}

	}()

	go func() {
		timeOutCntLeft := ret.params.EpochLimit
		timeOut := time.After(time.Duration(ret.params.EpochMillis) * time.Millisecond)
		receiveCntInThisEpoch := 0
		for {
			select {
			case <-ret.clientClose:

				ret.writeClose <- 1
			case receiveMsg := <-ret.receiveMsg:
				//validate
				ret.verify(receiveMsg)
				//update epoch timeout count
				receiveCntInThisEpoch++
				//push to receiveMessage queue
				ret.receiveMessageQueue = append(ret.receiveMessageQueue, receiveMsg)
			case <-timeOut:
				timeOutCntLeft--
				if timeOutCntLeft == 0 {
					ret.clientClose <- 1
				}
				if receiveCntInThisEpoch == 0 {
					//resend ack
				}
				// if there is msg that hasn't receive ack
				// resend that packet

			}
		}
	}()
	return &ret, nil
}

func (c *client) doWithEpoch(work func(chan error)) error {
	for tried := 0; tried < c.params.EpochLimit; tried++ {
		timeOut := time.After(time.Duration(c.params.EpochMillis) * time.Millisecond)
		channel := make(chan error)
		go func() {
			work(channel)
		}()
		select {
		case <-timeOut:
		case err := <-channel:
			return err
		}
	}
	return errors.New("time out")
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
		msg.Payload = msg.Payload[msg.Size:]
	}

	return true
}

func (c *client) ConnID() int {
	return c.connectionId
}

func (c *client) Read() ([]byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	// once read message, update readerTimerReset channel
	//
	select {} // Blocks indefinitely.
	return nil, errors.New("not yet implemented")
}

func (c *client) Write(payload []byte) error {
	return errors.New("not yet implemented")
}

func (c *client) Close() error {
	return errors.New("not yet implemented")
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
func (c *client) readSocket(conn net.Conn) {
	for {
		buffer := make([]byte, 1024)
		n, err := bufio.NewReader(conn).Read(buffer)
		//do I have to branch on eof and error?
		//if eof is received, it means that peer will not send any message and read routine can terminate.
		//but I can still send message as the current client is not closed.
		//if other error is encountered, it means that the connection is broken and should be shutdown.
		//when it comes to lsp, it's on a upper layer other then socket connection layer.
		if err != nil {
			fmt.Println(err.Error())
			//should tear down every thing
			c.Close()
			return
		}
		msg := Message{}
		decode(buffer[:n], &msg)
		c.receiveMsg <- msg
	}
}

func (c *client) stopReadingSocket(conn net.Conn) {
	conn.Close()
}

func writeSocket(conn net.Conn, sendMsg chan Message, explicitClose chan bool) {
	writer := bufio.NewWriter(conn)
	for {
		select {
		case msg := <-sendMsg:
			writer.Write(encode(msg))
		case <-explicitClose:
			return
		}
	}
}

type writerWithWindow struct {
	pendingMessage []Message
	needAck        int
	windowSize     int
	shutdown       chan int
	newMessage     chan Message
}

func newWriterWithWindow(windowSize int) writerWithWindow {
	ret := writerWithWindow{
		windowSize: windowSize,
		shutdown:   make(chan int),
		newMessage: make(chan Message),
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
	}()
}

func (www *writerWithWindow) add(msg Message) {
	www.pendingMessage = append(www.pendingMessage, msg)
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

func (www *writerWithWindow) close() {
	www.shutdown <- 1
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
