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
		connectionId: 0, nextSequenceNumber: 0, remoteNextSequenceNumber: 0, closed: false, remoteHost: hostport, params: *params, readTimerReset: make(chan int),
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
		if ret.connectionId != 0 {
			ret.connectionId = ack.ConnID
		}
		ret.mtx.Unlock()
		result <- nil
		return
	})
	if err != nil {
		return nil, err
	}
	go func() {
		for tried := 0; tried < params.EpochLimit; tried++ {
			timeOut := time.After(time.Duration(params.EpochMillis) * time.Millisecond)
			select {
			case <-timeOut:
			case <-ret.readTimerReset:
				tried = 0
			}
			msg, err := json.Marshal(*NewAck(ret.connectionId, 0))
			checkError(err)
			err = ret.Write(msg)
			checkError(err)
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
