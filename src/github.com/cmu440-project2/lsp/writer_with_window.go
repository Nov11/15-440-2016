package lsp

import (
	"github.com/cmu440-project2/lspnet"
	"fmt"
	"errors"
)

type writerWithWindow struct {
	pendingMessage []*Message
	needAck        int
	windowSize     int
	cmdShutdown    chan CloseCmd
	newMessage     chan *Message
	conn           *lspnet.UDPConn
	remoteAddress  *lspnet.UDPAddr
	returnChannel  chan error
	ack            chan int
	cmdResend      chan int
}

func newWriterWithWindow(windowSize int, conn *lspnet.UDPConn, addr *lspnet.UDPAddr, signalExit chan error) *writerWithWindow {
	ret := &writerWithWindow{
		windowSize:    windowSize,
		cmdShutdown:   make(chan CloseCmd),
		newMessage:    make(chan *Message),
		conn:          conn,
		remoteAddress: addr,
		ack:           make(chan int),
		cmdResend:     make(chan int),
		returnChannel: signalExit,
	}
	return ret
}

func (www *writerWithWindow) start() {
	go func() {
		var globalError error
		defer func() {
			fmt.Println("!!!!! writer exit")
			www.returnChannel <- globalError
		}()
		stop := false
		for !stop || len(www.pendingMessage) != 0 {
			select {
			case cmd := <-www.cmdShutdown:
				stop = true
				if cmd.reason != "" {
					globalError = errors.New(cmd.reason)
					www.pendingMessage = nil
				} else {
					www.windowSize = 99999
				}
			case msg := <-www.newMessage:
				if stop == true {
					continue
				}
				if msg.Type!= MsgData {
					fmt.Println("!!!!")
				}
				www.pendingMessage = append(www.pendingMessage, msg)
			case number := <-www.ack:
				for i := 0; i < www.needAck; i++ {
					if www.pendingMessage[i].SeqNum == number {
						fmt.Printf("message sent %v has been acked\n", www.pendingMessage[i])
						www.pendingMessage = append(www.pendingMessage[:i], www.pendingMessage[i+1:]...)
						www.needAck--
						break
					}
				}
			case <-www.cmdResend:
				for i := 0; i < www.needAck; i++ {
					err := www.writeMessage(www.pendingMessage[i])
					if err != nil {
						cmd := CloseCmd{reason: err.Error()}
						go func() { www.cmdShutdown <- cmd }()
						break
					}
				}
			default:
				for len(www.pendingMessage) > 0 && www.needAck < www.windowSize {
					err := www.writeMessage(www.pendingMessage[www.needAck])
					if err != nil {
						str := err.Error() + ": close writer"
						fmt.Println(str)
						cmd := CloseCmd{reason: str}
						go func() { www.cmdShutdown <- cmd }()
						break
					}
					www.needAck++
				}
			}
		}

	}()
}
func (www *writerWithWindow) writeMessage(message *Message) error {
	bb := encode(message)
	_, err := www.conn.WriteToUDP(bb, www.remoteAddress)
	fmt.Printf("writeMessage called with %v\n", message)
	return err
}
func (www *writerWithWindow) add(msg *Message) {
	if msg.Type == MsgConnect {
		fmt.Println("!!!!!!!!!!!!!!!!!!!")
	} else if msg.Type == MsgAck {
		www.writeMessage(msg)
	} else {
		www.newMessage <- msg
	}
}

func (www *writerWithWindow) getAck(number int) {
	www.ack <- number
}

func (www *writerWithWindow) resend() {
	www.cmdResend <- 1
}

func (www *writerWithWindow) close() {
	cmd := CloseCmd{}
	www.cmdShutdown <- cmd
}
