package lsp

import (
	"github.com/cmu440-project2/lspnet"
	"fmt"
	"errors"
)

var WWWLENGTH int = 2000

type writerWithWindow struct {
	pendingMessage     []*Message
	needAck            int
	windowSize         int
	cmdShutdown        chan CloseCmd
	newMessage         chan *Message
	conn               *lspnet.UDPConn
	remoteAddress      *lspnet.UDPAddr
	returnChannel      chan error
	ack                chan int
	cmdResend          chan int
	name               string
	nextSequenceNumber int
	connectionId       int
}

func newWriterWithWindow(windowSize int,
	conn *lspnet.UDPConn,
	addr *lspnet.UDPAddr,
	signalExit chan error,
	name string,
	connectionId int) *writerWithWindow {
	ret := &writerWithWindow{
		windowSize:         windowSize,
		cmdShutdown:        make(chan CloseCmd),
		newMessage:         make(chan *Message, WWWLENGTH),
		conn:               conn,
		remoteAddress:      addr,
		ack:                make(chan int, WWWLENGTH),
		cmdResend:          make(chan int, WWWLENGTH),
		returnChannel:      signalExit,
		name:               name,
		nextSequenceNumber: 1,
		connectionId:       connectionId,
	}
	return ret
}

func (www *writerWithWindow) start() {
	go func() {
		var globalError error
		defer func() {
			fmt.Println("!!!!! writer exit " + www.name)
			www.returnChannel <- globalError
		}()
		stop := false
		for !stop || len(www.pendingMessage) != 0 || len(www.newMessage) != 0 {
			//fmt.Printf("%v len(www.pendingMessage): %v stop : %v msgleft:%v\n", www.name, www.pendingMessage, stop, len(www.newMessage))
			select {
			case cmd := <-www.cmdShutdown:
				stop = true
				if cmd.reason != "" {
					globalError = errors.New(cmd.reason)
					www.pendingMessage = nil
					www.newMessage = nil
				}
			case msg := <-www.newMessage:
				list := []*Message{msg}
				list = append(list, readAllMsgs(www.newMessage)...)
				for _, msg = range list {
					//if stop == true {
					//	break
					//}
					msg.ConnID = www.connectionId
					if msg.Type == MsgAck {
						www.writeMessageBlocking(msg)
						continue
					}
					if msg.Type == MsgData {
						msg.SeqNum = www.nextSequenceNumber
						www.nextSequenceNumber++
					} else {
						fmt.Println("!!!!")
					}
					www.pendingMessage = append(www.pendingMessage, msg)
				}
				www.output()
			case number := <-www.ack:
				//fmt.Printf("%v ***** %v\n", www.name, number)
				list := []int{number}
				list = append(list, readAllInt(www.ack)...)
				//fmt.Printf("%v ***** %v\n", www.name, list)
				for _, n := range list {
					//if n == 10 {
					//
					//	fmt.Printf("-----%v ---- %v ----- %v\n", www.name, www.needAck, www.pendingMessage)
					//}
					for i := 0; i < www.needAck; i++ {
						if www.pendingMessage[i].SeqNum == n {
							//fmt.Printf(www.name+" message sent %v has been acked\n", www.pendingMessage[i])
							www.pendingMessage = append(www.pendingMessage[:i], www.pendingMessage[i+1:]...)
							www.needAck--
							break
						}
					}
				}
				www.output()
			case <-www.cmdResend:
				//fmt.Printf("%v resend start: [%v,%v](window index)\n", www.name, 0, www.needAck)
				for i := 0; i < www.needAck; i++ {
					err := www.writeMessageBlocking(www.pendingMessage[i])
					if err != nil {
						cmd := CloseCmd{reason: err.Error()}
						go func() { www.cmdShutdown <- cmd }()
						break
					}
				}
				//fmt.Println(www.name + " resend end");
				//default:

			}
		}

	}()
}

func (www *writerWithWindow) output() {
	for len(www.pendingMessage) > 0 && www.needAck < www.windowSize && www.needAck < len(www.pendingMessage) {
		//go
		func(msg *Message) {
			err := www.writeMessageBlocking(msg)
			if err != nil {
				str := err.Error() + ": close writer" + www.name
				fmt.Println(str)
				cmd := CloseCmd{reason: str}
				www.cmdShutdown <- cmd
			}
		}(www.pendingMessage[www.needAck])

		www.needAck++
	}
}

func (www *writerWithWindow) writeMessageBlocking(message *Message) error {
	bb := encode(message)
	var err error
	if www.remoteAddress == nil {
		_, err = www.conn.Write(bb)
	} else {
		_, err = www.conn.WriteToUDP(bb, www.remoteAddress)
	}
	//fmt.Printf("%s writeMessage called with %v target : %v\n", www.name, message, www.remoteAddress)
	return err
}

func (www *writerWithWindow) asyncWrite(msg *Message) {
	//if msg.Type == MsgConnect {
	//	fmt.Println("!!!!!!!!!!!!!!!!!!!")
	//} else if msg.Type == MsgAck {
	//	go func(copyMsg *Message) { www.writeMessage(copyMsg) }(msg)
	//} else {
	//	go func() { www.newMessage <- msg }()
	//}
	go func() { www.newMessage <- msg }()
}

func (www *writerWithWindow) seqWrite(msg *Message) {
	www.newMessage <- msg
}

func (www *writerWithWindow) getAck(number int) {
	//fmt.Printf("%v get ack %v \n", www.name, number)
	go func() { www.ack <- number }()
}

func (www *writerWithWindow) resend() {
	go func() { www.cmdResend <- 1 }()
}

func (www *writerWithWindow) close() {
	cmd := CloseCmd{}
	www.cmdShutdown <- cmd
}

func (www *writerWithWindow) forceClose(reason string) {
	cmd := CloseCmd{reason: reason}
	www.cmdShutdown <- cmd
}
