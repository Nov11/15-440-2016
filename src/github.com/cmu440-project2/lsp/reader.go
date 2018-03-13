package lsp

import (
	"fmt"
	"io"
	"errors"
	"github.com/cmu440-project2/lspnet"
)

//func readSocket(conn *lspnet.UDPConn, dataIncomingMessage chan *Message, signalReaderClosed chan error) {
//	var globalError error
//	defer func() {
//		fmt.Println("!!!!! reader exit")
//		signalReaderClosed <- globalError
//	}()
//
//	for {
//		buffer := make([]byte, 1024)
//		n, _, err := conn.ReadFromUDP(buffer)
//		if err != nil {
//			fmt.Println(err.Error())
//			if err != io.EOF {
//				//connection is lost. client should close
//
//			} else {
//				//peer will never send any more messages include lsp' ack
//				//any thing client send will be in vain
//				//client should shutdown itself
//
//			}
//			globalError = errors.New("read from socket returns " + err.Error())
//			return
//		}
//		msg := Message{}
//		decode(buffer[:n], &msg)
//		dataIncomingMessage <- &msg
//	}
//}

type Packet struct {
	msg  *Message
	addr *lspnet.UDPAddr
}

func readSocketWithAddress(conn *lspnet.UDPConn, dataOut chan *Packet, signalReaderClosed chan error) {
	var globalError error
	defer func() {
		fmt.Println("!!!!! reader exit")
		signalReaderClosed <- globalError
	}()

	for {
		buffer := make([]byte, 1024)
		n, addr, err := conn.ReadFromUDP(buffer)
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
		p := &Packet{msg: &msg, addr: addr}
		dataOut <- p
	}
}
