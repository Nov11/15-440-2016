package lsp

import (
	"encoding/json"
	"fmt"
	"os"
)

func decode(raw []byte, msg *Message) {
	err := json.Unmarshal(raw, msg)
	checkError(err)
}

func decodeError(raw [] byte, v *error) {
	err := json.Unmarshal(raw, v)
	checkError(err)
}

func encode(msg *Message) []byte {
	ret, err := json.Marshal(msg)
	checkError(err)
	return ret
}

func encodeString(str *string) []  byte {
	ret, err := json.Marshal(str)
	checkError(err)
	return ret
}

func decodeString(raw [] byte, v *string) {
	err := json.Unmarshal(raw, v)
	checkError(err)
}
func encodeError(v *error) []byte {
	ret, err := json.Marshal(v)
	checkError(err)
	return ret
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

type CloseCmd struct {
	reason string
}

func readAllPackets(c chan *Packet) []*Packet {
	var localPacketList []*Packet
DONE:
	for {
		select {
		case p := <-c:
			localPacketList = append(localPacketList, p)
		default:
			break DONE
		}
	}
	return localPacketList
}

func readAllMsgs(c chan *Message) []*Message {
	var localPacketList []*Message
DONE:
	for {
		select {
		case p := <-c:
			localPacketList = append(localPacketList, p)
		default:
			break DONE
		}
	}
	return localPacketList
}