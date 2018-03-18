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

func decodeInterface(raw [] byte, v interface{}) {
	err := json.Unmarshal(raw, v)
	checkError(err)
}

func encode(msg *Message) []byte {
	ret, err := json.Marshal(msg)
	checkError(err)
	return ret
}

func encodeInterface( v interface{}) []byte {
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
