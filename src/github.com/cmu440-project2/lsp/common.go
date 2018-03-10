package lsp

import (
	"encoding/json"
	"fmt"
	"os"
)

func decode(raw []byte, to interface{}) {
	err := json.Unmarshal(raw, to)
	checkError(err)
}

func encode(from interface{}) []byte {
	ret, err := json.Marshal(from)
	checkError(err)
	return ret
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

type CloseCmd struct{
	reason string
}