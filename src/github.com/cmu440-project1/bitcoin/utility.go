package bitcoin

import (
	"fmt"
	"os"
	"encoding/json"
)

func Encode(msg *Message) []byte {
	ret, err := json.Marshal(msg)
	CheckError(err)
	return ret
}

func Decode(raw []byte, msg *Message) {
	err := json.Unmarshal(raw, msg)
	CheckError(err)
}

func CheckError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(44)
	}
}


func Min(x, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}
