package main

import (
	"fmt"
	"os"

	"github.com/cmu440-project2/lsp"
	"errors"
	"github.com/cmu440-project2/bitcoin"
	"math"
)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (lsp.Client, error) {
	client, err := lsp.NewClient(hostport, lsp.NewParams(), "miner")
	if err != nil {
		fmt.Println("Failed to connect to server:", err)
		return nil, errors.New("connection failed")
	}
	msg := bitcoin.NewJoin()
	client.Write(bitcoin.Encode(msg))

	return client, nil
}

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
		return
	}

	hostport := os.Args[1]
	miner, err := joinWithServer(hostport)
	if err != nil {
		fmt.Println("Failed to join with server:", err)
		return
	}

	defer miner.Close()

	for true {
		//1. get request
		binary, err := miner.Read()
		bitcoin.CheckError(err)
		msg := bitcoin.Message{}
		bitcoin.Decode(binary, &msg)

		//2. compute request
		result := uint64(math.MaxUint64)
		n := uint64(0)
		for i := msg.Lower; i <= msg.Upper; i++ {
			v := bitcoin.Hash(msg.Data, i)
			if result > v {
				result = v
				n = i
			}
		}
		//3. send back result
		resp := bitcoin.NewResult(result, n)
		err = miner.Write(bitcoin.Encode(resp))
		bitcoin.CheckError(err)
	}
}
