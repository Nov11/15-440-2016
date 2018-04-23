package main

import (
	"fmt"
	"os"
	"strconv"
	"github.com/cmu440-project2/lsp"
	"github.com/cmu440-project2/bitcoin"
)

func main() {
	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport> <message> <maxNonce>", os.Args[0])
		return
	}
	hostport := os.Args[1]
	message := os.Args[2]
	maxNonce, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil {
		fmt.Printf("%s is not a number.\n", os.Args[3])
		return
	}

	client, err := lsp.NewClient(hostport, lsp.NewParams(), "client ~~")
	if err != nil {
		fmt.Println("Failed to connect to server:", err)
		return
	}

	defer client.Close()

	_ = message  // Keep compiler happy. Please remove!
	_ = maxNonce // Keep compiler happy. Please remove!
	request := bitcoin.NewRequest(message, 0, maxNonce)
	//1.write request to server

	err = client.Write(bitcoin.Encode(request))
	bitcoin.CheckError(err)
	//2.read response from server
	b, err := client.Read()
	if err != nil {
		fmt.Println(err)
		printDisconnected()
	}else{
		msg := bitcoin.Message{}
		bitcoin.Decode(b, &msg)
		printResult(msg.Hash, msg.Nonce)
	}

}

// printResult prints the final result to stdout.
func printResult(hash, nonce uint64) {
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}
