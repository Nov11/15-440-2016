// Contains the implementation of a LSP server.

package lsp

import (
	"errors"
	"github.com/cmu440-project2/lspnet"
	"strconv"
)

type server struct {
	address          *lspnet.UDPAddr
	connection       *lspnet.UDPConn
	nextConnectionId int
	connectIdList    map[int]lspnet.UDPConn
	closing          bool
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {

	address, err := lspnet.ResolveUDPAddr("udp", ":"+strconv.Itoa(port))
	checkError(err)

	conn, err := lspnet.ListenUDP("udp", address)
	if err != nil {
		return nil, err
	}
	ret := server{
		address:          address,
		connection:       conn,
		nextConnectionId: 0,
		connectIdList:    make(map[int]lspnet.UDPConn),
		closing:          false,}
	return &ret, nil
}

func (s *server) Read() (int, []byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {} // Blocks indefinitely.
	return -1, nil, errors.New("not yet implemented")
}

func (s *server) Write(connID int, payload []byte) error {
	return errors.New("not yet implemented")
}

func (s *server) CloseConn(connID int) error {
	return errors.New("not yet implemented")
}

func (s *server) Close() error {
	return errors.New("not yet implemented")
}
