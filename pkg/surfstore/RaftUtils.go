package surfstore

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sync"

	"google.golang.org/grpc"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	isLeaderMutex := sync.RWMutex{}
	isCrashedMutex := sync.RWMutex{}

	server := RaftSurfstore{
		isLeader:       false,
		isLeaderMutex:  &isLeaderMutex,
		term:           0,
		metaStore:      NewMetaStore(config.BlockAddrs),
		log:            make([]*UpdateOperation, 0),
		isCrashed:      false,
		isCrashedMutex: &isCrashedMutex,
		ID:             id,
		peers:          config.RaftAddrs,
		commitIndex:    -1,
		lastApplied:    -1,
		matchIndex:     make([]int64, len(config.RaftAddrs)),
		nextIndex:      make([]int64, len(config.RaftAddrs)),
		// pendingCommits: make([]*chan bool, 0)
	}
	for i := range server.matchIndex {
		server.matchIndex[i] = -1
		server.nextIndex[i] = 0
	}
	return &server, nil
}

// Start up the Raft server and the MetaStore server
func ServeRaftServer(server *RaftSurfstore) error {
	grpcServer := grpc.NewServer()
	addr := server.peers[server.ID]
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Println("Listening on" + addr)
	RegisterRaftSurfstoreServer(grpcServer, server)
	RegisterMetaStoreServer(grpcServer, server.metaStore)
	log.Println("Registered raft server and meta server")
	err = grpcServer.Serve(listener)
	if err != nil {
		return err
	}
	return nil
}
