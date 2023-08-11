package surfstore

import (
	"bufio"
	"net"
	//	"google.golang.org/grpc"
	"io"
	"log"

	//	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc"
)
const SURF_CLIENT string = "[Surfstore RPCClient]:"

func LoadRaftConfigFile(filename string) (ipList []string) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	serverCount := 0

	for index := 0; ; index++ {
		lineContent, _, e := configReader.ReadLine()
		if e != nil && e != io.EOF {
			log.Fatal(SURF_CLIENT, "Error During Reading Config", e)
		}

		if e == io.EOF {
			return
		}

		lineString := string(lineContent)
		splitRes := strings.Split(lineString, ": ")
		if index == 0 {
			serverCount, _ = strconv.Atoi(splitRes[1])
			ipList = make([]string, serverCount, serverCount)
		} else {
			ipList[index-1] = splitRes[1]
		}
	}
	return
}

func NewRaftServer(id int64, ips []string, blockStoreAddr string) (*RaftSurfstore, error) {
	// TODO any initialization you need to do here
	nextId := make([]int64, len(ips))
	isCrashedMutex := sync.RWMutex{}
	server := RaftSurfstore{
		// TODO initialize any fields you add here
		serverId: 		id,
		ip:				ips[id],
		ipList: 		ips,
		isLeader:       false,
		pendingReplicas: make([]chan bool, 0),
		term:           0,
		metaStore:      NewMetaStore(blockStoreAddr),
		log:            make([]*UpdateOperation, 0),
		commitIndex: 	-1,
		lastApplied: 	-1,
		nextIndex: 		nextId,
		isCrashed:      false,
		notCrashedCond: sync.NewCond(&isCrashedMutex),
		isCrashedMutex: isCrashedMutex,
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	grpcServer := grpc.NewServer()
	RegisterRaftSurfstoreServer(grpcServer, server)
	// Start listening and serving
	lis, err := net.Listen("tcp", server.ip)
	if err != nil {
		return err
	}
	return grpcServer.Serve(lis)
}
