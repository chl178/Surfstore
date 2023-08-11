package main

import (
	"cse224/proj5/pkg/surfstore"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc"
)

// Usage String
const USAGE_STRING = "./run-server.sh -s <service_type> -p <port> -l -d (blockStoreAddr*)"

// Set of valid services
var SERVICE_TYPES = map[string]bool{"meta": true, "block": true, "both": true}

// Exit codes
const EX_USAGE int = 64

func main() {
	// Custom flag Usage message
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage of %s:\n", USAGE_STRING)
		flag.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "  -%s: %v\n", f.Name, f.Usage)
		})
		fmt.Fprintf(w, "  (blockStoreAddr*): BlockStore Address (include self if service type is both)\n")
	}

	// Parse command-line argument flags
	service := flag.String("s", "", "(required) Service Type of the Server: meta, block, both")
	port := flag.Int("p", 8080, "(default = 8080) Port to accept connections")
	localOnly := flag.Bool("l", false, "Only listen on localhost")
	debug := flag.Bool("d", false, "Output log statements")
	flag.Parse()

	// Use tail arguments to hold BlockStore address
	args := flag.Args()
	blockStoreAddr := ""
	if len(args) == 1 {
		blockStoreAddr = args[0]
	}

	// Valid service type argument
	if _, ok := SERVICE_TYPES[strings.ToLower(*service)]; !ok {
		flag.Usage()
		os.Exit(EX_USAGE)
	}

	// Add localhost if necessary
	addr := "" // TODO: How will this work in public IP case
	if *localOnly {
		addr += "localhost"
	}
	addr += ":" + strconv.Itoa(*port)

	// Disable log outputs if debug flag is missing
	if !(*debug) {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}
	// until here we just take details for port for server. then depending on the service type we need to 
	// register the services and also initialize the blockstoreaddr. in case of "meta" and "both" service we will
	// have blockstoreaddr
	log.Fatal(startServer(addr, strings.ToLower(*service), blockStoreAddr)) // ?? why passed inside log.Fatal?
}

func startServer(hostAddr string, serviceType string, blockStoreAddr string) error {
	grpcServer := grpc.NewServer()
	// Register RPC services
	if serviceType == "both"{
		metaStore := surfstore.NewMetaStore(blockStoreAddr)
		surfstore.RegisterMetaStoreServer(grpcServer, metaStore)
		blockStore := surfstore.NewBlockStore()
		surfstore.RegisterBlockStoreServer(grpcServer, blockStore)
		// Start listening and serving
		lis, err := net.Listen("tcp", hostAddr)
		if err != nil {
			return fmt.Errorf("failed to listen: %v", err)
		}
		if err := grpcServer.Serve(lis); err != nil {
			return fmt.Errorf("failed to serve: %v", err)
		}
		return nil	
	} else if serviceType == "meta" {
		metaStore := surfstore.NewMetaStore(blockStoreAddr)
		surfstore.RegisterMetaStoreServer(grpcServer, metaStore)
		// Start listening and serving
		lis, err := net.Listen("tcp", hostAddr)
		if err != nil {
			return fmt.Errorf("failed to listen: %v", err)
		}
		if err := grpcServer.Serve(lis); err != nil {
			return fmt.Errorf("failed to serve: %v", err)
		}
		return nil
	} else if serviceType == "block"{
		blockStore := surfstore.NewBlockStore()
		surfstore.RegisterBlockStoreServer(grpcServer, blockStore)
		// Start listening and serving
		lis, err := net.Listen("tcp", hostAddr)
		if err != nil {
			return fmt.Errorf("failed to listen: %v", err)
		}
		if err := grpcServer.Serve(lis); err != nil {
			return fmt.Errorf("failed to serve: %v", err)
		}
		return nil
	}
	return nil
}
