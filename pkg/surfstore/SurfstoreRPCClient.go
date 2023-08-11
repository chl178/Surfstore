package surfstore

import (
	context "context"
	"fmt"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir       string
	BlockSize     int
}
// we are creating a sort of wrapper for BlockStoreClient and MetaStoreClient
func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	success, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	*succ = success.Flag

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	hashesOut, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = hashesOut.Hashes

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// connect to the server
	var fileInfoMap *FileInfoMap
	var success bool
	var globalErr error
	success = false
	for _, metaStoreAddr := range surfClient.MetaStoreAddrs{
		if success{
			break
		}
		func() {
			var conn *grpc.ClientConn
			conn, globalErr = grpc.Dial(metaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if globalErr != nil {
				return
			}
			c := NewRaftSurfstoreClient(conn)

			// perform the call
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			fileInfoMap, globalErr = c.GetFileInfoMap(ctx, &emptypb.Empty{})
			if globalErr != nil { // any sort of error, move on and look for new one
				// if !(strings.Contains(globalErr.Error(), STR_NOT_LEADER) || strings.Contains(globalErr.Error(), STR_SERVER_CRASHED)){
				// 	conn.Close()
				// 	return
				// } else { // not a leader or crashed then close the connection and report error
				// 	conn.Close()
				// 	return
				// }
				conn.Close()
				return
			}
			success = true
			if success{
				globalErr = conn.Close() // close the conn to leader because we have got the data
				return
			}
		}()
	}
	*serverFileInfoMap = fileInfoMap.FileInfoMap
	if success{
		return nil
	} else {
		return fmt.Errorf("Unable to satisfy GetFileInfoMap GRPC request from any server")
	}
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	// connect to the server
	var ver *Version
	var success bool
	var globalErr error
	success = false
	for _, metaStoreAddr := range surfClient.MetaStoreAddrs{
		if success{
			break
		}
		func() {
			var conn *grpc.ClientConn
			conn, globalErr = grpc.Dial(metaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if globalErr != nil {
				return
			}
			c := NewRaftSurfstoreClient(conn)

			// perform the call
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			ver, globalErr = c.UpdateFile(ctx, fileMetaData)
			if globalErr != nil {
				// if !(strings.Contains(globalErr.Error(), STR_NOT_LEADER)){
				// 	conn.Close()
				// 	return
				// } else { // not a leader or crashed then close the connection and report error
				// 	conn.Close()
				// 	return
				// }
				conn.Close()
				return
			}
			success = true
			if success{
				globalErr = conn.Close() // close the conn to leader because we have got the data
				return
			}
		}()
	}
	*latestVersion = ver.Version	
	if success{
		return nil
	} else {
		return fmt.Errorf("Unable to satisfy UpdateFile GRPC request from any server")
	}
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	// connect to the server
	var bAddr *BlockStoreAddr
	var success bool
	var globalErr error
	success = false
	for _, metaStoreAddr := range surfClient.MetaStoreAddrs{
		if success{
			break
		}
		func() {
			var conn *grpc.ClientConn
			conn, globalErr = grpc.Dial(metaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if globalErr != nil {
				return
			}
			c := NewRaftSurfstoreClient(conn)

			// perform the call
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			bAddr, globalErr = c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
			if globalErr != nil {
				// if !(strings.Contains(globalErr.Error(), STR_NOT_LEADER)){
				// 	conn.Close()
				// 	return
				// } else { // not a leader or crashed then close the connection and report error
				// 	conn.Close()
				// 	return
				// }
				conn.Close()
				return
			}
			success = true
			if success{
				globalErr = conn.Close() // close the conn to leader because we have got the data
				return
			}
		}()
	}
	*blockStoreAddr = bAddr.Addr
	if success{
		return nil
	} else {
		return fmt.Errorf("Unable to satisfy GetBlockStoreAddr GRPC request from any server")
	}
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
			MetaStoreAddrs: addrs,
			BaseDir:       baseDir,
			BlockSize:     blockSize,
	}
}