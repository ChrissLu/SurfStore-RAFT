package surfstore

import (
	context "context"
	"fmt"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
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

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	h, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	*blockHashes = h.Hashes
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	*succ = s.Flag
	return conn.Close()

}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	bhout, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = bhout.Hashes
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	for _, metaStore := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(metaStore, grpc.WithInsecure())
		if err != nil {
			continue
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		FIM, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			conn.Close()
			continue
		}
		*serverFileInfoMap = FIM.FileInfoMap
		return conn.Close()
	}
	return fmt.Errorf("cluster down, no leader found")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for _, metaStore := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(metaStore, grpc.WithInsecure())
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		v, err := c.UpdateFile(ctx, fileMetaData)
		if err != nil {
			fmt.Println(err.Error())
			conn.Close()
			continue
		}
		*latestVersion = v.Version
		//fmt.Println(77777)
		return conn.Close()
	}
	return fmt.Errorf("cluster down, no leader found")
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	for _, metaStore := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(metaStore, grpc.WithInsecure())
		if err != nil {
			continue
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		bsm, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})
		if err != nil {
			//print("222222222222222")
			//fmt.Println("failed to GetBlockStoreMap in client", err.Error())
			conn.Close()
			continue
		}
		tempMap := make(map[string][]string)
		for server := range bsm.BlockStoreMap {
			tempMap[server] = bsm.BlockStoreMap[server].Hashes
		}
		*blockStoreMap = tempMap
		return conn.Close()

	}
	return fmt.Errorf("cluster down, no leader found")
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for _, metaStore := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(metaStore, grpc.WithInsecure())
		if err != nil {
			continue
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		a, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		if err != nil {
			conn.Close()
			continue
		}
		*blockStoreAddrs = a.BlockStoreAddrs

		return conn.Close()
	}
	return fmt.Errorf("cluster down, no leader found")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
