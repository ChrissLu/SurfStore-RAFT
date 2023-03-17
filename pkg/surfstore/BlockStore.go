package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	//mtx      sync.Mutex
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	//bs.mtx.Lock()
	block, ok := bs.BlockMap[blockHash.Hash]
	//bs.mtx.Unlock()
	if !ok {
		return &Block{}, fmt.Errorf("no block found")
	}
	return block, nil

}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	hash := GetBlockHashString(block.BlockData)
	//bs.mtx.Lock()
	bs.BlockMap[hash] = block
	//bs.mtx.Unlock()
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	HashesOut := &BlockHashes{Hashes: make([]string, 0)}
	for _, hashIn := range blockHashesIn.Hashes {
		//bs.mtx.Lock()
		if _, ok := bs.BlockMap[hashIn]; ok {
			HashesOut.Hashes = append(HashesOut.Hashes, hashIn)
		}
		//bs.mtx.Unlock()
	}
	return HashesOut, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	hashes := make([]string, 0, len(bs.BlockMap))
	for hash := range bs.BlockMap {
		hashes = append(hashes, hash)
	}
	return &BlockHashes{Hashes: hashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
