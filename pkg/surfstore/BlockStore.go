package surfstore

import (
	context "context"
	"errors"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	val, found := bs.BlockMap[blockHash.Hash]
	if found {
		return &Block{BlockData: val.BlockData, BlockSize: val.BlockSize}, nil
	} else{
		return nil, errors.New("hash not found")
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	blockHash := GetBlockHashString(block.BlockData)
	b := new(Block)
	b.BlockData = block.BlockData
	b.BlockSize = block.BlockSize
	bs.BlockMap[blockHash] = b
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	var residentBlockHashes []string
	for _, hash := range blockHashesIn.Hashes{
		_, found := bs.BlockMap[hash]
		if found{
			residentBlockHashes = append(residentBlockHashes, hash)
		}
	}
	return &BlockHashes{Hashes: residentBlockHashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
