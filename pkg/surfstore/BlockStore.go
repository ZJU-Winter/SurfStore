package surfstore

import (
	context "context"
	"fmt"
	"log"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	if block, ok := bs.BlockMap[blockHash.Hash]; ok {
		return block, nil
	}
	return nil, fmt.Errorf("fail to find a block with hashvalue %v", blockHash.GetHash())
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	hash := GetBlockHashString(block.BlockData)
	if _, ok := bs.BlockMap[hash]; ok {
		log.Printf("put a existed block with hashvalue %v\n", hash)
		return &Success{
			Flag: true,
		}, nil
	}
	bs.BlockMap[hash] = block
	log.Printf("put a block with hashvalue %v\n", hash)
	return &Success{
		Flag: true,
	}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	rst := &BlockHashes{
		Hashes: make([]string, 0, len(blockHashesIn.Hashes)),
	}
	for _, hash := range blockHashesIn.Hashes {
		if _, ok := bs.BlockMap[hash]; ok {
			rst.Hashes = append(rst.Hashes, hash)
		}
	}
	return rst, nil
}

// Returns a list containing all block hashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	rst := &BlockHashes{}
	hashes := make([]string, 0)
	for hash := range bs.BlockMap {
		hashes = append(hashes, hash)
	}
	rst.Hashes = hashes
	return rst, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
