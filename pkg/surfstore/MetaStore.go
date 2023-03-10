package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{
		FileInfoMap: m.FileMetaMap,
	}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	if prevMetaData, ok := m.FileMetaMap[fileMetaData.Filename]; ok {
		if fileMetaData.Version-1 == prevMetaData.Version {
			prevMetaData.Version = fileMetaData.Version
			prevMetaData.BlockHashList = fileMetaData.BlockHashList
			// fmt.Printf("after updated new version number should be same:%v\n", prevMetaData.Version == m.FileMetaMap[fileMetaData.Filename].Version)
			return &Version{
				Version: prevMetaData.Version,
			}, nil
		}
		return &Version{
			Version: -1,
		}, fmt.Errorf("wrong version number when updating file info")
	} else {
		m.FileMetaMap[fileMetaData.Filename] = &FileMetaData{
			Filename:      fileMetaData.Filename,
			Version:       fileMetaData.Version,
			BlockHashList: fileMetaData.BlockHashList,
		}
		return &Version{
			Version: fileMetaData.Version,
		}, nil
	}
}

// Given a list of block hashes, find out which block server they belong to.
// Returns a mapping from block server address to blockhashes.
func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	rst := &BlockStoreMap{}
	blockStoreMap := make(map[string]*BlockHashes)
	for _, blockHash := range blockHashesIn.Hashes {
		serverAdd := m.ConsistentHashRing.GetResponsibleServer(blockHash)
		if _, ok := blockStoreMap[serverAdd]; !ok {
			blockStoreMap[serverAdd] = &BlockHashes{
				Hashes: make([]string, 0),
			}
			blockStoreMap[serverAdd].Hashes = append(blockStoreMap[serverAdd].Hashes, blockHash)
		} else {
			blockStoreMap[serverAdd].Hashes = append(blockStoreMap[serverAdd].Hashes, blockHash)
		}
	}
	rst.BlockStoreMap = blockStoreMap
	return rst, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{
		BlockStoreAddrs: m.BlockStoreAddrs,
	}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
