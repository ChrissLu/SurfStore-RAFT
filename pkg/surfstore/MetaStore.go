package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap map[string]*FileMetaData
	//BlockStoreAddr     string
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	//mtx                sync.Mutex
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	FileName := fileMetaData.Filename
	new_Version := fileMetaData.Version
	version := new_Version

	//m.mtx.Lock()
	_, ok := m.FileMetaMap[FileName]
	if !ok {
		m.FileMetaMap[FileName] = fileMetaData
	} else {
		if new_Version != m.FileMetaMap[FileName].Version+1 {
			version = -1
		} else {
			m.FileMetaMap[FileName] = fileMetaData
		}
	}
	//m.mtx.Unlock()
	//PrintMetaMap(m.FileMetaMap)
	return &Version{Version: version}, nil

}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	//println(99999999)
	blockStoreMap := make(map[string]*BlockHashes)
	for _, blockHash := range blockHashesIn.Hashes {
		ResponsibleServer := m.ConsistentHashRing.GetResponsibleServer(blockHash)
		if _, ok := blockStoreMap[ResponsibleServer]; !ok {
			blockStoreMap[ResponsibleServer] = &BlockHashes{Hashes: make([]string, 0)}
		}
		blockStoreMap[ResponsibleServer].Hashes = append(blockStoreMap[ResponsibleServer].Hashes, blockHash)
	}
	//println(888888)
	return &BlockStoreMap{BlockStoreMap: blockStoreMap}, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
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
