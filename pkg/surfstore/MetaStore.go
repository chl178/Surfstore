package surfstore

import (
	context "context"
	"errors"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	fmap := make(map[string]*FileMetaData)
	for k,v := range m.FileMetaMap{
		fmap[k] = v
	}
	return &FileInfoMap{FileInfoMap: fmap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) { //TODO: add logic for deleted file
	currentFileMeta, found := m.FileMetaMap[fileMetaData.Filename]
	if !found{
		if fileMetaData.Version == 1{
			m.FileMetaMap[fileMetaData.Filename] = &FileMetaData{Filename: fileMetaData.Filename, Version: fileMetaData.Version, BlockHashList: fileMetaData.BlockHashList}
			return &Version{Version: m.FileMetaMap[fileMetaData.Filename].Version}, nil
		} else { return nil, errors.New("invalid version number for the new file") }
	}
	if currentFileMeta.Version + 1 == fileMetaData.Version{
		// copyOfHashList := make([]string, len(fileMetaData.BlockHashList))
		// copy(copyOfHashList, fileMetaData.BlockHashList)
		m.FileMetaMap[fileMetaData.Filename].BlockHashList = fileMetaData.BlockHashList
		m.FileMetaMap[fileMetaData.Filename].Version = fileMetaData.Version
		return &Version{Version: m.FileMetaMap[fileMetaData.Filename].Version}, nil
	} else {
		return &Version{Version: -1}, nil
	}
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
