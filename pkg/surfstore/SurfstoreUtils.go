package surfstore

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

func ClientSync(client RPCClient) {
	localFiles, err := getLocalFileHashesMap(client)
	if err != nil {
		log.Fatalf("error when loading local file hashes, %v", err)
	}
	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Fatalf("error when loading local index, %v", err)
	}
	remoteIndex := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remoteIndex)
	if err != nil {
		log.Fatalf("error when loading remote index, %v", err)
	}
	currStateMap := getCurrStateMap(localFiles, localIndex)
	newLocalIndex := doSync(currStateMap, remoteIndex, client)
	err = WriteMetaFile(newLocalIndex, client.BaseDir)
	if err != nil {
		log.Fatalf("error when writing new local index file, %v", err)
	}
}

// return the hash values of the local files in the base directory
func getLocalFileHashesMap(client RPCClient) (map[string]*BlockHashes, error) {
	baseDir, _ := filepath.Abs(client.BaseDir)
	metaFileStats, err := os.Stat(baseDir)
	fileHashesMap := make(map[string]*BlockHashes)
	if err != nil || !metaFileStats.IsDir() {
		return nil, err
	}
	files, err := ioutil.ReadDir(baseDir)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		if file.Name() == "index.db" || file.IsDir() || strings.Contains(file.Name(), ",") || strings.Contains(file.Name(), "/") {
			continue
		}
		hashes, _, err := getHashesAndBlocks(file.Name(), client)
		if err != nil {
			return nil, err
		}
		fileHashesMap[file.Name()] = &BlockHashes{
			Hashes: hashes,
		}
	}
	return fileHashesMap, nil
}

// return hash values of a local file
func getHashesAndBlocks(filename string, client RPCClient) ([]string, []*Block, error) {
	baseDir, _ := filepath.Abs(client.BaseDir)
	filePath := ConcatPath(baseDir, filename)
	rst := make([]string, 0)
	blockes := make([]*Block, 0)
	allData, err := os.ReadFile(filePath)
	if err != nil {
		return nil, nil, err
	}
	for i := 0; i < len(allData); i += client.BlockSize {
		end := 0
		if i+client.BlockSize < len(allData) {
			end = i + client.BlockSize
		} else {
			end = len(allData)
		}
		rst = append(rst, GetBlockHashString(allData[i:end]))
		blockes = append(blockes, &Block{BlockData: allData[i:end], BlockSize: int32(end - i)})
	}
	return rst, blockes, nil
}

func hasSameHashList(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// compare local files and the local index to get a current state map
//  1. traverse all local files
//     1.1 new local file, version # = 1
//     1.2 changed file, version # += 1, update block hash list
//     1.3 unchanged file, version # stays the same
//  2. traverse local index
//     2.1 a file doesn't exist in the local files, version # += 1, update block hash list to [0]
func getCurrStateMap(localFiles map[string]*BlockHashes, localIndex map[string]*FileMetaData) map[string]*FileMetaData {
	rst := make(map[string]*FileMetaData)
	for filename := range localFiles {
		if metaData, ok := localIndex[filename]; ok {
			// unchanged
			if hasSameHashList(metaData.BlockHashList, localFiles[filename].Hashes) {
				rst[filename] = &FileMetaData{
					Filename:      metaData.Filename,
					Version:       metaData.Version,
					BlockHashList: localFiles[filename].Hashes,
				}
			} else { // changed
				rst[filename] = &FileMetaData{
					Filename:      metaData.Filename,
					Version:       metaData.Version + 1,
					BlockHashList: localFiles[filename].Hashes,
				}
			}
		} else { // new file
			rst[filename] = &FileMetaData{
				Filename:      filename,
				Version:       1,
				BlockHashList: localFiles[filename].Hashes,
			}
		}
	}

	for filename := range localIndex {
		// deleted file
		if _, ok := localFiles[filename]; !ok {
			rst[filename] = &FileMetaData{
				Filename:      filename,
				Version:       localIndex[filename].Version + 1,
				BlockHashList: []string{"0"},
			}
		}
	}
	return rst
}

// do sync between the current state and remote index, return the final local index
//  1. current state has new files, version == 1 && remote doesn't have the file
//     1.1 PutBlock
//     1.2 UpdateFile, check the returned version
//     1.3 UpdateLocalIndex if the return version != -1
//  2. remote index has files that local index doesn't, block hashes != [0]
//     2.1 GetBlock and construct the file
//     2.2 UpdateLocalIndex
//  3. both have the file
//     3.1 same hashes
//     - update local index's version number to the remote one
//     3.2 different hashes
//     - cur state version <= remote version
//     - GetBlock, construct file and update local index
//     - cur state verion > remote version(should be one greater)
//     - PutBlock, UpdateFile, check the return version, UpdateLocalIndex if the returned version != -1
func doSync(curStateMap, remoteIndex map[string]*FileMetaData, client RPCClient) map[string]*FileMetaData {
	rst := make(map[string]*FileMetaData)
	for filename, curState := range curStateMap {
		if _, ok := remoteIndex[filename]; ok {
			remoteState := remoteIndex[filename]
			// unchanged
			if hasSameHashList(curState.BlockHashList, remoteState.BlockHashList) {
				log.Printf("same content, updating version number on %v", filename)
				rst[filename] = &FileMetaData{
					Filename:      filename,
					Version:       remoteState.Version,
					BlockHashList: curState.BlockHashList,
				}
			} else { // changed
				if remoteState.Version >= curState.Version { // downlowd
					log.Printf("different content, downloading a new version %v", filename)
					err := downloadFile(remoteIndex[filename], rst, client)
					if err != nil {
						log.Printf("error when downloading a different version of file, %v\n", err)
						continue
					}
				} else { // upload
					log.Printf("different content, uploading a new version %v\n", filename)
					err := uploadFile(curState, rst, client)
					if err != nil {
						log.Printf("error when uploading a different version file, %v\n", err)
						continue
					}
				}
			}
		} else { // new file to upload
			log.Printf("uploading a new file %v", filename)
			err := uploadFile(curState, rst, client)
			if err != nil {
				log.Printf("error when uploading a new file, %v\n", err)
				continue
			}
		}
	}

	for filename := range remoteIndex {
		// new file to download
		if _, ok := curStateMap[filename]; !ok && remoteIndex[filename].BlockHashList[0] != "0" {
			log.Printf("downloading a new file %v", filename)
			err := downloadFile(remoteIndex[filename], rst, client)
			if err != nil {
				log.Printf("error when downloading a new file, %v\n", err)
				continue
			}
		}
	}
	return rst
}

func uploadFile(fileMetaData *FileMetaData, rst map[string]*FileMetaData, client RPCClient) error {
	var success bool
	var latestVersion int32
	blockStoreMap := make(map[string][]string)
	hashToBlockMap := make(map[string]*Block)
	if isEmptyFile(fileMetaData) {
		log.Printf("uploading a deleted file\n")
		err := client.UpdateFile(fileMetaData, &latestVersion)
		if err != nil || latestVersion == -1 {
			return fmt.Errorf("update file info failed, version error, %v", err)
		}
		rst[fileMetaData.Filename] = &FileMetaData{
			Filename:      fileMetaData.Filename,
			Version:       latestVersion,
			BlockHashList: fileMetaData.BlockHashList,
		}
		return nil
	}
	_, blockes, err := getHashesAndBlocks(fileMetaData.Filename, client)
	if err != nil {
		return err
	}
	for _, block := range blockes {
		hashToBlockMap[GetBlockHashString(block.BlockData)] = block
	}
	err = client.GetBlockStoreMap(fileMetaData.BlockHashList, &blockStoreMap)
	if err != nil {
		return fmt.Errorf("get block store map failed %v", err)
	}
	for blockStoreAdd, hashes := range blockStoreMap {
		for _, hash := range hashes {
			block := hashToBlockMap[hash]
			err = client.PutBlock(block, blockStoreAdd, &success)
			if err != nil || !success {
				return fmt.Errorf("put a block failed, %v", err)
			}
			log.Printf("put a block with hashvalue %v to blockstore %v\n", hash, blockStoreAdd)
		}
	}
	err = client.UpdateFile(fileMetaData, &latestVersion)
	if err != nil || latestVersion == -1 {
		return fmt.Errorf("update file info failed, version error, %v", err)
	}
	rst[fileMetaData.Filename] = &FileMetaData{
		Filename:      fileMetaData.Filename,
		Version:       latestVersion,
		BlockHashList: fileMetaData.BlockHashList,
	}
	return nil
}

func downloadFile(fileMetaData *FileMetaData, rst map[string]*FileMetaData, client RPCClient) error {
	blockes := make([]*Block, 0)
	baseDir, _ := filepath.Abs(client.BaseDir)
	filePath := ConcatPath(baseDir, fileMetaData.Filename)
	blockStoreMap := make(map[string][]string)
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("create file %v failed", fileMetaData.Filename)
	}
	if isEmptyFile(fileMetaData) {
		log.Printf("file %v is deleted in remote, deleting in local...\n", fileMetaData.Filename)
		os.Remove(filePath)
		rst[fileMetaData.Filename] = &FileMetaData{
			Filename:      fileMetaData.Filename,
			Version:       fileMetaData.Version,
			BlockHashList: fileMetaData.BlockHashList,
		}
		return nil
	}

	err = client.GetBlockStoreMap(fileMetaData.BlockHashList, &blockStoreMap)
	if err != nil {
		return fmt.Errorf("get block store map failed, %v", err)
	}
	for _, hash := range fileMetaData.BlockHashList {
		blockStoreAdd, err := getBlockStoreAdd(hash, &blockStoreMap)
		if err != nil {
			return fmt.Errorf("get the block store add faield %v", err)
		}
		block := &Block{}
		err = client.GetBlock(hash, blockStoreAdd, block)
		if err != nil {
			return fmt.Errorf("get a block failed, %v", err)
		}
		blockes = append(blockes, block)
		log.Printf("get a block with hashvalue %v from block store %v\n", hash, blockStoreAdd)
	}
	for _, block := range blockes {
		n, err := file.Write(block.BlockData)
		if err != nil || n != int(block.BlockSize) {
			return fmt.Errorf("error when writing to a file %v", err)
		}
	}
	rst[fileMetaData.Filename] = &FileMetaData{
		Filename:      fileMetaData.Filename,
		Version:       fileMetaData.Version,
		BlockHashList: fileMetaData.BlockHashList,
	}
	return nil
}

// given a block store hash value, return the block store address which stores that block
func getBlockStoreAdd(blockHash string, blockStoreMap *map[string][]string) (string, error) {
	for serverAdd, hashes := range *blockStoreMap {
		for _, hash := range hashes {
			if blockHash == hash {
				return serverAdd, nil
			}
		}
	}
	return "", fmt.Errorf("can't find the right block store server")
}

// is deleted or is empty
func isEmptyFile(fileMetaData *FileMetaData) bool {
	return len(fileMetaData.BlockHashList) == 0 || fileMetaData.BlockHashList[0] == "0"
}
