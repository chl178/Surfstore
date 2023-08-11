package surfstore

import (
	"bufio"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
)
type scannedFileMetaData struct{
	Filename string
	BlockList []Block 
	BlockHashList []string
}
func scanFiles(baseDir string) []string {
	var dirFiles []string
	files, err := ioutil.ReadDir(baseDir)
    if err != nil {
        log.Fatal(err)
    }
    for _, f := range files {
		dirFiles = append(dirFiles, f.Name())
    }
	return dirFiles
}
func checkAndCreateLocalIndex(baseDir string, dirFiles []string){
	flag := false
	for _, str := range dirFiles{
		if str == "index.txt"{
			flag = true
			break
		}
	}
	if !flag {
		filename, err := filepath.Abs(ConcatPath(baseDir, "index.txt"))
		if err != nil {
			log.Fatal(err)
		}
		f, err := os.Create(filename)
		if err != nil{
			log.Fatal("error while creating index.txt file")
		}
		defer f.Close()
	}
}
func computeScannedFileMap(baseDir string, dirFiles []string, blockSize int) map[string]*scannedFileMetaData{
	scannedFileInfoMap := make(map[string]*scannedFileMetaData) // scans the current state of local directory and returns datastructure
	for _, file := range dirFiles{
		if file == "index.txt"{
			continue
		}
		hashList := []string{}
		blockList := []Block{}
		filename, err := filepath.Abs(ConcatPath(baseDir, file))
		if err != nil {
			log.Fatal(err)
		}
		f, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		reader := bufio.NewReader(f)
		for {
			buf := make([]byte, blockSize)
			n, err := reader.Read(buf)
			if err != nil {
				if err != io.EOF {
					log.Fatal(err)
				}
				break
			}
			hashList = append(hashList, GetBlockHashString(buf[:n]))
			blockList = append(blockList, Block{BlockData: buf[:n], BlockSize: int32(n)})
		}
		scannedFileInfoMap[file] = &scannedFileMetaData{Filename: file, BlockHashList: hashList, BlockList: blockList}
	}
	return scannedFileInfoMap
}
func contains(list []string, str string) bool {
    for _, ele := range list {
        if ele == str {
            return true
        }
    }
    return false
}
// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	var blockStoreAddr string
	err := client.GetBlockStoreAddr(&blockStoreAddr)
	if err != nil {
		log.Fatal(err)
	}
	// scan base directory 
	dirFiles := scanFiles(client.BaseDir)
	checkAndCreateLocalIndex(client.BaseDir, dirFiles)
	scannedFileMetaDataBeforeDownload := computeScannedFileMap(client.BaseDir, dirFiles, client.BlockSize)
	_ = scannedFileMetaDataBeforeDownload	
	// get local fileInfoMap
	localFileInfoMap, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil{
		log.Fatal(err)
	}
	// get remote fileInfoMap
	remoteFileInfoMap := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&remoteFileInfoMap); err != nil{
		log.Fatal(err)
	}
	
	// Download new files and new updates
	for file, remoteFileMetaData := range remoteFileInfoMap{
		localFileMetaData, found := localFileInfoMap[file]
		if !found { // files on remote but not on local
			log.Println("Downloading new file from server")
			filename, err := filepath.Abs(ConcatPath(client.BaseDir, file))
			if err != nil {
				log.Fatal(err)
			}		
			f, err := os.Create(filename)
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()
			if len(remoteFileMetaData.BlockHashList) == 1 && remoteFileMetaData.BlockHashList[0] == "0"{ // downloading a delete update
				log.Println("entered deletion")
				_, err = os.Stat(filename)
				if err != nil {
					if !os.IsNotExist(err){ // not already deleted from local and still got erro
						log.Fatal(err)
					}
				}
				if !os.IsNotExist(err){ // not already deleted from local then delete it
					err = os.Remove(filename)
					if err != nil{
						log.Fatal(err)
					}	
				}
				fileMetaData := &FileMetaData{Filename: remoteFileMetaData.Filename, Version: remoteFileMetaData.Version, BlockHashList: remoteFileMetaData.BlockHashList}
				localFileInfoMap[file] = fileMetaData // we would have got the same version, filename, and blockhashlist as server									
			}else {
				for _, hash := range remoteFileMetaData.BlockHashList{
					block := Block{}
					err := client.GetBlock(hash, blockStoreAddr, &block)
					if err!= nil{
						log.Fatal(err)
					}
					_, err = f.Write(block.BlockData[:block.BlockSize])
					if err != nil {
						log.Fatal(err)
					}
				}
				fileMetaData := &FileMetaData{Filename: remoteFileMetaData.Filename, Version: remoteFileMetaData.Version, BlockHashList: remoteFileMetaData.BlockHashList}
				localFileInfoMap[file] = fileMetaData // we would have got the same version, filename, and blockhashlist as server					
			}
		} else if remoteFileMetaData.Version > localFileMetaData.Version{
			log.Println("Downloading new updates from server")
			filename, err := filepath.Abs(ConcatPath(client.BaseDir, file))
			if err != nil {
				log.Fatal(err)
			}		
			f, err := os.Create(filename)
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()
			if len(remoteFileMetaData.BlockHashList) == 1 && remoteFileMetaData.BlockHashList[0] == "0"{ // downloading a delete update
				log.Println("entered deletion")
				_, err = os.Stat(filename)
				if err != nil {
					if !os.IsNotExist(err){ // not already deleted from local and still got erro
						log.Fatal(err)
					}
				}
				if !os.IsNotExist(err){ // not already deleted from local then delete it
					err = os.Remove(filename)
					if err != nil{
						log.Fatal(err)
					}	
				}
				fileMetaData := &FileMetaData{Filename: remoteFileMetaData.Filename, Version: remoteFileMetaData.Version, BlockHashList: remoteFileMetaData.BlockHashList}
				localFileInfoMap[file] = fileMetaData // we would have got the same version, filename, and blockhashlist as server									
			}else {
				for _, hash := range remoteFileMetaData.BlockHashList{
					block := Block{}
					err := client.GetBlock(hash, blockStoreAddr, &block)
					if err!= nil{
						log.Fatal(err)
					}
					_, err = f.Write(block.BlockData[:block.BlockSize])
					if err != nil {
						log.Fatal(err)
					}
				}
				fileMetaData := &FileMetaData{Filename: remoteFileMetaData.Filename, Version: remoteFileMetaData.Version, BlockHashList: remoteFileMetaData.BlockHashList}
				localFileInfoMap[file] = fileMetaData // we would have got the same version, filename, and blockhashlist as server					
			}
		}
	}
	// write current state to local index (after download)
	
	dirFilesAfterDownload := scanFiles(client.BaseDir)
	scannedFileMetaDataAfterDownload := computeScannedFileMap(client.BaseDir, dirFilesAfterDownload, client.BlockSize)
		
	// // once we are on same version with remote, add local changes to local index and then do the updates
	// Upload new files and new updates
	// at this step localFileInfoMap must be exactly same as remoteFileInfoMap
	for file, scannedFileMetaData := range scannedFileMetaDataAfterDownload{
		_, found := localFileInfoMap[file]
		if !found{ // handles files that are in local storage but not in local index
			log.Println("Uploading new file to server")
			filename, err := filepath.Abs(ConcatPath(client.BaseDir, file))
			if err != nil {
				log.Fatal(err)
			}		
			f, err := os.Open(filename)
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()
			reader := bufio.NewReader(f)
			for { // put blocks on blockstore
				buf := make([]byte, client.BlockSize)
				n, err := reader.Read(buf)
				if err != nil {
					if err != io.EOF {
						log.Fatal(err)
					}
					break
				}
				var succ bool
				err = client.PutBlock(&Block{BlockData: buf[:n], BlockSize: int32(n)}, blockStoreAddr, &succ)
				if err!= nil{
					log.Fatal(err)
				}
				if !succ{
					log.Fatal("unsuccessful put block operation")
				}
			}
			var latestVersion int32
			err = client.UpdateFile(&FileMetaData{Filename: scannedFileMetaData.Filename, BlockHashList: scannedFileMetaData.BlockHashList, Version: 1}, &latestVersion)
			if err != nil{
				log.Fatal(err)
			}
			if latestVersion == -1{ // download changes from server and discard local changes
				log.Println("Uploading new file failed: Downloading new file from server")
				remoteFileInfoMap := make(map[string]*FileMetaData)
				if err := client.GetFileInfoMap(&remoteFileInfoMap); err != nil{
					log.Fatal(err)
				}
				filename, err := filepath.Abs(ConcatPath(client.BaseDir, file))
				if err != nil {
					log.Fatal(err)
				}		
				f, err := os.Create(filename)
				if err != nil {
					log.Fatal(err)
				}
				defer f.Close()
				remoteFileMetaData := remoteFileInfoMap[file]
				if len(remoteFileMetaData.BlockHashList) == 1 && remoteFileMetaData.BlockHashList[0] == "0"{ // downloading a delete update
					log.Println("entered deletion")
					_, err = os.Stat(filename)
					if err != nil {
						if !os.IsNotExist(err){ // not already deleted from local and still got erro
							log.Fatal(err)
						}
					}
					if !os.IsNotExist(err){ // not already deleted from local then delete it
						err = os.Remove(filename)
						if err != nil{
							log.Fatal(err)
						}	
					}
					fileMetaData := &FileMetaData{Filename: remoteFileMetaData.Filename, Version: remoteFileMetaData.Version, BlockHashList: remoteFileMetaData.BlockHashList}
					localFileInfoMap[file] = fileMetaData // we would have got the same version, filename, and blockhashlist as server									
				}else {
					for _, hash := range remoteFileMetaData.BlockHashList{
						block := Block{}
						err := client.GetBlock(hash, blockStoreAddr, &block)
						if err!= nil{
							log.Fatal(err)
						}
						_, err = f.Write(block.BlockData[:block.BlockSize])
						if err != nil {
							log.Fatal(err)
						}
					}
					fileMetaData := &FileMetaData{Filename: remoteFileMetaData.Filename, Version: remoteFileMetaData.Version, BlockHashList: remoteFileMetaData.BlockHashList}
					localFileInfoMap[file] = fileMetaData // we would have got the same version, filename, and blockhashlist as server					
				}
					
			}else { // do not allocated scannedFileMetaData directly because it does not have version number
				localFileInfoMap[scannedFileMetaData.Filename] = &FileMetaData{Filename: scannedFileMetaData.Filename, BlockHashList: scannedFileMetaData.BlockHashList, Version: latestVersion}
			}
		} else {
			if !reflect.DeepEqual(scannedFileMetaData.BlockHashList, localFileInfoMap[file].BlockHashList){
				log.Println("Uploading new updates to server")
				var residentHashes []string
				err := client.HasBlocks(scannedFileMetaData.BlockHashList, blockStoreAddr, &residentHashes)
				if err != nil{
					log.Fatal(err)
				} // upload missing blocks to blockstore
				for i,hash := range scannedFileMetaData.BlockHashList{
					if !contains(residentHashes, hash){
						var succ bool
						blockData := scannedFileMetaData.BlockList[i].BlockData
						blockSize := scannedFileMetaData.BlockList[i].BlockSize	
						err = client.PutBlock(&Block{BlockData: blockData, BlockSize: blockSize}, blockStoreAddr, &succ)
						if err!= nil{
							log.Fatal(err)
						}
						if !succ{
							log.Fatal("unsuccessful put block operation")
						}		
					}
				} // update metadatastore
				var latestVersion int32
				err = client.UpdateFile(&FileMetaData{Filename: scannedFileMetaData.Filename, BlockHashList: scannedFileMetaData.BlockHashList, Version: localFileInfoMap[file].Version + 1}, &latestVersion)
				if err != nil{
					log.Fatal(err)
				}
				if latestVersion == -1{ // download changes from server and discard local changes
					log.Println("Uploading new updates failed: Downloading new file from server")
					filename, err := filepath.Abs(ConcatPath(client.BaseDir, file))
					remoteFileInfoMap := make(map[string]*FileMetaData)
					if err := client.GetFileInfoMap(&remoteFileInfoMap); err != nil{
						log.Fatal(err)
					}
					if err != nil {
						log.Fatal(err)
					}		
					f, err := os.Create(filename)
					if err != nil {
						log.Fatal(err)
					}
					defer f.Close()
					remoteFileMetaData := remoteFileInfoMap[file]
					if len(remoteFileMetaData.BlockHashList) == 1 && remoteFileMetaData.BlockHashList[0] == "0"{ // downloading a delete update
						log.Println("entered deletion")
						_, err = os.Stat(filename)
						if err != nil {
							if !os.IsNotExist(err){ // not already deleted from local and still got erro
								log.Fatal(err)
							}
						}
						if !os.IsNotExist(err){ // not already deleted from local then delete it
							err = os.Remove(filename)
							if err != nil{
								log.Fatal(err)
							}	
						}
						fileMetaData := &FileMetaData{Filename: remoteFileMetaData.Filename, Version: remoteFileMetaData.Version, BlockHashList: remoteFileMetaData.BlockHashList}
						localFileInfoMap[file] = fileMetaData // we would have got the same version, filename, and blockhashlist as server									
					}else {
						for _, hash := range remoteFileMetaData.BlockHashList{
							block := Block{}
							err := client.GetBlock(hash, blockStoreAddr, &block)
							if err!= nil{
								log.Fatal(err)
							}
							_, err = f.Write(block.BlockData[:block.BlockSize])
							if err != nil {
								log.Fatal(err)
							}
						}
						fileMetaData := &FileMetaData{Filename: remoteFileMetaData.Filename, Version: remoteFileMetaData.Version, BlockHashList: remoteFileMetaData.BlockHashList}
						localFileInfoMap[file] = fileMetaData // we would have got the same version, filename, and blockhashlist as server					
					}
		
				}else { // do not allocated scannedFileMetaData directly because it does not have version number
					localFileInfoMap[scannedFileMetaData.Filename] = &FileMetaData{Filename: scannedFileMetaData.Filename, BlockHashList: scannedFileMetaData.BlockHashList, Version: latestVersion}
				}
			}
		}
	}

	dirFilesAfterUpload := scanFiles(client.BaseDir)
	scannedFileMetaDataAfterUpload := computeScannedFileMap(client.BaseDir, dirFilesAfterUpload, client.BlockSize)
	
	for file, localFileMetaData := range localFileInfoMap{
		_, found := scannedFileMetaDataAfterUpload[file]
		if !found && !(len(localFileMetaData.BlockHashList)==1 && localFileMetaData.BlockHashList[0]=="0"){ // file has been deleted locally
			log.Println("Adding deletion update of file to server")
			var latestVersion int32
			err = client.UpdateFile(&FileMetaData{Filename: localFileMetaData.Filename, BlockHashList: []string{"0"}, Version: localFileMetaData.Version + int32(1)}, &latestVersion)
			if err != nil{
				log.Fatal(err)
			}
			if latestVersion == -1{
				log.Println("Deleting file failed: Downloading updates from server")
				remoteFileInfoMap := make(map[string]*FileMetaData)
				if err := client.GetFileInfoMap(&remoteFileInfoMap); err != nil{
					log.Fatal(err)
				}
				filename, err := filepath.Abs(ConcatPath(client.BaseDir, file))
				if err != nil {
					log.Fatal(err)
				}		
				f, err := os.Create(filename)
				if err != nil {
					log.Fatal(err)
				}
				defer f.Close()
				remoteFileMetaData := remoteFileInfoMap[file]
				if len(remoteFileMetaData.BlockHashList) == 1 && remoteFileMetaData.BlockHashList[0] == "0"{ // downloading a delete update
					log.Println("entered deletion")
					_, err = os.Stat(filename)
					if err != nil {
						if !os.IsNotExist(err){ // not already deleted from local and still got erro
							log.Fatal(err)
						}
					}
					if !os.IsNotExist(err){ // not already deleted from local then delete it
						err = os.Remove(filename)
						if err != nil{
							log.Fatal(err)
						}	
					}
					fileMetaData := &FileMetaData{Filename: remoteFileMetaData.Filename, Version: remoteFileMetaData.Version, BlockHashList: remoteFileMetaData.BlockHashList}
					localFileInfoMap[file] = fileMetaData // we would have got the same version, filename, and blockhashlist as server									
				}else {
					for _, hash := range remoteFileMetaData.BlockHashList{
						block := Block{}
						err := client.GetBlock(hash, blockStoreAddr, &block)
						if err!= nil{
							log.Fatal(err)
						}
						_, err = f.Write(block.BlockData[:block.BlockSize])
						if err != nil {
							log.Fatal(err)
						}
					}
					fileMetaData := &FileMetaData{Filename: remoteFileMetaData.Filename, Version: remoteFileMetaData.Version, BlockHashList: remoteFileMetaData.BlockHashList}
					localFileInfoMap[file] = fileMetaData // we would have got the same version, filename, and blockhashlist as server					
				}					
			}else{
				fileMetaData := &FileMetaData{Filename: localFileMetaData.Filename, BlockHashList: []string{"0"}, Version: latestVersion}
				localFileInfoMap[file] = fileMetaData
			}
		}
	}
	// write current state to local index.txt file
	err = WriteMetaFile(localFileInfoMap, client.BaseDir)
	if err != nil{
		log.Fatal(err)
	}
}
