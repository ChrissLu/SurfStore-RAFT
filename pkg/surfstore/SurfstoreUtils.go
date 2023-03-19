package surfstore

import (
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"reflect"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {

	LocalMap, err := LoadMetaFromMetaFile(client.BaseDir) //return a new local map if not exist
	if err != nil {
		log.Fatalln("Error Loading MetaData", err.Error())
	}

	files, err := ioutil.ReadDir(client.BaseDir)
	// for _, f := range files {
	// 	println(f.Name())
	// }
	if err != nil {
		log.Fatalln("Error reading files", err.Error())
	}

	var filesNotDeleted []string
	// calculate and update local map (local index)
	for _, file := range files {
		if file.Name() == DEFAULT_META_FILENAME || file.IsDir() {
			continue
		}
		//println(file.Name())
		BlockNumber := int(math.Ceil(float64(file.Size()) / float64(client.BlockSize)))

		//calculate new hash list
		new_BlockHashList := make([]string, BlockNumber)
		data, err := os.Open(client.BaseDir + "/" + file.Name())
		if err != nil {
			log.Fatalln("Error opening file: " + file.Name())
		}
		for i := 0; i < BlockNumber; i++ {

			blockData := make([]byte, client.BlockSize)
			n, _ := data.Read(blockData)
			//print("new hashlist:   ")
			blockData = blockData[:n]
			//fmt.Println(blockData)
			if n == 0 {
				new_BlockHashList[i] = "-1"
			}

			new_BlockHashList[i] = GetBlockHashString(blockData)
		}

		// compare with and update local index file
		_, ok := LocalMap[file.Name()]
		if !ok {
			// the file is new
			LocalMap[file.Name()] = &FileMetaData{Filename: file.Name(), Version: 1, BlockHashList: new_BlockHashList} // version start from?
		} else if !reflect.DeepEqual(LocalMap[file.Name()].BlockHashList, new_BlockHashList) {
			// some block changed
			LocalMap[file.Name()].Version += 1
			LocalMap[file.Name()].BlockHashList = new_BlockHashList
		}

		filesNotDeleted = append(filesNotDeleted, file.Name())

	}

	//handle local deleted files
	for fileName, LocalMeta := range LocalMap {
		if !Contains(filesNotDeleted, fileName) {
			if !(len(LocalMeta.BlockHashList) == 1 && LocalMeta.BlockHashList[0] == "0") {
				LocalMeta.Version += 1
				LocalMeta.BlockHashList = []string{"0"}
			}
		}
	}

	// read remote index
	RemoteMap := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&RemoteMap)
	if err != nil {
		fmt.Println("failed:GetFileInfoMap", err.Error())
	}
	fmt.Println("RemoteMap", RemoteMap)

	// get block store addrs, should not be here if there are more than 1 addrs
	var addrs []string
	err = client.GetBlockStoreAddrs(&addrs)
	//fmt.Println(addrs)
	if err != nil {
		fmt.Println("Error getting remote block store addrs", err.Error())
	}
	//println("****************")
	//download
	for filename, RemoteMetaData := range RemoteMap {
		println("remote version: ", RemoteMetaData.Version)
		println("local version: ", LocalMap[filename].Version)
		err = nil
		if LocalMetaData, ok := LocalMap[filename]; ok {
			if RemoteMetaData.Version >= LocalMap[filename].Version {
				println(3)
				err = downloadFile(client, RemoteMetaData, LocalMetaData, addrs)
			}
		} else {
			LocalMap[filename] = &FileMetaData{}
			println(4)
			err = downloadFile(client, RemoteMetaData, LocalMap[filename], addrs)
		}

		if err != nil {
			fmt.Println("Error downloading:", err.Error())
		}
	}
	//println("------------------")

	//upload
	for filename2, LocalMetaData := range LocalMap {
		err = nil
		if RemoteMetaData, ok := RemoteMap[filename2]; ok {
			if LocalMetaData.Version > RemoteMetaData.Version {
				println(1)
				err = uploadFile(client, LocalMetaData, addrs)
			}
		} else {
			//new file in local
			println(2)
			err = uploadFile(client, LocalMetaData, addrs)
		}
		if err != nil {
			fmt.Println("Error uploading: ", err.Error())
		}
	}
	//println("^^^^^^^^^^^^^")
	WriteMetaFile(LocalMap, client.BaseDir)

}

func uploadFile(client RPCClient, LocalMetaData *FileMetaData, blockAddrs []string) error {
	//isNewFile := RemoteMetaData.Filename == ""
	LocalHashList := LocalMetaData.BlockHashList
	var latestVersion int32
	// Open local file, read only
	path := client.BaseDir + "/" + LocalMetaData.Filename
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			client.UpdateFile(LocalMetaData, &latestVersion)
			LocalMetaData.Version = latestVersion
			return nil
		}
		return err
	}
	defer file.Close()

	BlockStoreMap := make(map[string][]string)
	if err = client.GetBlockStoreMap(LocalHashList, &BlockStoreMap); err != nil {
		//println("!!!!!!!!!")
		return err
	}
	//fmt.Println(BlockStoreMap)
	//fmt.Println(LocalHashList)

	// for bServer := range BlockStoreMap {
	// 	HashListOfServer := BlockStoreMap[bServer]

	// upload blocks
	for index, hash := range LocalHashList {
		responsibleServer := ""
		for server := range BlockStoreMap {
			if Contains(BlockStoreMap[server], hash) {
				responsibleServer = server
				break
			}
		}
		if responsibleServer == "" {
			return fmt.Errorf("can't get responsibleServer")
		}

		// var hashList_out []string //subset of localHashList that sre stored in the responsible BlockStore
		// if err = client.HasBlocks(BlockStoreMap[responsibleServer], responsibleServer, &hashList_out); err != nil {
		// 	return fmt.Errorf("failed to call HasBlock: %v", err.Error())
		// }

		// if Contains(hashList_out, hash) {
		// 	// The block is in remote to, no need to upload, but should set offset for os.Read
		// 	b := make([]byte, client.BlockSize)
		// 	file.Read(b)
		// } else {
		var succ bool
		blockData := make([]byte, client.BlockSize)
		n, err := file.Read(blockData)
		//fmt.Println(blockData[:n])
		if err != nil {
			return fmt.Errorf("falied to read %v-th block from the file %v", index, LocalMetaData.Filename)
		}
		block := Block{BlockData: blockData[:n], BlockSize: int32(n)}
		client.PutBlock(&block, responsibleServer, &succ)
		if !succ {
			return fmt.Errorf("PutBlock did not succeed")
		}
		//}
	}

	// }
	//fmt.Println("WWWWWWWW")
	client.UpdateFile(LocalMetaData, &latestVersion)
	if err != nil {
		return err
	} else if latestVersion == -1 {
		LocalMetaData.Version = latestVersion
	}

	return nil

}

func downloadFile(client RPCClient, RemoteMetaData *FileMetaData, LocalMetaData *FileMetaData, blockAddrs []string) error {
	RemoteHashList := RemoteMetaData.BlockHashList
	LocalHashList := LocalMetaData.BlockHashList
	*LocalMetaData = *RemoteMetaData
	//println("jfiuhaskjdkaehjfdkj")
	// Open or create the file, write
	path := client.BaseDir + "/" + RemoteMetaData.Filename
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	//deleted file in remote
	if len(RemoteHashList) == 1 && RemoteHashList[0] == "0" {
		err = os.Remove(file.Name())
		if err != nil {
			return err
		} else {
			return nil
		}
	}

	BlockStoreMap := make(map[string][]string)
	if err = client.GetBlockStoreMap(RemoteHashList, &BlockStoreMap); err != nil {
		return err
	}

	//for bServer := range BlockStoreMap {
	//download blocks from remote
	for index, hash := range RemoteHashList {
		if Contains(LocalHashList, hash) {
			//fmt.Println("Contains")
			continue
			// data:=make([]byte,client.BlockSize)
			// n,_:=file.ReadAt(data,int64(index)*int64(client.BlockSize))
			// data = data[:n]
		} else {
			responsibleServer := ""
			for server := range BlockStoreMap {
				if Contains(BlockStoreMap[server], hash) {
					responsibleServer = server
					break
				}
			}
			if responsibleServer == "" {
				return fmt.Errorf("can't get responsibleServer")
			}
			var block Block
			if err = client.GetBlock(hash, responsibleServer, &block); err != nil {
				return err
			}
			if index == len(RemoteHashList)-1 {
				file.Truncate(int64(index * client.BlockSize))
			}
			file.WriteAt(block.BlockData, int64(index*client.BlockSize))

		}
	}

	//}

	return nil
}

func Contains(slice []string, target string) bool {
	for _, element := range slice {
		if element == target {
			return true
		}
	}
	return false
}
