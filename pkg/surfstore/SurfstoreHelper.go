package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes(fileName,version,hashIndex,hashValue)
							values (?,?,?,?)`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	defer db.Close()

	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()

	for fileName, fileMeta := range fileMetas {

		for index, hash := range fileMeta.BlockHashList {
			statement, err = db.Prepare(insertTuple)
			if err != nil {
				log.Fatal("Error During Meta Write Back")
			}
			statement.Exec(fileName, fileMeta.Version, index, hash)
		}
	}
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `select distinct fileName
									from indexes`

const getTuplesByFileName string = `select version, hashValue
									from indexes 
									where fileName = ?
									order by hashIndex`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	// if os.IsNotExist(e) {
	// 	file, _ := os.Create(metaFilePath)
	// 	metaFileStats, _ = file.Stat()
	// 	file.Close()
	// } else
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		return fileMetaMap, fmt.Errorf("failed to open sql file")
	}
	defer db.Close()

	//get distinct file names
	rows, err := db.Query(getDistinctFileName)
	if err != nil {
		return fileMetaMap, fmt.Errorf("failed to get distinct file names")
	}

	fileNames := make([]string, 0)
	for rows.Next() {
		var name string
		rows.Scan(&name)
		fileNames = append(fileNames, name)
	}

	//
	for _, filename := range fileNames {

		rows, err := db.Query(getTuplesByFileName, filename)
		if err != nil {
			return fileMetaMap, fmt.Errorf("failed to get tuples from the file: %v", filename)
		}
		var version int
		var hashList []string
		for rows.Next() {
			var hash string
			var v int
			rows.Scan(&v, &hash)
			version = v
			hashList = append(hashList, hash)
		}
		FileMeta := &FileMetaData{Filename: filename, Version: int32(version), BlockHashList: hashList}
		fileMetaMap[filename] = FileMeta

	}

	return fileMetaMap, nil

}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
