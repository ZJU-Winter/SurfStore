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

func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/* Writing To Local Metadata File */
const createTableSQL string = `create table if not exists indexes (
    fileName TEXT,
    version INT,
    hashIndex INT,
    hashValue TEXT
);`

func insertTupleSQL(filename string, version, hashIndex int, hashValue string) string {
	return fmt.Sprintf("insert into indexes(fileName, version, hashIndex, hashValue) values (\"%v\", %v, %v, \"%v\");", filename, version, hashIndex, hashValue)
}

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove the index.db file if it exists
	outputMetaPath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			return fmt.Errorf("error when deleting exsiting index file, %v", e)
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		return fmt.Errorf("error when creating index file, %v", err)
	}

	// create the table and create a new index.db file
	createStatement, err := db.Prepare(createTableSQL)
	if err != nil {
		return fmt.Errorf("error when preparing creation statement, %v", err)
	}
	defer createStatement.Close()
	createStatement.Exec()

	// insert tuples
	for filename, fileMetaData := range fileMetas {
		for i, hashValue := range fileMetaData.BlockHashList {
			log.Println(insertTupleSQL(filename, int(fileMetaData.Version), i, hashValue))
			insertSatement, err := db.Prepare(insertTupleSQL(filename, int(fileMetaData.Version), i, hashValue))
			if err != nil {
				return fmt.Errorf("error when preparing insertion statement, %v", err)
			}
			defer insertSatement.Close()
			insertSatement.Exec()
		}
	}
	return nil
}

/* Reading From Local Metadata File */
const selectDistinctFileNameSQL string = `select distinct fileName from indexes;`

func selectFileInfoSQL(filename string) string {
	return fmt.Sprintf("select version, hashValue from indexes where fileName = \"%v\" order by hashIndex asc;", filename)
}

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, err error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, err := os.Stat(metaFilePath)
	if err != nil || metaFileStats.IsDir() {
		log.Println("no index.db when loading, created")
		_, err := os.Create(metaFilePath)
		if err != nil {
			return fileMetaMap, err
		}
		db, err := sql.Open("sqlite3", metaFilePath)
		if err != nil {
			return fileMetaMap, fmt.Errorf("error when creating index file, %v", err)
		}
		createStatement, err := db.Prepare(createTableSQL)
		if err != nil {
			return fileMetaMap, fmt.Errorf("error when preparing creation statement, %v", err)
		}
		defer createStatement.Close()
		createStatement.Exec()
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		return nil, fmt.Errorf("error when opening index file, %v", err)
	}

	// get filenames
	filenames := make([]string, 0)
	distinctFileNameStatement, err := db.Prepare(selectDistinctFileNameSQL)
	if err != nil {
		return nil, fmt.Errorf("error when preparing distinct file name statement, %v", err)
	}
	defer distinctFileNameStatement.Close()
	rows, err := distinctFileNameStatement.Query()
	if err != nil {
		return nil, fmt.Errorf("error when querying distinct file names, %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var filename string
		if err := rows.Scan(&filename); err != nil {
			return nil, fmt.Errorf("error when scanning file names, %v", err)
		}
		filenames = append(filenames, filename)
	}

	// get file info
	for _, filename := range filenames {
		hashList := make([]string, 0)
		var version int
		var hashValue string
		fileInfoStatement, err := db.Prepare(selectFileInfoSQL(filename))
		if err != nil {
			return nil, fmt.Errorf("error when preparing file info statement, %v", err)
		}
		defer fileInfoStatement.Close()
		rows, err := fileInfoStatement.Query()
		if err != nil {
			return nil, fmt.Errorf("error when querying file info, %v", err)
		}
		defer rows.Close()
		for rows.Next() {
			if err := rows.Scan(&version, &hashValue); err != nil {
				return nil, fmt.Errorf("error when scanning file info, %v", err)
			}
			hashList = append(hashList, hashValue)
		}
		fileMetaMap[filename] = &FileMetaData{
			Filename:      filename,
			Version:       int32(version),
			BlockHashList: hashList,
		}
	}
	return fileMetaMap, nil
}

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
