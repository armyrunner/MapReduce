package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"net/url"
	_ "github.com/mattn/go-sqlite3"
)

func openDatabase(filename string) (*sql.DB, error) {

	path := filename
	options :=
		"?" + "_busy_timeout=10000" +
			"&" + "_case_sensitive_like=OFF" +
			"&" + "_foreign_keys=ON" +
			"&" + "_journal_mode=OFF" +
			"&" + "_locking_mode=NORMAL" +
			"&" + "mode=rw" +
			"&" + "_synchronous=OFF"
	db, err := sql.Open("sqlite3", path+options)
	if err != nil {
		log.Fatalf("opening db: %v", err)
	}
	
	// fmt.Println("Success in opening database")

	return db, nil
}

func createDatabase(filename string) (*sql.DB, error) {

	os.Remove(filename)

	db, err := openDatabase(filename)
	if err != nil {
		return nil,err
	}

	createStmt := `
	CREATE TABLE IF NOT EXISTS pairs(
		key text primary key,
		value text)`

	if _, err := db.Exec(createStmt); err != nil {
		log.Fatalf("running create database: %v", err)
	}

	// fmt.Println("Success in creating table pairs")

	return db, nil
}

func splitDatabase(source, outputPattern string, m int) ([]string, error) {

	// open the database
	// fmt.Println("opening database")
	maindb, err := openDatabase(source)
	//create a list slice of all the outputdatabase
	var allSplits  []*sql.DB
	//create a list of slices of pathnames
	var outputs []string

	if err != nil {
		log.Fatalf("split database: %v", err)
	}

	defer maindb.Close()

	for i := 0; i < m; i++ { 

		var splitDB *sql.DB

		//get outpout of pathnames
		pathnamestring := fmt.Sprintf(outputPattern,i)
		
		//createing new database
		splitDB, err := createDatabase(pathnamestring)

		if err != nil {
			log.Fatalf("Fail to created output database: %v", err)
		}

		allSplits = append(allSplits, splitDB)

		outputs = append(outputs, pathnamestring)
	}

	// query the database for the key/ value on the input file

	rows, err := maindb.Query(`SELECT key, value FROM pairs;`)
	if err != nil {
		log.Fatalf("Did not find key/value in pairs %v",err)
	}

	// initialize the index value
	keys := 0
	index := 0

	// looping through all the rows
	for rows.Next() {

		var key string
		var value string

		err := rows.Scan(&key, &value)

		db := allSplits[index]

		_, err = db.Exec(`INSERT INTO pairs (key,value) values (?, ?)`, key, value)
		if err != nil {
			log.Fatalf("Did not insert key/value into pairs")
		}

		index++
		keys++
		if index >= m{
			index = 0
		}
	}

	//check err if anything went wrong
	if err := rows.Err(); err != nil{
		fmt.Println("We have an error splitDatabase rows.Err()", err);
	}

	maindb.Close()

	// new for loop to close all close;
	for i := 0; i < m; i++{
		allSplits[i].Close()
	}

	return outputs, err
}

// need to make function mergeDatabase

func mergeDatabase(urls []string, path string, temp string) (*sql.DB, error) {

	//create the output databasef
	var outputdb *sql.DB
	outputdb, err := createDatabase(path)
	fmt.Println("created datbase")
	if err != nil {
		log.Fatalf("Did not create database %v", err)

	}

	// loop through all the urls
	fmt.Println("begining to loop through the urls")
	for _, url := range urls {

		//download the url
		path, err = download(url, temp)
		fmt.Println("downloaded url")
		if err != nil {
			outputdb.Close()
			return nil, fmt.Errorf("mergeDatabases: %v", err)
		}

		// gatherinto the databaser or merge database
		err = gatherinto(outputdb, path)
		if err != nil {
			outputdb.Close()
			return nil, fmt.Errorf("mergeDatabases: %v", err)
		}

		//delete temperary string variable
		err = os.Remove(temp)
		if err != nil {
			log.Fatalf("Did not remove temp %v", err)
		}

	}

	return outputdb, nil
}

// need to make function download
func download(URL, path string) (string, error) {

	res, err := http.Get(URL)
	if err != nil {
		return "", fmt.Errorf("download: http.Get error: %v", err)
	}
	if res.StatusCode != http.StatusOK{
		return "", fmt.Errorf("download: http.Get: not okay %v", res.Body)
	}
	defer res.Body.Close()
	fileURL, err := url.Parse(URL)
	if err != nil {
		return "", fmt.Errorf("download: url.Parse: %v", err)
	}
	filepath := fileURL.Path
	log.Printf("filepath: %v", filepath)
	segments := strings.Split(filepath, "/")
	fileName := segments[len(segments)-1]
	fullFilePath := path + "/" + fileName
	log.Printf("fullfile path: %v", fullFilePath)
	output, err := os.Create(fullFilePath)
	if err != nil {
		return "", fmt.Errorf("download: os.Create: %v", err)
	}
	defer output.Close()

	_, err = io.Copy(output, res.Body)
	if err != nil {
		return "", fmt.Errorf("download: io.Copy: %v", err)
	}
	fmt.Println("finished downloading")

	return fullFilePath, err
}

// need to make function gatherinto

func gatherinto(db *sql.DB, path string) error {

	querydatabase := fmt.Sprintf("attach '%s' as merge;", path)

	_, err := db.Exec(querydatabase)
	if err != nil {
		log.Fatalf("Did not attach to merge %v", err)

	}

	_, err = db.Exec(`INSERT INTO pairs SELECT * FROM merge.pairs`)
	if err != nil {
		log.Fatalf("Did not insert into merge.pairs %v", err)

	}

	_, err = db.Exec("detach merge;")
	if err != nil {
		log.Fatalf("Did not detach merge %v", err)
	}

	return nil

}

