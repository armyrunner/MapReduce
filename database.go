package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
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

	return db, nil
}

func createDatabase(filename string) (*sql.DB, error) {

	os.Remove(filename)

	db, err := openDatabase(filename)
	if err != nil {
		os.Remove(filename)
		return nil,err
	}

	createStmt := `
	CREATE TABLE IF NOT EXISTS pairs(
		key text primary key,
		value text)`

	if _, err := db.Exec(createStmt); err != nil {
		db.Close()
		os.Remove(filename)
		return nil, err
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
		pathnamestring := filepath.Join(outputPattern,i)
		
		//createing new database
		splitDB, err := createDatabase(pathnamestring)

		if err != nil {
			for _, t := range allSplits{
				t.Close()
			}
			maindb.Close()
			return nil, err
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
			return nil, err

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
	for i := 0; i < m; i++ {
		allSplits[i].Close()
	}
	
	return outputs, err
}

// need to make function mergeDatabase

func mergeDatabase(urls []string, path string, temp string) (*sql.DB, error) {

	//create the output database
	outputdb, err := createDatabase(path)
	fmt.Println("created datbase")
	if err != nil {
		os.Remove(path)
		return nil,err

	}

	// loop through all the urls
	fmt.Println("begining to loop through the urls")
	tempdb, err := createDatabase(temp)
	for _, url := range urls {

		//download the url
		err = download(url, temp)
		fmt.Println("downloaded urls")
		if err != nil {
			tempdb.Close()
			os.Remove(temp)
			return nil,err

		}

		// gatherinto the databaser or merge database
		err = gatherinto(outputdb, path)
		if err != nil {
			tempdb.Close()
			return nil, err
		}

	}

	outputdb.Close()
	return outputdb, nil
}

// need to make function download
func download(url, path string) error {

	fmt.Println("started downloads")
	pathname, err := os.Create(path)
	if err != nil {
		log.Fatalf("Failed to create file %v", err)

	}
	defer pathname.Close()

	res, err := http.Get(url)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	_, err = io.Copy(pathname, res.Body)
	if err != nil {
		return err
	}
	fmt.Println("finished downloading")

	return nil
}

// need to make function gatherinto

func gatherinto(db *sql.DB, path string) error {

	querydatabase := fmt.Sprintf("attach '%s' as merge;", path)

	_, err := db.Exec(querydatabase)
	if err != nil {
		db.Close()
		os.Remove(path)
		return err

	}

	_, err = db.Exec(`INSERT INTO pairs SELECT * FROM merge.pairs`)
	if err != nil {
		db.Close()
		os.Remove(path)
		return err

	}

	_, err = db.Exec("detach merge;")
	if err != nil {
		db.Close()
		os.Remove(path)
		return err
	}

	return nil

}

