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
	fmt.Println("Success in opening database")
	db.Close()
	return db, nil
}

func createDatabase(filename string) (*sql.DB, error) {

	os.Remove(filename)

	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		log.Fatalf("opening db: %v", err)
	}

	createStmt := `
	CREATE TABLE IF NOT EXISTS pairs(
		key text primary key,
		value text)`

	if _, err := db.Exec(createStmt); err != nil {
		log.Fatalf("running create database: %v", err)
	}

	fmt.Println("Success in creating table pairs")

	return db, nil
}

func splitDatabase(source, outputPattern string, m int) ([]string, error) {

	// open the database
	fmt.Println("opening database")
	db, err := openDatabase(source)
	if err != nil {
		log.Fatalf("split database: %v", err)
	}

	defer db.Close()

	//create a list slice of all the outputdatabase
	var outputdb  []*sql.DB

	//create a list of slices of pathnames
	var pathnames []string

	for i := 0; i < m; i++ { 

		//get outpout of pathnames
		pathnamestring := fmt.Sprintf(outputPattern, i)

		//createing new database
		outputdatabase, err := createDatabase(pathnamestring)

		if err != nil {
			log.Fatalf("Fail to created output database", err)
		}

		outputdb = append(outputdb, outputdatabase)

		pathnames = append(pathnames, pathnamestring)
	}

	// query the database for the key/ value on the input file

	rows, err := db.Query("SELECT key, value FROM pairs;")
	if err != nil {
		log.Fatalf("Did not find key/value in pairs")
	}


	// initialize the index value
	index := 0


	// looping through all the rows
	for rows.Next() {

		var key string
		var value string

		db := outputdb[index]
		err = rows.Scan(&key, &value)

		// if err != nil {
		// 	log.Fatalf("Did not insert key/value into pairs")
		// }

		_, err := db.Exec(`INSERT INTO pairs (key,value) values(?,?)`, key, value)
		if err != nil {
			log.Fatalf("Did not insert key/value into pairs")
		}
		index++

		if index >= len(outputdb){
			index = 0
		}
	}

	defer rows.Close()

	//check err if anything went wrong

	// new for loop to close all close;

	return pathnames, nil
}

// need to make function mergeDatabase

func mergeDatabase(urls []string, path string, temp string) (*sql.DB, error) {

	//create the output databaser
	outputdb, err := createDatabase(path)
	if err != nil {
		log.Fatalf("Did not create database %v", err)

	}

	// loop through all the urls
	for _, url := range urls {

		//download the url
		err = download(url, temp)
		if err != nil {
			log.Fatalf("Did not dounload %v", err)

		}

		// gatherinto the databaser or merge database
		err = gatherinto(outputdb, path)
		if err != nil {
			log.Printf("Did not gatherinto %v", err)

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
func download(url, path string) error {

	pathname, err := os.Create(path)
	if err != nil {
		log.Fatalf("Failed to create file %v", err)

	}
	defer pathname.Close()

	res, err := http.Get(url)
	if err != nil {

		log.Fatalf("Failed to download file %v", err)

	}
	defer res.Body.Close()

	_, err = io.Copy(pathname, res.Body)
	if err != nil {
		log.Fatalf("Failed to copy file %v", err)
	}

	return nil
}

// need to make function gatherinto

func gatherinto(db *sql.DB, path string) error {

	querydatabase := fmt.Sprintf("attach '%s' as merge;", path)

	_, err := db.Exec(querydatabase)
	if err != nil {
		log.Fatalf("Did not attach to merge %v", err)

	}

	_, err = db.Exec("INSERT INTO PAIRS SELECT * FROM merge.pairs")
	if err != nil {
		log.Fatalf("Did not insert into merge.pairs %v", err)

	}

	_, err = db.Exec("detach merge;")
	if err != nil {
		log.Fatalf("Did not detach merge %v", err)
	}

	return nil

}
