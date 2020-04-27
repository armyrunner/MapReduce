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

	createStmt := `CREATE TABLE IF NOT EXISTS pairs(
		key text primary key,
		value text)`

 	_, err  = db.Exec(createStmt)
	if err != nil {
		db.Close()
		os.Remove(filename)
		return nil, err
	}

	// fmt.Println("Success in creating table pairs")
	
	return db, nil
}

func splitDatabase(source, outputPattern string, m int) ([]string, error) {

	// open the database
	fmt.Println("opening database")
	maindb, err := openDatabase(source)

	//create a list slice of all the outputdatabase
	allSplits := make([]*sql.DB,0)

	//create a list of slices of pathnames
	outputs := make([]string,0)

	if err != nil {
		log.Fatalf("split database: %v", err)
	}

	defer maindb.Close()

	for i := 0; i < m; i++ { 
	
		//get outpout of pathnames
		pathnamestring := fmt.Sprintf(outputPattern,i)

		//createing new database
		splitDB, err := createDatabase(pathnamestring)

		defer splitDB.Close()

		if err != nil {

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

	// // looping through all the rows
	for rows.Next() {

		var key string
		var value string

		err := rows.Scan(&key, &value)

		db := allSplits[index]

		_, err = db.Exec(`INSERT INTO pairs (key,value) values (?, ?)`, key, value)
		if err != nil {
			return nil, err
		}

		index++
		keys++

		if index >= m{
			index = 0
		}
	}

	//check err if anything went wrong
	if err := rows.Err(); err != nil{
		return nil, err
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

	newdb, err := createDatabase(path)
	// fmt.Println(tempdb)
	if err != nil {
		fmt.Println("1 err: ",err)
		return nil, err
	}

	var splitCount = 0
	tempdb, err := createDatabase(path)
	// fmt.Println(tempdb)
	if err != nil {
		fmt.Println("2 err: ",err)
		return nil, err
	}

	
	for _, url := range urls {
		err = download(url, temp)
		if err != nil {
			// tempdb.Close()
			// os.Remove(temp)
			fmt.Println("3 err: ",err)
			return nil, err
		}
		
		newdb.QueryRow(`SELECT COUNT(key) FROM pairs`).Scan(&splitCount)
		fmt.Printf("The Number is: %d\n",splitCount)
		
		err = gatherinto(newdb, temp)
		if err != nil {

			fmt.Println("4 err: ",err)
			// tempdb.Close()
			return nil, err
		}
	}

	newdb.QueryRow(`SELECT COUNT(key) FROM pairs`).Scan(&splitCount)
	fmt.Printf("The Number is: %d\n",splitCount)

	tempdb.Close()
	return tempdb, nil
}

// need to make function download
func download(URL, path string) (error) {

	output, err := os.Create(path)
	if err !=nil{
		return err
	}
	
	defer output.Close()
	
	res, err := http.Get(URL)
	if err != nil {
		return err
	}

	defer res.Body.Close()

	_, err = io.Copy(output, res.Body)
	if err != nil {
		return err
	}
	fmt.Println("finished downloading",output )

	return nil
}

// need to make function gatherinto

func gatherinto(db *sql.DB, path string) error {

	fmt.Println("started to gatherinto ",path)
	querydatabase := fmt.Sprintf("attach '%s' as merge;", path)

	_, err := db.Exec(querydatabase)
	if err != nil {
		fmt.Printf(" Err 1 GatherInto: %v",err)
	}

	_, err = db.Exec(`INSERT INTO pairs SELECT * FROM merge.pairs;`)
	if err != nil {
		fmt.Printf(" Err 2 GatherInto: %v",err)

	}

	_, err = db.Exec("detach merge;")
	if err != nil {
		fmt.Printf(" Err 3 GatherInto: %v",err)
	}

	return nil

}


