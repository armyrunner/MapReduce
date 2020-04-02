package mapreduce

import (
	"database/sql"
	"fmt"
	"log"
	"os"
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

	db.Close()
	return db, nil
}

func createDatabase(filename string) (*sql.DB, error) {

	os.Remove(filename)

	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		log.Fatalf("opening db: %v", err)
	}

	create_stmt := `
	CREATE TABLE IF NOT EXISTS pairs(
		key text primary key,
		value text)`

	if _, err := db.Exec(create_stmt); err != nil {
		log.Fatalf("running create database: %v", err)
	}

	return db, nil
}

func countRows(db *sql.DB) (int, error) {

	rows, err := db.Query("SELECT count(1) FROM pairs;")

	if err != nil {
		fmt.Println("failed to count rows")
		return -1, err
	}

	defer rows.Close()

	count := 0

	for rows.Next() {
		err := rows.Scan(&count)
		if err != nil {
			fmt.Println("Failed to Scan through rows")
			return -1, err

		}
		log.Println("number of rows: ", count)
	}

	return count, nil

}

func splitDatabase(source, outputPattern string, m int) ([]string, error) {

	// open the database
	db, err := openDatabase(source)
	if err != nil {
		log.Fatalf("split database: %v", err)
	}

	defer db.Close()

	//check the number
	count, err := countRows(db)

	if count < m {
		log.Fatalf("count %v", count, " is less then m %v", m, err)
	}

	fmt.Println("Number of Rows is:", count)

	//create a list slice of all the outputdatabase
	outputdb := make([]*sql.DB, 0)

	//create a list of slices of pathnames
	pathnames := make([]string, 0)
	for i := 0; i < m; i++ {

		//get outpout of pathnames
		pathnamestring := fmt.Sprintf(outputPattern, i)

		//createing new database
		outputdatabase, err := createDatabase(pathnamestring)

		if err != nil {
			log.Fatalf("Fail to split database")
		}

		outputdb = append(outputdb, outputdatabase)

		pathnames = append(pathnames, pathnamestring)
	}

	// query the database for the key/ value

	rows, err := db.Query("SELECT key, value FROM pairs;")
	if err != nil {
		log.Fatalf("Did not find key/value in pairs")

	}

	defer rows.Close()

	// initialize the index value
	index := 0
	var key string
	var value string

	// looping through all the rows
	for rows.Next() {
		db := outputdb[index]
		rows.Scan(&key, &value)

		_, err := db.Exec(`INSERT INTO pairs (key,value) values(?,?)`, key, value)
		index++
		defer db.Close()

		if err != nil {
			log.Fatalf("Did not insert key/value into pairs")
		}
	}

	return pathnames, nil
}

// need to make function mergeDatabase

// need to make function download

// need to make function gatherinto
