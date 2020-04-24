package main

import (

	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"log"
)


//TestDatabase to see if my database.go works
func TestDatabase(){

	var totalRows int
	mainDB, _ := openDatabase("DBFiles/austen.db")
	mainDB.QueryRow(`SELECT COUNT(key) FROM pairs;`).Scan(&totalRows)
	fmt.Println("The number of rows is: ", totalRows)

	tempdir := filepath.Join("./tmp/", fmt.Sprintf("mapreduce.%d",os.Getpid()))
	log.Printf("Tmp dir is %s\n",tempdir)
	os.MkdirAll(tempdir,0777)
	defer os.RemoveAll(tempdir)

    filePaths, err := splitDatabase("DBFiles/austen.db",tempdir+"/map_%d_source.db",10)
	if err == nil{
		var splitCount, splitTotal int
		for _, f := range filePaths{
			splitCount = 0
			fDB, err := openDatabase(f)
			if err == nil{
				fDB.QueryRow(`SELECT COUNT(key) FROM pairs`).Scan(&splitCount)
				fDB.Close()
				fmt.Printf("%s: %d\n",f,splitCount)
				splitTotal += splitCount
			} else {
				fmt.Printf(" %s: %v",f,err)
				break
			}
		}
		if splitTotal != totalRows{
			fmt.Printf("Split Total (%d) != Source Total (%d)", splitTotal, totalRows)
		}
		
	}else{
		log.Fatalf("%v",err)
	}

	address := "localhost:8080"
	go func() {
		fmt.Println("Started Server")
		http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir("mnt/d/School/cs3410/src/MapReduce"))))
		if err := http.ListenAndServe(address, nil); err != nil {
			fmt.Printf("Error in HTTP server [%s] %v", address, err)
			log.Fatalf("%v",err)
		}
	}()

	for i, file := range filePaths {
		_, fn := filepath.Split(file)
		filePaths[i] = "http://" + address + "/data/" + fn
	}
	
	_, err = mergeDatabase(filePaths, filepath.Join(tempdir, "copyausten.db"), filepath.Join( "temp.db"))
	if err != nil {
		log.Fatalf("Error in mergeDatabase %v",err)
	}

	var copytot int
	copyDB, _ := openDatabase(filepath.Join(tempdir, "copyausten.db"))
	copyDB.QueryRow(`SELECT COUNT(key) FROM pairs;`).Scan(&copytot)
	copyDB.Close()
	fmt.Printf("Total Rows Copied:  %d\n", copytot)

	if copytot != totalRows {
		fmt.Printf("Copy Total (%d) != Source Total (%d)", copytot, totalRows)
	}

}