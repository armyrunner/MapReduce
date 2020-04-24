package main

import (
	"log"
	"fmt"
	"net/http"
	// "io"
	"os"
	"path/filepath"
)

func main() {

	// path, err := createDatabase("test.db")

	// if err != nil {
	// 	log.Fatalf("did not create databaser %v", err)
	// }

	// _, err = openDatabase("test.db")

	// if err != nil {
	// 	log.Fatalf("did not open database %v", err)
	// }

	_, err := splitDatabase("austen.db", "output-%d.db", 50)
	if err != nil {
		log.Fatalf("failed to split datapase %v", err)
	}

	var address = ":8080"
	tempdir := filepath.Join("./tmp/", fmt.Sprintf("mapReduce.%d", os.Getpid()))
	log.Printf("Tmp dir is %s\n", tempdir)


	go func() {
		log.Println("Server is running")
		http.Handle("/data/", http.StripPrefix("/data/", http.FileServer(http.Dir("Desktop/cs3410/src/mapReduce"))))
		if err := http.ListenAndServe(address, nil); err != nil {
			log.Printf("Error in HTTP server for %s: %v", address, err)
		}
	}()

	// curl http://localhost:8080/data/austen.db

}
