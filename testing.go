package main

import (
	"log"
	// "os"
	// "path/filepath"
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

	// _, err = countRows(path)
	// if err != nil {
	// 	log.Fatalf("Failed to countRow")
	// }

	_, err := splitDatabase("austen.db", "output-%d.db", 50)
	if err != nil {
		log.Fatalf("failed to split datapase %v", err)
	}

	// go func() {
	// 	http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))+
	// 	if err := http.ListenAndServe(address, nil); err != nil {
	// 		log.Printf("Error in HTTP server for %s: %v", myaddress, err)
	// 	}
	// }()

}
