package main

import (

	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"log"
	"strings"
	"strconv"
	"unicode"
)


//TestDatabase to see if my database.go works
// func TestDatabase(){

// 	var totalRows int
// 	mainDB, _ := openDatabase("DBFiles/austen.db")
// 	mainDB.QueryRow(`SELECT COUNT(key) FROM pairs;`).Scan(&totalRows)
// 	fmt.Println("The number of rows is: ", totalRows)

// 	// tempdir := filepath.Join("./tmp/", fmt.Sprintf("mapreduce.%d",os.Getpid()))
// 	tempdir := "./tmp"
// 	log.Printf("Tmp dir is %s\n",tempdir)
// 	os.MkdirAll(tempdir,0777)
// 	defer os.RemoveAll(tempdir)

//  	filePaths, err := splitDatabase("DBFiles/austen.db",tempdir+"/map_%d_source.db",10)
// 	if err == nil{
// 		var splitCount, splitTotal int
// 		for _, f := range filePaths{
// 			splitCount = 0
// 			fDB, err := openDatabase(f)
// 			if err == nil{
// 				fDB.QueryRow(`SELECT COUNT(key) FROM pairs`).Scan(&splitCount)
// 				fDB.Close()
// 				fmt.Printf("%s: %d\n",f,splitCount)
// 				splitTotal += splitCount
// 			} else {
// 				fmt.Printf(" %s: %v",f,err)
// 				break
// 			}
// 		}
// 		if splitTotal != totalRows{
// 			fmt.Printf("Split Total (%d) != Source Total (%d)", splitTotal, totalRows)
// 		}
		
// 	}else{
// 		log.Fatalf("%v",err)
// 	}

// 	address := "localhost:8080"
// 	go func() {
// 		fmt.Println("Started Server")
// 		http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))
// 		if err := http.ListenAndServe(address, nil); err != nil {
// 			fmt.Printf("Error in HTTP server [%s] %v", address, err)
// 			log.Fatalf("%v",err)
// 		}
// 	}()

// 	for i, file := range filePaths {
// 		_, fn := filepath.Split(file)
// 		filePaths[i] = "http://" + address + "/data/" + fn
// 	}
	
// 	_, err = mergeDatabase(filePaths, filepath.Join(tempdir, "copyausten.db"), filepath.Join(tempdir,"tempdb.db"))
// 	if err != nil {
// 		log.Fatalf("Error in mergeDatabase %v",err)
// 	}

// 	fmt.Println("Success! Now Counting the copyausten.db")

// 	copytot := 1
// 	copyDB,_:= openDatabase(filepath.Join(tempdir, "copyausten.db"))
// 	fmt.Println("the data base is open")

// 	copyDB.QueryRow(`SELECT COUNT(key) FROM pairs;`).Scan(&copytot)
// 	copyDB.Close()
// 	fmt.Printf("Total Rows Copied:  %d\n", copytot)

// 	if copytot != totalRows {
// 		fmt.Printf("Copy Total (%d) != Source Total (%d)", copytot, totalRows)
// 	}

// }

func testPart2(){
	var mapTasks = 9
	var reduceTasks = 3

    tempdir := filepath.Join(os.TempDir(), fmt.Sprintf("mapreduce.%d", os.Getpid()))
	// tempdir := "./tmp"
	log.Printf("Tmp dir is %s\n",tempdir)
	os.RemoveAll(tempdir)
	os.MkdirAll(tempdir,0777)
	// defer os.RemoveAll(tempdir)
	address := "localhost:8080"

	go func() {
		log.Printf("Starting HTTP File Server: %s\tServing\t%s", address, tempdir)
		http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))
		if err := http.ListenAndServe(address, nil); err != nil {
			log.Printf("Error in HTTP server for %s: %v", address, err)
		}

	}()

	filePaths, err := splitDatabase("DBFiles/austen.db",tempdir+"/map_%d_source.db",10)
	newclient := Client{}

	for i := 0; i < mapTasks; i++{
		var process MapTask
		process.M = mapTasks
		process.R = reduceTasks
		process.N = i
		process.SourceHost = "http://" + address + "/data/" + mapSourceFile(i)
		err := process.Process(tempdir,newclient)
		if err != nil{
			log.Fatal(err)
		}

	}

	for i := 0; i < reduceTasks; i++{
		var process ReduceTask
		process.M = mapTasks
		process.R = reduceTasks
		process.N = i
		process.SourceHosts = make([]string,0,mapTasks*reduceTasks)
		for j := 0; j < mapTasks; j++{
			process.SourceHosts = append(process.SourceHosts, "http://" + address + "/data/" + mapOutputFile(j,i))
		}
		err := process.Process(tempdir,newclient)
		if err != nil{
			log.Fatal(err)
		}

	}
	
	for i, file := range filePaths {
		_, fn := filepath.Split(file)
		filePaths[i] = "http://" + address + "/data/" + fn
	}

	_, err = mergeDatabase(filePaths, filepath.Join(tempdir, "copyausten.db"), filepath.Join(tempdir,"temp.db"))
	if err != nil {
		log.Fatalf("Error in mergeDatabase %v",err)
	}
	
	copyDB, _ := openDatabase(filepath.Join(tempdir, "copyausten.db"))
	counts := copyDB.QueryRow(`SELECT key, value FROM pairs ORDER BY value+0 desc limit 20;`)
	copyDB.Close()
	fmt.Printf("Word Counts: %v", counts)

}

// Client struct for test
type Client struct{}

// Map : Test
func (c Client) Map(key, value string, output chan<- Pair) error {
	defer close(output)
	lst := strings.Fields(value)
	for _, elt := range lst {
		word := strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				return unicode.ToLower(r)
			}
			return -1
		}, elt)
		if len(word) > 0 {
			output <- Pair{Key: word, Value: "1"}
		}
	}
	return nil
}

// Reduce : Test
func (c Client) Reduce(key string, values <-chan string, output chan<- Pair) error {
	defer close(output)
	count := 0
	for v := range values {
		i, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		count += i
	}
	p := Pair{Key: key, Value: strconv.Itoa(count)}
	output <- p
	return nil
}