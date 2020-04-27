package main


import (
	"log"
	"mapreduce"
	"strconv"
	"strings"
	"unicode"
	"flag"
)

func main() {
	var c Client
	if err := mapreduce.Start(c); err != nil {
		log.Fatalf("%v", err)
	}
}

// Map and Reduce functions for a basic wordcount client

// Client struct
type Client struct{}

// Map struct
func (c Client) Map(key, value string, output chan<- mapreduce.Pair) error {
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
			output <- mapreduce.Pair{Key: word, Value: "1"}
		}
	}
	return nil
}

// Reduce comment
func (c Client) Reduce(key string, values <-chan string, output chan<- mapreduce.Pair) error {
	defer close(output)
	count := 0
	for v := range values {
		i, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		count += i
	}
	p := mapreduce.Pair{Key: key, Value: strconv.Itoa(count)}
	output <- p
	return nil
}

// Start comment
func Start(client Interface) error{
	var trueMaster bool
	flag.BoolVar(&isMaster, "master", false, "Boolean indicated whether this master is true (default false = worker")
	var port string
	flag.StringVar(&port, "port", "8080", "Port to listen on")
	var masterAddress string
	flag.StringVar(&masterAddress, "MasterAddress", getLocalAddress()+":8080", "Address of master, ignore if it is master")
	var split bool
	split = true
	var M, R int
	flag.IntVar(&M, "m", 9, "Number of Map Tasks, ignore if it is the worker")
	flag.IntVar(&R, "r", 3, "Number of Reduce Tasks, ignore if it is the worker")
	var incoming string
	
	flag.StringVar(&incoming, "Incoming file", "austen.sqlite3", "input database file, ignore if it is a worker")
	flage.Parse()
	var err error

	if !trueMaster{
		address := getAddress(port)
		log.Printf("Worker Listening on address: %v", address)
		log.Printf("Master located at address: %v", masterAddress)
		err = worker(address, masterAddress, client)
	} else {
		address := getAddress(port)
		log.Printf("Master Listening on address: %v", address)
		err = master(address, infile, M, R, doSplit)
	}

	log.Printf("Goodbye!")
	return err
	
}