package main


import(
	"database/sql"
	"hash/fnv"
	"path/filepath"
	"log"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
)

// MapTask struct
type MapTask struct {
	M, R int // total number of map and reduce tasks
	N int // map task number, 0-based
	SourceHost string // address of host with map input file
}
// ReduceTask Struct
type ReduceTask struct {
	M, R int // total number of map and reduce tasks
	N int // reduce task number, 0-based
	SourceHosts []string // addresses of map workers
}
// Pair Struct
type Pair struct {
	Key string
	Value string
}
// Interface Struct
type Interface interface {
	Map(key, value string, output chan<- Pair) error
	Reduce(key string, values <-chan string, output chan<- Pair) error
}

func mapSourceFile(m int) string 		{ return fmt.Sprintf("map_%d_source.db", m) }
func mapInputFile(m int) string 		{ return fmt.Sprintf("map_%d_input.db", m) }
func mapOutputFile(m, r int) string 	{ return fmt.Sprintf("map_%d_output_%d.db", m, r) }
func reduceInputFile(r int) string 		{ return fmt.Sprintf("reduce_%d_input.db", r) }
func reduceOutputFile(r int) string 	{ return fmt.Sprintf("reduce_%d_output.db", r) }
func reducePartialFile(r int) string 	{ return fmt.Sprintf("reduce_%d_partial.db", r) }
func reduceTempFile(r int) string 		{ return fmt.Sprintf("reduce_%d_temp.db", r) }
func makeURL(host, file string) string 	{ return fmt.Sprintf("http://%s/data/%s", host, file) }

// Process : Map Process
func (task *MapTask) Process(tempdir string, client Interface) error{

	newpath := filepath.Join(tempdir,mapInputFile(task.N))

	log.Printf("Process started on Maptask: %v", task.N)

	err := download(task.SourceHost, newpath)
	if err != nil{
		return fmt.Errorf("Map Process: %v", err)
	}

	inputdb, err := openDatabase(newpath)
	if err != nil {
		return fmt.Errorf("Map Process: %v", err)
	}
	defer inputdb.Close()

	var outputDBs []*sql.DB
	for i := 0; i < task.R; i++{
		output := mapOutputFile(task.N, i)
		outputDB, err := createDatabase(tempdir + "/" + output)
		if err != nil{
			return fmt.Errorf("Map Process: %v", err)
		}
		outputDBs = append(outputDBs, outputDB)
		defer outputDB.Close()
	}

	var statements []*sql.Stmt
	for i := 0; i < task.R; i++{
		stmt, err := outputDBs[i].Prepare("INSERT INTO pairs (key,value) VALUES (?,?);")
		if err != nil{
			return fmt.Errorf("Map Process Prepare: %v", err)
		}
		statements = append(statements, stmt)
		defer stmt.Close()
	}

	rows, err := inputdb.Query("SELECT key, value FROM pairs;")
	if err != nil{
		inputdb.Close()
		return fmt.Errorf("Map Process: DB Select: %v", err)
	}
	defer rows.Close()

	processed := 0
	total := 0

	for rows.Next(){
		output := make(chan Pair, 100)
		finished := make(chan error)
		var key string
		var value string
		if err := rows.Scan(&key, &value); err != nil{
			inputdb.Close()
			return fmt.Errorf("Map Process: scanner: %v", err)
		}
		processed++
		go func(){
			for pair := range output{
				total++
				hash := fnv.New32()
				hash.Write([]byte(pair.Key))
				r := int(hash.Sum32()) % task.R
				if _, err := statements[r].Exec(pair.Key, pair.Value); err != nil{
					err = fmt.Errorf("Map Process: Exec: %v", err)
				}
			}
			finished <- nil
		}()
		err = client.Map(key, value, output)
		if err != nil{
			return fmt.Errorf("Map Process: Mapping: %v", err)
		}
		err = <-finished
	}
	log.Printf("Maptask.Process processed %v pairs and generated %v pairs", processed, total)
	return err
}

// Process : Reduce Process
func (task *ReduceTask) Process(tempdir string, client Interface) error{
	log.Printf("Reduce Task Running on %v...", task.N)
	input := reduceInputFile(task.N)
	inputdb, err := mergeDatabase(task.SourceHosts, tempdir+"/"+input, tempdir)
	if err != nil{
		return fmt.Errorf("Reduce Process: %v", err)
	}
	defer inputdb.Close()

	output := reduceOutputFile(task.N)
	outputdb, err := createDatabase(tempdir + "/" + output)
	if err != nil{
		return fmt.Errorf("Reduce Process: %v", err)
	}
	defer outputdb.Close()

	rows, err := inputdb.Query("SELECT key, value FROM pairs ORDER BY key, value;")
	if err != nil{
		inputdb.Close()
		return fmt.Errorf("Reduce Process: Query: %v", err)
	}
	var previous string
	previous = ""
	var inputstr chan string
	var outputpair chan Pair
	var finished chan struct{}
	processed := 0
	total := 0
	stmt, err := outputdb.Prepare("INSERT INTO pairs (key,value) VALUES (?,?);")
	if err != nil{
		return fmt.Errorf("Reduce Process: Prepare: %v", err)
	}
	defer stmt.Close()
	for rows.Next(){
		var key string
		var value string
		if err := rows.Scan(&key, &value); err != nil{
			inputdb.Close()
			return fmt.Errorf("Reduce Process: Scanner: %v", err)
		}
		processed++
		if key != previous && inputstr != nil{
			close(inputstr)
			inputstr = nil
			<-finished
			<-finished
		}
		if key != previous{
			previous = key
			inputstr = make(chan string, 100)
			outputpair = make(chan Pair, 10)
			finished = make(chan struct{})
			go func(output chan Pair){
				for pair := range output{
					total ++
					if _, err := stmt.Exec(pair.Key, pair.Value); err != nil{
						err = fmt.Errorf("Reduce Process: Exec: %v", err)
					}
				}
				finished <- struct{}{}
			}(outputpair)
			go func(inputstr chan string, outputpair chan Pair){
				err = client.Reduce(key, inputstr, outputpair)
				if err != nil{
					err = fmt.Errorf("Reduce Process: REDUCE: %v", err)
					log.Fatalf("%v", err)
				}
				finished <- struct{}{}
			}(inputstr, outputpair)
		}
		inputstr <- value
	}
	close(inputstr)
	inputstr = nil
	<-finished
	<-finished
	log.Printf("ReduceTask.Process processed %v pairs, and generated %v total pairs", processed, total)
	return err
}
