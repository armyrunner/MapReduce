package main

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

func mapSourceFile(m int) string { return fmt.Sprintf("map_%d_source.db", m) }
func mapInputFile(m int) string { return fmt.Sprintf("map_%d_input.db", m) }
func mapOutputFile(m, r int) string { return fmt.Sprintf("map_%d_output_%d.db", m, r) }
func reduceInputFile(r int) string { return fmt.Sprintf("reduce_%d_input.db", r) }
func reduceOutputFile(r int) string { return fmt.Sprintf("reduce_%d_output.db", r) }
func reducePartialFile(r int) string { return fmt.Sprintf("reduce_%d_partial.db", r) }
func reduceTempFile(r int) string { return fmt.Sprintf("reduce_%d_temp.db", r) }
func makeURL(host, file string) string { return fmt.Sprintf("http://%s/data/%s", host, file) }