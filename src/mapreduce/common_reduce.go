package mapreduce

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	/// 1. read all content from intermediate file.
	/// 2. decode it and sort.
	/// 3. call reduceF() defined by user then write result to output file.

	var kvs KeyValueSlice
	for i := 0; i < nMap; i++ {
		fname := reduceName(jobName, i, reduceTask)
		f, err := os.Open(fname)
		defer f.Close()
		if err != nil {
			log.Fatalf("open file %s: %v", fname, err)
		}
		dec := json.NewDecoder(f)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err != io.EOF {
					fmt.Printf("decode file %s: %v\n", fname, err)
				}
				break
			}
			kvs = append(kvs, &kv)
		}
	}

	if len(kvs) <= 0 {
		fmt.Println("not found any keyvalue")
		return
	}

	sort.Sort(kvs)

	f, err := os.Create(outFile)
	if err != nil {
		log.Fatalf("create file %s: %v", outFile, err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)

	prevKey := kvs[0].Key
	values := []string{kvs[0].Value}
	for i := 1; i < len(kvs); i++ {
		if kvs[i].Key != prevKey {
			enc.Encode(KeyValue{prevKey, reduceF(prevKey, values)})
			prevKey = kvs[i].Key
			values = nil
		}
		values = append(values, kvs[i].Value)
	}
	enc.Encode(KeyValue{prevKey, reduceF(prevKey, values)})
}

/// KeyValue implement sort.Interface
type KeyValueSlice []*KeyValue

func (p KeyValueSlice) Len() int           { return len(p) }
func (p KeyValueSlice) Less(i, j int) bool { return p[i].Key < p[j].Key }
func (p KeyValueSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
