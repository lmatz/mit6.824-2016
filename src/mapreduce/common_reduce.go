package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"io"
)
// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	var collections map[string] []string = make(map[string] []string)
	var interFiles    [] *os.File = make([] *os.File, nMap)
	var decoders      [] *json.Decoder = make([] *json.Decoder, nMap)
	var err error

	for i:=0 ; i<nMap ; i++ {
		interFileName := reduceName(jobName,i,reduceTaskNumber)
		interFiles[i], err = os.Open(interFileName)
		if err != nil {
			log.Fatal(err)
		}
		defer interFiles[i].Close()

		decoders[i] = json.NewDecoder(interFiles[i])

		for {
			var kv KeyValue
			err := decoders[i].Decode(&kv)
			if err == io.EOF {
				break
			} else if err!=nil {
				log.Fatal(err)
			}
			collections[kv.Key] = append(collections[kv.Key],kv.Value)
		}
	}

	var mergeFileName string = mergeName(jobName,reduceTaskNumber) 
	mergeFile,err := os.Create(mergeFileName)
	if  err!=nil {
		log.Fatal(err)
	}
	defer mergeFile.Close()
	encoder := json.NewEncoder(mergeFile)

	for key, value := range collections {
		err := encoder.Encode(KeyValue{Key:key, Value:reduceF(key,value)})
		if err!=nil {
			log.Fatal(err)
		}
	}

}
