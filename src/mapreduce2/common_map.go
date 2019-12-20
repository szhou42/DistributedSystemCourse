package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"os"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	// Step 1: Read the inFile in as a string named contents
	contents, err := ioutil.ReadFile(inFile)
	if err != nil {
		fmt.Println("Unable to read inFile ", inFile)
		return
	}

	// Step 2: Pass the inFile filename and its contents into mapF, to get a list of k/v pairs
	kvPairs := mapF(inFile, string(contents))

	// Step 3: For each key, calculate filename it should go into
	filenameToKVPairs := make(map[string][]KeyValue)
	for _, kvPair := range kvPairs {
		rFilename := reduceName(jobName, mapTask, ihash(kvPair.Key) % nReduce)
		if _, ok := filenameToKVPairs[rFilename]; !ok {
			filenameToKVPairs[rFilename] = make([]KeyValue, 0)
		}
		filenameToKVPairs[rFilename] = append(filenameToKVPairs[rFilename], kvPair)
	}
	// fmt.Print("filenameToKVPairs: \n")
	// fmt.Print(filenameToKVPairs)

	// Step 4: write all k/v pairs of the corresponding file
	// For each filename, open it, write all the k/v pairs into it (using filenameToKVPairs to retrieve the right k/v pairs)
	// Done.
	for filename, kvPairs := range filenameToKVPairs {
		jsonFile, err := os.Create(filename)
		if err != nil {
			fmt.Println("Unable to create file ", filename)
			return
		}
		jsonWriter := io.Writer(jsonFile)
		enc := json.NewEncoder(jsonWriter)

		for _, kv := range kvPairs {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Println("Error encoding a k/v pair")
				return
			}
		}
		jsonFile.Close()
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
