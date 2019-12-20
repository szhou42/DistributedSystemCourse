package mapreduce

import (
	"encoding/json"
	"fmt"
	"io"
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
	// Step 1: Read in all the intermediate k/v pairs from the intermediate files(use reduceName to figure out what files to read)
	mergedKVPairs := make([]KeyValue, 0)
	for i := 0; i < nMap; i++ {
		filename := reduceName(jobName, i, reduceTask)
		jsonFile, err := os.Open(filename)
		if err != nil {
			fmt.Println("Unable to open file ", filename)
			return
		}
		jsonReader := io.Reader(jsonFile)
		dec := json.NewDecoder(jsonReader)

		var kvPair KeyValue
		err = dec.Decode(&kvPair)
		for err == nil {
			mergedKVPairs = append(mergedKVPairs, kvPair)
			err = dec.Decode(&kvPair)
		}
		jsonFile.Close()
	}

	// Step 2: Sort all th k/v pairs by key
	sort.Slice(mergedKVPairs, func(i, j int) bool {
		return mergedKVPairs[i].Key < mergedKVPairs[j].Key
	})

	// Step 3: For each unique key, append all values into a slice, then do reduce on it to get a string, write that (key,string) into outFile
	// Done
	jsonFile, err := os.Create(outFile)
	if err != nil {
		fmt.Println("Unable to create file ", outFile)
		return
	}
	jsonWriter := io.Writer(jsonFile)
	enc := json.NewEncoder(jsonWriter)

	kvPairsGroup := make([]string, 0)
	for i, kvPair := range mergedKVPairs {
		kvPairsGroup = append(kvPairsGroup, kvPair.Value)

		if i == len(mergedKVPairs) - 1 || mergedKVPairs[i].Key != mergedKVPairs[i + 1].Key {
			reducedVal := reduceF(kvPair.Key, kvPairsGroup)
			enc.Encode(KeyValue{kvPair.Key, reducedVal})
			if err != nil {
				fmt.Println("Error encoding a k/v pair with reduced value")
				return
			}
			kvPairsGroup = make([]string, 0)
		}
	}
	jsonFile.Close()
}
