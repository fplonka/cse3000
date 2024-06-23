package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Embedding struct {
	CorpusID int64  `json:"corpusid"`
	Vector   string `json:"vector"`
}

func makeEmbeddingFilter(ids map[int64]bool) func(string) bool {
	return func(line string) bool {
		var embedding Embedding
		if err := json.Unmarshal([]byte(line), &embedding); err != nil {
			fmt.Println("Error parsing JSON:", err)
			return false
		}
		return ids[embedding.CorpusID]
	}
}

func downloadFilteredEmbeddings(doParellel bool) {
	baseURL := "https://api.semanticscholar.org/datasets/v1/release/"
	datasetName := "embeddings-specter_v2"

	links, err := getDownloadLinks(baseURL, datasetName)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Link count:", len(links))

	downloadPath := "/Users/filip/Code/rp/rdataprep/datasets/embeddings"
	if doParellel {
		downloadPath += "_parallel"
	}
	os.MkdirAll(downloadPath, os.ModePerm)

	ids, err := readIDs("/Users/filip/Code/rp/rdataprep/datasets/papers/all_cs_papers_simple.ndjson")
	if err != nil {
		fmt.Println("Error reading IDs:", err)
		return
	}
	filter := makeAbstractFilter(ids)

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 10) // Buffer size of 10 to limit concurrent go routines

	for _, link := range links {
		if doParellel {
			wg.Add(1)
			go func(link string) {
				semaphore <- struct{}{} // Acquire semaphore by sending an empty struct
				_, err := downloadFile(link, downloadPath, filter)
				if err != nil {
					fmt.Printf("error with link %v: %v\n", link[:100], err)
				}
				<-semaphore // Release semaphore by receiving from the channel
				wg.Done()
			}(link)
		} else {
			_, err := downloadFile(link, downloadPath, filter)
			if err != nil {
				fmt.Printf("error with link %v: %v\n", link[:100], err)
				continue
			}
		}
	}

	wg.Wait()

	validateFiles(downloadPath)
}

// Input structure representing the JSON data with a string vector
type InputRecord struct {
	CorpusID int    `json:"corpusid"`
	Vector   string `json:"vector"`
}

// Output structure representing the JSON data with an integer vector
type OutputRecord struct {
	CorpusID int       `json:"corpusid"`
	Vector   []float64 `json:"vector"`
}

// Function to convert string vector to integer vector
func convertVector(vectorStr string) ([]float64, error) {
	// Remove brackets and split the string by commas
	vectorStr = strings.Trim(vectorStr, "[]")
	vectorStrs := strings.Split(vectorStr, ",")

	// Convert each string to an integer
	var vector []float64
	for _, v := range vectorStrs {
		floatVal, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return nil, err
		}
		vector = append(vector, floatVal)
	}
	return vector, nil
}

// Function to parse the input file and write the output file
func fixEmbedding(inputPath, outputPath string) error {
	// Open input file
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer inputFile.Close()

	// Open output file
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	scanner := bufio.NewScanner(inputFile)
	writer := bufio.NewWriter(outputFile)

	// Process each line
	for scanner.Scan() {
		var inputRecord InputRecord
		err := json.Unmarshal(scanner.Bytes(), &inputRecord)
		if err != nil {
			return err
		}

		// Convert the vector
		intVector, err := convertVector(inputRecord.Vector)
		if err != nil {
			return err
		}

		// Create output record
		outputRecord := OutputRecord{
			CorpusID: inputRecord.CorpusID,
			Vector:   intVector,
		}

		// Write output record as JSON
		outputBytes, err := json.Marshal(outputRecord)
		if err != nil {
			return err
		}
		_, err = writer.WriteString(string(outputBytes) + "\n")
		if err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	writer.Flush()
	return nil
}
