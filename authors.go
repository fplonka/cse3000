package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
)

type Author struct {
	AuthorID string `json:"authorid"`
}

func makeAuthorFilter(ids map[int64]bool) func(string) bool {
	return func(line string) bool {
		var author Author
		if err := json.Unmarshal([]byte(line), &author); err != nil {
			fmt.Println("Error parsing JSON:", err)
			return false
		}
		id, err := strconv.Atoi(author.AuthorID)
		if err != nil {
			panic(err)
		}
		return ids[int64(id)]
	}
}

func downloadFilteredAuthors(doParellel bool) {
	baseURL := "https://api.semanticscholar.org/datasets/v1/release/"
	datasetName := "authors"

	links, err := getDownloadLinks(baseURL, datasetName)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Link count:", len(links))

	downloadPath := "/Users/filip/Code/rp/rdataprep/datasets/authors"
	if doParellel {
		downloadPath += "_parallel"
	}
	os.MkdirAll(downloadPath, os.ModePerm)

	ids, err := readIDs("/Users/filip/Code/rp/rdataprep/datasets/authorids.ndjson")
	if err != nil {
		fmt.Println("Error reading IDs:", err)
		return
	}
	filter := makeAuthorFilter(ids)

	fmt.Println("len of ids:", len(ids))

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

type AuthorFull struct {
	AuthorID      int64  `json:"authorid,string"`
	CitationCount int    `json:"citationcount"`
	HIndex        int    `json:"hindex"`
	Name          string `json:"name"`
	PaperCount    int    `json:"papercount"`
}

// processFile reads the input NDJSON file, converts the authorid to int64, and writes to the output file.
func fixAuthorFile(inputFile, outputFile string) error {
	inFile, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("could not open input file: %v", err)
	}
	defer inFile.Close()

	outFile, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("could not create output file: %v", err)
	}
	defer outFile.Close()

	scanner := bufio.NewScanner(inFile)
	writer := bufio.NewWriter(outFile)

	for scanner.Scan() {
		var m map[string]any

		if err := json.Unmarshal(scanner.Bytes(), &m); err != nil {
			return fmt.Errorf("could not unmarshal JSON: %v", err)
		}

		// fmt.Println(m)
		m["authorid"], err = strconv.ParseInt(m["authorid"].(string), 10, 64)
		if err != nil {
			panic(err)
		}

		// Re-marshal to ensure consistent formatting
		updatedJSON, err := json.Marshal(m)
		if err != nil {
			return fmt.Errorf("could not marshal JSON: %v", err)
		}

		if _, err := writer.Write(updatedJSON); err != nil {
			return fmt.Errorf("could not write JSON: %v", err)
		}
		if _, err := writer.WriteString("\n"); err != nil {
			return fmt.Errorf("could not write newline: %v", err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input file: %v", err)
	}

	writer.Flush()
	return nil
}
