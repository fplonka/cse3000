package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type Abstract struct {
	CorpusID int64  `json:"corpusid"`
	Abstract string `json:"abstract"`
}

func makeAbstractFilter(ids map[int64]bool) func(string) bool {
	return func(line string) bool {
		var abstract Abstract
		if err := json.Unmarshal([]byte(line), &abstract); err != nil {
			fmt.Println("Error parsing JSON:", err)
			return false
		}
		return ids[abstract.CorpusID]
	}
}

func downloadFilteredAbstracts(doParellel bool) {
	baseURL := "https://api.semanticscholar.org/datasets/v1/release/"
	datasetName := "abstracts"

	links, err := getDownloadLinks(baseURL, datasetName)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Link count:", len(links))

	downloadPath := "/Users/filip/Code/rp/rdataprep/datasets/abstracts"
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
