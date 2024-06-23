package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

// Output structure for the papers

// Citation structure for the citation data
type Citation struct {
	CitationID     int64      `json:"citationid"`
	CitingCorpusID int64      `json:"citingcorpusid"`
	CitedCorpusID  int64      `json:"citedcorpusid"`
	IsInfluential  bool       `json:"isinfluential"`
	Contexts       []string   `json:"contexts,omitempty"`
	Intents        [][]string `json:"intents"`
}

// Read IDs from a file of JSON lines and store them in a map
func readIDs(filename string) (map[int64]bool, error) {
	type Output struct {
		CorpusID int64 `json:"corpusid"`
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	ids := make(map[int64]bool)

	for scanner.Scan() {
		var paper Output
		if err := json.Unmarshal(scanner.Bytes(), &paper); err != nil {
			return nil, err
		}
		ids[paper.CorpusID] = true
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return ids, nil
}

// Filter function to check if the citation should be included
func makeCitationFilter(ids map[int64]bool) func(string) bool {
	return func(line string) bool {
		var citation Citation
		if err := json.Unmarshal([]byte(line), &citation); err != nil {
			fmt.Println("Error parsing JSON:", err)
			return false
		}
		return ids[citation.CitingCorpusID] || ids[citation.CitedCorpusID]
	}
}

func downloadFilteredCitations() {
	baseURL := "https://api.semanticscholar.org/datasets/v1/release/"
	datasetName := "citations"

	links, err := getDownloadLinks(baseURL, datasetName)
	if err != nil {
		fmt.Println(err)
		return
	}

	// fmt.Println(len(links))
	// return

	downloadPath := "/Users/filip/Code/rp/rdataprep/datasets/citations"
	os.MkdirAll(downloadPath, os.ModePerm)

	ids, err := readIDs("/Users/filip/Code/rp/rdataprep/datasets/papers/all_cs_papers_simple.ndjson")
	if err != nil {
		fmt.Println("Error reading IDs:", err)
		return
	}
	filterRelevantCitation := makeCitationFilter(ids)

	for _, link := range links {
		_, err := downloadFile(link, downloadPath, filterRelevantCitation)
		if err != nil {
			fmt.Printf("error with link %v: %v\n", link[:100], err)
			continue
		}
	}

	validateFiles(downloadPath)
}

// processFile reads each line from inputFile, removes the 'contexts', and writes the result to outputFile.
func simplifyCitations(inputFilePath, outputFilePath string) error {
	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %v", err)
	}
	defer inputFile.Close()

	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer outputFile.Close()

	scanner := bufio.NewScanner(inputFile)
	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	for scanner.Scan() {
		var citation Citation
		line := scanner.Text()

		if err := json.Unmarshal([]byte(line), &citation); err != nil {
			return fmt.Errorf("failed to unmarshal JSON: %v", err)
		}

		citation.Contexts = nil // Remove contexts

		modifiedJSON, err := json.Marshal(citation)
		if err != nil {
			return fmt.Errorf("failed to marshal JSON: %v", err)
		}

		if _, err := writer.WriteString(string(modifiedJSON) + "\n"); err != nil {
			return fmt.Errorf("failed to write to output file: %v", err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error during scanning: %v", err)
	}

	return nil
}
