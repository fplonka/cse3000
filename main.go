package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

func simplifyDataset(inputFilePath, outputFilePath string, keys ...string) error {
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
	const maxCapacity int = 1024 * 1024
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	for scanner.Scan() {
		line := scanner.Text()

		var m map[string]interface{}

		if err := json.Unmarshal([]byte(line), &m); err != nil {
			return fmt.Errorf("failed to unmarshal JSON: %v", err)
		}

		for _, key := range keys {
			delete(m, key)
		}

		modifiedJSON, err := json.Marshal(m)
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

func main() {
	computeInnovations()
	// downloadFilteredAuthors(true)
	// downloadFilteredEmbeddings(true)
	// prefix := "/Users/filip/Code/rp/rdataprep/datasets/"
	// validateFiles("/Users/filip/Code/rp/rdataprep/datasets/citations")
	// err := simplifyDataset(prefix+"authors_parallel/all_authors.ndjson", prefix+"authors_parallel/all_authors_simple.ndjson",
	// "externalids", "url", "aliases", "affiliations", "homepage")
	// if err != nil {
	// panic(err)
	// }
	// downloadFilteredCitations()
	// err := fixEmbedding("/Users/filip/Code/rp/rdataprep/datasets/embeddings_parallel/all_embeddings_simple.ndjson",
	// 	"/Users/filip/Code/rp/rdataprep/datasets/embeddings_parallel/all_embeddings_simple2.ndjson")
	// if err != nil {
	// 	panic(err)
	// }

	// prefix := "/Users/filip/Code/rp/rdataprep/datasets/"

	// err := fixAuthorFile("/Users/filip/Code/rp/rdataprep/datasets/authors_parallel/all_authors_simple.ndjson",
	// 	"/Users/filip/Code/rp/rdataprep/datasets/authors_parallel/all_authors_simple2.ndjson")
	// if err != nil {
	// 	panic(err)
	// }
}
