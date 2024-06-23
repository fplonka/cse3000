package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

type ReleaseData struct {
	Datasets []struct {
		Name string `json:"name"`
	}
}

type DatasetLinks struct {
	Files []string `json:"files"`
}

type LineFilter func(string) bool

// Download the file from the provided URL to the specified path
func downloadFile(downloadUrl string, downloadPath string, filter LineFilter) (string, error) {
	parsedURL, err := url.Parse(downloadUrl)
	if err != nil {
		return "", err
	}

	filename := filepath.Base(parsedURL.Path)
	filename = filename[:len(filename)-3]
	cleanFilename := filepath.Join(downloadPath, filename)

	_, err = os.Stat(cleanFilename)
	if !os.IsNotExist(err) {
		fmt.Println("skipping", cleanFilename)
		return "", nil
	}

	fmt.Println("clean filename:", cleanFilename)

	successful := false
	defer func() {
		if !successful {
			os.Remove(cleanFilename) // Attempt to delete the file on failure
		}
	}()

	resp, err := http.Get(downloadUrl)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to download file: %s", resp.Status)
	}

	gzipReader, err := gzip.NewReader(resp.Body)
	if err != nil {
		return "", err
	}
	defer gzipReader.Close()

	file, err := os.Create(cleanFilename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(gzipReader)
	const maxCapacity int = 1024 * 1024 // your required line length
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)
	for scanner.Scan() {
		line := scanner.Text()
		if filter(line) {
			_, err = file.WriteString(line + "\n")
			if err != nil {
				return "", err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}

	successful = true
	fmt.Printf("Downloaded '%s' to '%s'\n", filename, downloadPath)
	return cleanFilename, nil
}

func downloadFile2(downloadUrl string, downloadPath string, filter LineFilter) (string, error) {
	parsedURL, err := url.Parse(downloadUrl)
	if err != nil {
		return "", err
	}

	filename := filepath.Base(parsedURL.Path)
	filename = filename[:len(filename)-3]
	cleanFilename := filepath.Join(downloadPath, filename)

	_, err = os.Stat(cleanFilename)
	if !os.IsNotExist(err) {
		fmt.Println("skipping", cleanFilename)
		return "", nil
	}

	fmt.Println("clean filename:", cleanFilename)

	resp, err := http.Get(downloadUrl)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to download file: %s", resp.Status)
	}

	gzipReader, err := gzip.NewReader(resp.Body)
	if err != nil {
		return "", err
	}
	defer gzipReader.Close()

	file, err := os.Create(cleanFilename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	lines := make(chan string)
	done := make(chan bool)

	go func() {
		scanner := bufio.NewScanner(gzipReader)
		const maxCapacity int = 1024 * 1024 // your required line length
		buf := make([]byte, maxCapacity)
		scanner.Buffer(buf, maxCapacity)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
		if err := scanner.Err(); err != nil {
			fmt.Printf("Scanner error: %v\n", err)
		}
	}()

	go func() {
		for line := range lines {
			if filter(line) {
				_, err = writer.WriteString(line + "\n")
				if err != nil {
					fmt.Printf("Write error: %v\n", err)
				}
			}
		}
		done <- true
	}()

	<-done

	fmt.Printf("Downloaded '%s' to '%s'\n", filename, downloadPath)
	return cleanFilename, nil
}

// Fetches the download links for a specific dataset

const apiKey = "INSERT_API_KEY_HERE_TO_USE_THIS_CODE"
const release = "2024-04-30"

func getDownloadLinks(baseURL string, datasetName string) ([]string, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", baseURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get releases: %s", resp.Status)
	}

	req, err = http.NewRequest("GET", baseURL+release, nil)
	if err != nil {
		return nil, err
	}

	resp, err = client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get datasets: %s", resp.Status)
	}

	var data ReleaseData
	json.NewDecoder(resp.Body).Decode(&data)
	for _, dataset := range data.Datasets {
		if dataset.Name == datasetName {
			req, err = http.NewRequest("GET", baseURL+release+"/dataset/"+datasetName, nil)
			if err != nil {
				return nil, err
			}
			req.Header.Set("x-api-key", apiKey)

			resp, err = client.Do(req)
			if err != nil {
				return nil, err
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return nil, fmt.Errorf("failed to get download links: %s", resp.Status)
			}

			var links DatasetLinks
			json.NewDecoder(resp.Body).Decode(&links)
			return links.Files, nil
		}
	}

	return nil, fmt.Errorf("dataset %s does not exist", datasetName)
}

// Define a struct that mirrors the JSON data structure
type Paper struct {
	S2FieldsOfStudy []struct {
		Category string `json:"category"`
		Source   string `json:"source"`
	} `json:"s2fieldsofstudy"`
}

// Filter function that checks if "Computer Science" is a category in s2fieldsofstudy
func filterComputerSciencePaper(line string) bool {
	// only take random 1/10 of papers
	if rand.IntN(10) != 0 {
		return false
	}

	var paper Paper
	if err := json.Unmarshal([]byte(line), &paper); err != nil {
		// Handle error if JSON is not properly formatted or expected fields are missing
		return false
	}

	cnt := 0.0
	// Check each entry in the s2fieldsofstudy array
	for _, field := range paper.S2FieldsOfStudy {
		if field.Category == "Computer Science" {
			cnt++
		}
	}

	// require majority vote of category entries to say computer science
	return cnt/float64(len(paper.S2FieldsOfStudy)) > 0.5
}

// validateJSON takes a string and checks if it's valid JSON.
func validateJSON(data string) error {
	var js json.RawMessage
	return json.Unmarshal([]byte(data), &js)
}

// processFile reads each line from the file and checks if it's valid JSON.
func processFile(filePath string, wg *sync.WaitGroup, errChan chan<- error) {
	defer wg.Done()
	file, err := os.Open(filePath)
	if err != nil {
		errChan <- fmt.Errorf("error opening file %s: %v", filePath, err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	const maxCapacity int = 400000 // your required line length
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)
	for scanner.Scan() {
		line := scanner.Text()
		if err := validateJSON(line); err != nil {
			errChan <- fmt.Errorf("invalid JSON in file %s: %v", filePath, err)
			// Do not return here if you want to continue checking other lines
		}
	}
	if err := scanner.Err(); err != nil {
		errChan <- fmt.Errorf("error reading file %s: %v", filePath, err)
	}
	fmt.Println("file valid:", filePath)
}

func validateFiles(path string) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		fmt.Printf("Failed to read directory: %v\n", err)
		return
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(files)) // Buffer to hold errors

	for _, file := range files {
		if !file.IsDir() {
			wg.Add(1)
			go processFile(filepath.Join(path, file.Name()), &wg, errChan)
		}
	}

	// Close the error channel once all goroutines complete.
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Collect errors
	for e := range errChan {
		fmt.Println(e)
	}

}

func downloadFilteredCsPapers() {
	baseURL := "https://api.semanticscholar.org/datasets/v1/release/"
	datasetName := "papers"

	links, err := getDownloadLinks(baseURL, datasetName)
	if err != nil {
		fmt.Println(err)
		return
	}

	downloadPath := "/Users/filip/Code/rp/rdataprep/datasets/papers"
	os.MkdirAll(downloadPath, os.ModePerm)

	for _, link := range links {
		_, err := downloadFile(link, downloadPath, filterComputerSciencePaper)
		if err != nil {
			fmt.Printf("error with link %v: %v\n", link[:100], err)
			continue
		}
	}

	validateFiles(downloadPath)
}

// Input JSON structure
type Input struct {
	CorpusID int64  `json:"corpusid"`
	Title    string `json:"title"`
	Authors  []struct {
		AuthorID string `json:"authorId"`
		Name     string `json:"name"`
	} `json:"authors"`
	Year                     *int    `json:"year"`
	ReferenceCount           int     `json:"referencecount"`
	CitationCount            int     `json:"citationcount"`
	InfluentialCitationCount int     `json:"influentialcitationcount"`
	PublicationDate          *string `json:"publicationdate"`
}

// Output JSON structure
type Output struct {
	CorpusID                 int64   `json:"corpusid"`
	Title                    string  `json:"title"`
	AuthorIDs                []int   `json:"authorIds"`
	Year                     *int    `json:"year"`
	ReferenceCount           int     `json:"referencecount"`
	CitationCount            int     `json:"citationcount"`
	InfluentialCitationCount int     `json:"influentialcitationcount"`
	PublicationDate          *string `json:"publicationdate"`
}

var cnt int

// Processes the file and extracts simplified JSON
func processAndSimplify(inputPath string, outputPath string) error {
	// Open input file
	inFile, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer inFile.Close()

	// Create output file
	outFile, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	scanner := bufio.NewScanner(inFile)
	for scanner.Scan() {
		var input Input
		line := scanner.Text()

		// Unmarshal JSON from the file into the struct
		if err := json.Unmarshal([]byte(line), &input); err != nil {
			return err
		}

		// Create output structure, assuming only the first author is needed
		output := Output{
			CorpusID:                 input.CorpusID,
			Title:                    input.Title,
			Year:                     input.Year,
			ReferenceCount:           input.ReferenceCount,
			CitationCount:            input.CitationCount,
			InfluentialCitationCount: input.InfluentialCitationCount,
			PublicationDate:          input.PublicationDate,
		}

		for _, author := range input.Authors {
			cnt++
			idInt, err := strconv.Atoi(author.AuthorID)
			if err != nil {
				fmt.Println("ERROR ON: ", author)
				continue
			}
			output.AuthorIDs = append(output.AuthorIDs, idInt)
		}

		// Marshal the output struct to JSON
		outputJSON, err := json.Marshal(output)
		if err != nil {
			return err
		}

		// Write the JSON to the output file
		if _, err := outFile.WriteString(string(outputJSON) + "\n"); err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

func removeUnnecessaryFields() {
	inputPath := "/Users/filip/Code/rp/rdataprep/datasets/papers/all_cs_papers.ndjson"
	outputPath := "/Users/filip/Code/rp/rdataprep/datasets/papers/all_cs_papers_simple.ndjson"

	if err := processAndSimplify(inputPath, outputPath); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to process file: %v\n", err)
	}

	fmt.Println("total:", cnt)
}
