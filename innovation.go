package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strings"
)

type TermPair struct {
	Term1 string
	Term2 string
}

type Document struct {
	CorpusID          string
	PublicationDate   string
	ProcessedAbstract []string
}

func computeInnovations() {
	// Open the NDJSON file
	file, err := os.Open("datasets/processed_abstracts.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	// scanner := bufio.NewScanner(file)

	// var documents []Document
	// for scanner.Scan() {
	// 	var record map[string]interface{}
	// 	err := json.Unmarshal(scanner.Bytes(), &record)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}

	// 	// Parse the record into a Document
	// 	doc := Document{
	// 		CorpusID:          int64(record["corpusid"].(float64)),
	// 		PublicationDate:   record["publicationdate"].(string),
	// 		ProcessedAbstract: strings.Split(record["filtered_abstracts_text"].(string), " "),
	// 	}
	// 	if len(doc.ProcessedAbstract) == 0 {
	// 		continue
	// 	}
	// 	documents = append(documents, doc)
	// }

	// if err := scanner.Err(); err != nil {
	// 	log.Fatal(err)
	// }
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	// Parse the records into documents
	var documents []Document
	for _, record := range records[1:] {
		doc := Document{
			CorpusID:          record[0],
			PublicationDate:   record[1],
			ProcessedAbstract: strings.Split(record[2][1:len(record[2])-1], " "), // Assuming the list is saved as a string
		}
		documents = append(documents, doc)
	}

	// Sort documents by publication date
	sort.Slice(documents, func(i, j int) bool {
		return documents[i].PublicationDate < documents[j].PublicationDate
	})

	// documents = documents[0:20000]

	fmt.Println("have", len(documents), "documents")

	termCounts := make(map[string]int)
	pairCounts := make(map[TermPair]int)
	var innovationScores []float64

	// First pass to calculate term counts and pair counts
	for docIdx, doc := range documents {
		if docIdx%1000 == 0 {
			fmt.Println("at:", docIdx)
			fmt.Println("have", len(pairCounts), "pairs")
		}

		significantNewPairCount := 0

		for i, term1 := range doc.ProcessedAbstract {
			termCounts[term1]++
			for j, term2 := range doc.ProcessedAbstract {
				if i > j {
					continue
				}
				pair := TermPair{term1, term2}
				pairCounts[pair]++

				if pairCounts[pair] == 1 { // first time seeing this pair
					pmi := calculatePMI(pair, termCounts, pairCounts)
					if pmi > 0 {
						significantNewPairCount++
					}
				}
			}
		}

		innovationScores = append(innovationScores, float64(significantNewPairCount))
	}

	// // Second pass to calculate innovation scores
	// seenTermPairs := make(map[TermPair]bool)
	// for i, doc := range documents {
	// 	if i%1000 == 0 {
	// 		fmt.Println("at:", i)
	// 	}

	// 	significantNewPairCount := 0

	// 	for i, term1 := range doc.ProcessedAbstract {
	// 		termCounts[term1]++
	// 		for j, term2 := range doc.ProcessedAbstract {
	// 			if i > j {
	// 				continue
	// 			}

	// 			pair := TermPair{term1, term2}

	// 			if seenTermPairs[pair] {
	// 				continue
	// 			}
	// 			seenTermPairs[pair] = true

	// 			pmi := calculatePMI(pair, termCounts, pairCounts)
	// 			if pmi > 0 {
	// 				significantNewPairCount++
	// 			}
	// 		}
	// 	}
	// 	innovationScores = append(innovationScores, float64(significantNewPairCount))
	// }

	// Save innovation scores to CSV
	outputFile, err := os.Create("innovation_scores.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer outputFile.Close()

	writer := csv.NewWriter(outputFile)
	defer writer.Flush()

	writer.Write([]string{"corpusid", "publication_date", "innovation_score"})
	for i, doc := range documents {
		writer.Write([]string{fmt.Sprintf("%v", doc.CorpusID), doc.PublicationDate, fmt.Sprintf("%f", innovationScores[i])})
	}
}

func calculatePMI(pair TermPair, termCounts map[string]int, pairCounts map[TermPair]int) float64 {
	totalPairs := float64(len(pairCounts))
	totalTerms := float64(len(termCounts))
	term1, term2 := pair.Term1, pair.Term2
	pTerm1 := float64(termCounts[term1]) / float64(totalTerms)
	pTerm2 := float64(termCounts[term2]) / float64(totalTerms)
	pPair := float64(pairCounts[pair]) / float64(totalPairs)
	if pTerm1 == 0 || pTerm2 == 0 || pPair == 0 {
		return 0
	}
	val := math.Log2(pPair / (pTerm1 * pTerm2))
	return val
}
