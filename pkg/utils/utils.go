package utils

import (
	"bytes"
	"encoding/csv"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sort"
	"Go-Mini-Spark/pkg/types"
	"strings"
)

const longFuncValue = 5

var toInt = func(row types.Row) (int, bool) {
	switch v := row.Value.(type) {
	case int:
		return v, true
	case string:
		if num, err := strconv.Atoi(v); err == nil {
			return num, true
		}
		return 0, false
	case float64:
		return int(v), true
	default:
		return 0, false
	}
}

var FuncRegistry = map[string]interface{}{
    "ToUpper": func(r types.Row) types.Row {
		str, ok := r.Value.(string)
		if !ok {
			log.Printf("ToUpper: expected string but got %T\n", r.Value)
			return types.Row{Key: r.Key, Value: r.Value}
		}
		return types.Row{Key: r.Key, Value: strings.ToUpper(str)}
    },

	"ToLower": func(r types.Row) types.Row {
		str, ok := r.Value.(string)
		if !ok {
			log.Printf("ToLower: expected string but got %T\n", r.Value)
			return types.Row{Key: r.Key, Value: r.Value}
		}
		return types.Row{Key: r.Key, Value: strings.ToLower(str)}
    },

	"CountVowels": func(r types.Row) types.Row {
		str, ok := r.Value.(string)
		if !ok {
			log.Printf("CountVowels: expected string but got %T\n", r.Value)
			return types.Row{Key: r.Key, Value: 0}
		}
		count := 0
		for _, char := range str {
			if strings.ContainsRune("aeiouAEIOU", char) {
				count++
			}
		}
		return types.Row{Key: r.Key, Value: count}
	},

    "IsLong": func(r types.Row) bool {
        str, ok := r.Value.(string)
		if !ok {
			log.Printf("IsLong: expected string but got %T\n", r.Value)
			return false
		}
		return len(str) > longFuncValue
    },

    "SplitWords": func(r types.Row) []types.Row {
        str, ok := r.Value.(string)
		if !ok {
			log.Printf("SplitWords: expected string but got %T\n", r.Value)
			return []types.Row{}
		}
		words := strings.Fields(str)
		rows := make([]types.Row, len(words))
		for i, word := range words {
			rows[i] = types.Row{Key: r.Key, Value: word}
		}
		return rows
    },

	"Max": func(a types.Row, b types.Row) types.Row {
		numA, okA := toInt(a)
		numB, okB := toInt(b)
		if !okA || !okB {
			log.Printf("max: expected int but got %T and %T\n", a.Value, b.Value)
			return types.Row{Key: nil, Value: 0}
		}
		if numA > numB {
			return a
		}
		return b
	},
}

// Map applies a function to each element in a slice and returns a new slice
func Map(data []types.Row, fn func(types.Row) types.Row) []types.Row {
	result := make([]types.Row, len(data))
	for i, item := range data {
		result[i] = fn(item)
	}
	return result
}

// Filter returns a new slice containing only elements that satisfy the predicate
func Filter(data []types.Row, predicate func(types.Row) bool) []types.Row {
	var result []types.Row
	for _, item := range data {
		if predicate(item.Value.(types.Row)) {
			result = append(result, item)
		}
	}
	return result
}

// FlatMap applies a function that returns a slice to each element and flattens the result
func FlatMap(rows []types.Row, fn func(types.Row) []types.Row) []types.Row {
    result := []types.Row{}

    for _, row := range rows {
        out := fn(row)
        result = append(result, out...)
    }

    return result
}

// Join performs an inner join on two collections by key
func Join(left []map[string]interface{}, right []map[string]interface{}, leftKey string, rightKey string) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)

	// Build a map for right collection for O(1) lookup
	rightMap := make(map[interface{}][]map[string]interface{})
	for _, item := range right {
		if key, exists := item[rightKey]; exists {
			rightMap[key] = append(rightMap[key], item)
		}
	}

	// Perform join
	for _, leftItem := range left {
		if leftKeyValue, exists := leftItem[leftKey]; exists {
			if rightMatches, found := rightMap[leftKeyValue]; found {
				for _, rightItem := range rightMatches {
					// Merge left and right items
					merged := make(map[string]interface{})
					for k, v := range leftItem {
						merged[k] = v
					}
					for k, v := range rightItem {
						merged[k] = v
					}
					result = append(result, merged)
				}
			}
		}
	}

	return result
}

func Reduce(data []types.Row, fn func(a types.Row, b types.Row) types.Row) types.Row {
    if len(data) == 0 {
        return types.Row{}   
    }
    acc := data[0]
    for i := 1; i < len(data); i++ {
        acc = fn(acc, data[i])
    }
    return acc
}

// Shuffle redistributes data across partitions based on key hashing
// Returns a map where keys are partition IDs and values are the items for that partition
func Shuffle(data []map[string]interface{}, keyField string, numPartitions int) map[int][]map[string]interface{} {
	partitions := make(map[int][]map[string]interface{})

	for _, item := range data {
		if key, exists := item[keyField]; exists {
			// Simple hash function to determine partition
			partition := HashPartition(fmt.Sprintf("%v", key), numPartitions)
			partitions[partition] = append(partitions[partition], item)
		}
	}

	return partitions
}

// HashPartition determines which partition a key belongs to using hash function
func HashPartition(key string, numPartitions int) int {
	hash := 0
	for _, char := range key {
		hash = ((hash << 5) - hash) + int(char)
	}
	if hash < 0 {
		hash = -hash
	}
	return hash % numPartitions
}

// ReadCSV reads data from a CSV file and returns it as a slice of maps
func ReadCSV(filename string) ([]map[string]interface{}, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("Error opening CSV file %s: %v\n", filename, err)
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		log.Printf("Error reading CSV headers: %v\n", err)
		return nil, err
	}

	var result []map[string]interface{}
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading CSV record: %v\n", err)
			continue
		}

		row := make(map[string]interface{})
		for i, header := range headers {
			if i < len(record) {
				row[header] = record[i]
			}
		}
		result = append(result, row)
	}

	return result, nil
}

// WriteCSV writes data to a CSV file
func WriteCSV(filename string, data []map[string]interface{}) error {
	if len(data) == 0 {
		return fmt.Errorf("no data to write")
	}

	file, err := os.Create(filename)
	if err != nil {
		log.Printf("Error creating CSV file %s: %v\n", filename, err)
		return err
	}
	defer file.Close()

	// Get headers from first row
	var headers []string
	firstRow := data[0]
	for key := range firstRow {
		headers = append(headers, key)
	}
	sort.Strings(headers) // Sort for consistent ordering

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write headers
	writer.Write(headers)

	// Write data rows
	for _, row := range data {
		record := make([]string, len(headers))
		for i, header := range headers {
			record[i] = fmt.Sprintf("%v", row[header])
		}
		writer.Write(record)
	}

	return nil
}

// ReadJSONL reads data from a JSONL (JSON Lines) file and returns it as a slice of maps
func ReadJSONL(filename string) ([]map[string]interface{}, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("Error opening JSONL file %s: %v\n", filename, err)
		return nil, err
	}
	defer file.Close()

	var result []map[string]interface{}
	scanner := csv.NewReader(file)

	for {
		line, err := scanner.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading JSONL line: %v\n", err)
			continue
		}

		if len(line) > 0 {
			var obj map[string]interface{}
			err := json.Unmarshal([]byte(line[0]), &obj)
			if err != nil {
				log.Printf("Error parsing JSON: %v\n", err)
				continue
			}
			result = append(result, obj)
		}
	}

	return result, nil
}

// WriteJSONL writes data to a JSONL (JSON Lines) file
func WriteJSONL(filename string, data []map[string]interface{}) error {
	file, err := os.Create(filename)
	if err != nil {
		log.Printf("Error creating JSONL file %s: %v\n", filename, err)
		return err
	}
	defer file.Close()

	for _, obj := range data {
		jsonBytes, err := json.Marshal(obj)
		if err != nil {
			log.Printf("Error marshaling JSON: %v\n", err)
			continue
		}
		file.WriteString(string(jsonBytes) + "\n")
	}

	return nil
}

// SerializeData serializes data using gob encoding
func SerializeData(data interface{}) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(data)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DeserializeData deserializes data from gob encoding
func DeserializeData(data []byte, result interface{}) error {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	err := decoder.Decode(result)
	if err != nil {
		return err
	}
	return nil
}
