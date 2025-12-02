package utils

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sort"
	"Go-Mini-Spark/pkg/types"
	"strings"
	"hash/fnv"
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

// Shuffle redistribuye filas entre particiones basado en el hash de la key.
func Shuffle(rows []types.Row, numPartitions int) map[int][]types.Row {
    partitions := make(map[int][]types.Row)

    for _, row := range rows {
        if row.Key == nil {
            continue
        }

        // Convertir key a string para hashing
        keyStr := fmt.Sprintf("%v", row.Key)

        partition := HashPartition(keyStr, numPartitions)

        partitions[partition] = append(partitions[partition], row)
    }

    return partitions
}


func HashPartition(key string, numPartitions int) int {
    h := fnv.New32a()
    h.Write([]byte(key))
    return int(h.Sum32()) % numPartitions
}

func Join(leftRows []types.Row, rightRows []types.Row) []types.Row {
    // 1. Construimos un índice por clave para el lado derecho
    rightIndex := make(map[interface{}][]types.Row)
    for _, r := range rightRows {
        rightIndex[r.Key] = append(rightIndex[r.Key], r)
    }

    var result []types.Row

    // 2. Recorremos el lado izquierdo y buscamos coincidencias
    for _, left := range leftRows {
        matches := rightIndex[left.Key]
        if len(matches) == 0 {
            continue // no hay match
        }

        leftMap, ok := left.Value.(map[string]interface{})
        if !ok {
            continue
        }

        for _, right := range matches {
            rightMap, ok := right.Value.(map[string]interface{})
            if !ok {
                continue
            }

            // 3. Crear value combinado (shallow merge)
            merged := make(map[string]interface{})

            // copiar left
            for k, v := range leftMap {
                merged[k] = v
            }
            // copiar right (si hay colisiones, right sobrescribe)
            for k, v := range rightMap {
                merged[k] = v
            }

            // 4. Añadir resultado
            result = append(result, types.Row{
                Key:   left.Key,  // clave del join
                Value: merged,
            })
        }
    }

    return result
}


// WriteCSV writes a slice of Row into a CSV file.
// Assumes: Row.Key = ID, Row.Value = map[string]interface{}
func WriteCSV(filename string, data []types.Row) error {
    if len(data) == 0 {
        return fmt.Errorf("no data to write")
    }

    file, err := os.Create(filename)
    if err != nil {
        log.Printf("Error creating CSV file %s: %v\n", filename, err)
        return err
    }
    defer file.Close()

    writer := csv.NewWriter(file)
    defer writer.Flush()

    // Extract headers from first row
    first := data[0]

    valueMap, ok := first.Value.(map[string]interface{})
    if !ok {
        return fmt.Errorf("WriteCSV: row.Value is not a map[string]interface{}")
    }

    // Build header list
    headers := []string{"id"} // "id" always first

    for k := range valueMap {
        if k != "id" { // avoid duplicating if it exists inside the map
            headers = append(headers, k)
        }
    }

    sort.Strings(headers[1:]) // sort all except the first

    // Write headers
    if err := writer.Write(headers); err != nil {
        return err
    }

    // Write rows
    for _, row := range data {
        valueMap, ok := row.Value.(map[string]interface{})
        if !ok {
            continue
        }

        record := make([]string, len(headers))

        for i, h := range headers {
            if h == "id" {
                record[i] = fmt.Sprintf("%v", row.Key)
            } else {
                record[i] = fmt.Sprintf("%v", valueMap[h])
            }
        }

        if err := writer.Write(record); err != nil {
            return err
        }
    }

    return nil
}

func FlattenPartition(joinedPartitions map[int][]types.Row) []types.Row {
	flat := []types.Row{}
	for partID, rows := range joinedPartitions {
		log.Printf("Adding %d rows from partition %d", len(rows), partID)
		flat = append(flat, rows...)
	}
	return flat
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

func FindIndex(slice []string, target string) int {
    for i, v := range slice {
        if v == target {
            return i
        }
    }
    return -1
}
