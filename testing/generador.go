// gen_test_files.go
// Genera archivos de prueba para /sortfile, /wordcount, /grep, /compress y /hashfile
// Por defecto crea:
//   - numbers.txt  : 5,000,000 líneas (~50 MB) con enteros aleatorios [1, 1,000,000]
//   - largefile.txt: 1,000,000 líneas; cada línea es el texto Lorem repetido 10 veces (UTF-8)
//   - testfile.bin : 10 MB de bytes aleatorios
//
// Uso (por defecto):
//
//	go run gen_test_files.go
//
// Uso (personalizado):
//
//	go run gen_test_files.go -numbers numbers.txt -lines 5000000 -min 1 -max 1000000 \
//	  -text largefile.txt -text-iters 1000000 -text-repeats 10 \
//	  -bin testfile.bin -bin-bytes 10485760
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	// Banderas
	// numbersPath := flag.String("numbers", "numbers.txt", "Ruta del archivo de números (uno por línea)")
	lines := flag.Int("lines", 10_000_000, "Cantidad de líneas para el archivo de números")
	minNum := flag.Int("min", 1, "Valor mínimo (inclusive) para los números aleatorios")
	maxNum := flag.Int("max", 1_000_000, "Valor máximo (exclusive) para los números aleatorios")

	textPath := flag.String("text", "largefile2.txt", "Ruta del archivo de texto grande (UTF-8)")
	textIters := flag.Int("text-iters", 100_000, "Veces que se escribe una línea")
	textRepeats := flag.Int("text-repeats", 2, "Repeticiones del texto base por línea")

	flag.Parse()

	// Validaciones básicas
	if err := validateRanges(*minNum, *maxNum, *lines, *textIters, *textRepeats, 0); err != nil {
		fatal(err)
	}

	// // 1) numbers.txt
	// fmt.Printf("Generando %s con %d líneas de enteros aleatorios [%d, %d)...\n",
	// 	*numbersPath, *lines, *minNum, *maxNum)
	// if err := genNumbersFile(*numbersPath, *lines, *minNum, *maxNum); err != nil {
	// 	fatal(fmt.Errorf("falló generar %s: %w", *numbersPath, err))
	// }
	// fmt.Println("OK.")

	// 2) largefile.txt
	fmt.Printf("Generando %s con %d líneas (cada una repite %d veces el texto base)...\n",
		*textPath, *textIters, *textRepeats)
	if err := genLargeTextFile(*textPath, *textIters, *textRepeats); err != nil {
		fatal(fmt.Errorf("falló generar %s: %w", *textPath, err))
	}
	fmt.Println("OK.")
}

func validateRanges(minNum, maxNum, lines, textIters, textRepeats int, binBytes int64) error {
	if maxNum <= minNum {
		return errors.New("max debe ser mayor que min")
	}
	if lines <= 0 {
		return errors.New("lines debe ser > 0")
	}
	if textIters <= 0 || textRepeats <= 0 {
		return errors.New("text-iters y text-repeats deben ser > 0")
	}
	return nil
}

func genNumbersFile(path string, lines, min, max int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Buffer grande para minimizar syscalls
	const bufSize = 1 << 20 // 1 MiB
	w := bufio.NewWriterSize(f, bufSize)
	defer w.Flush()

	// RNG seed
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Usamos strconv.AppendInt para evitar allocations intermedias
	buf := make([]byte, 0, 16) // suficiente para números y '\n'
	for i := 0; i < lines; i++ {
		n := r.Intn(max-min) + min
		buf = strconv.AppendInt(buf[:0], int64(n), 10)
		buf = append(buf, '\n')
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}
	return w.Flush()
}

func genLargeTextFile(path string, iterations, repeats int) error {
	const lorem = "Lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	const bufSize = 1 << 20 // 1 MiB
	w := bufio.NewWriterSize(f, bufSize)
	defer w.Flush()

	// Prearma la línea (texto base repetido 'repeats' veces)
	line := strings.Repeat(lorem, repeats)

	for i := 0; i < iterations; i++ {
		if _, err := w.WriteString(line); err != nil {
			return err
		}
		if err := w.WriteByte('\n'); err != nil {
			return err
		}
	}
	return w.Flush()
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "ERROR:", err)
	os.Exit(1)
}
