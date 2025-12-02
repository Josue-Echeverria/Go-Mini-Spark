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
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

// Configuración de archivos de prueba
const (
	// Archivo de números
	numbersPath = "numbers.txt"
	lines       = 10_000_000 // Cantidad de líneas para el archivo de números
	minNum      = 1          // Valor mínimo (inclusive) para los números aleatorios
	maxNum      = 1_000_000  // Valor máximo (exclusive) para los números aleatorios

	// Archivo de texto grande
	textPath    = "largefile2.txt" // Ruta del archivo de texto grande (UTF-8)
	textIters   = 100_000          // Veces que se escribe una línea
	textRepeats = 2                // Repeticiones del texto base por línea

	// Archivo binario
	binPath  = "testfile.bin"
	binBytes = 10 * 1024 * 1024 // 10 MB de bytes aleatorios

	// Archivo CSV de ventas
	salesCSVPath = "sales.csv"
	salesRecords = 50_000 // Cantidad de registros de ventas

	// Archivo CSV de catálogo
	catalogCSVPath = "catalog.csv"
	catalogRecords = 5_000 // Cantidad de productos en el catálogo
)

func main() {
	// Validaciones básicas
	if err := validateRanges(minNum, maxNum, lines, textIters, textRepeats, binBytes); err != nil {
		fatal(err)
	}

	// // 1) numbers.txt
	// fmt.Printf("Generando %s con %d líneas de enteros aleatorios [%d, %d)...\n",
	// 	numbersPath, lines, minNum, maxNum)
	// if err := genNumbersFile(numbersPath, lines, minNum, maxNum); err != nil {
	// 	fatal(fmt.Errorf("falló generar %s: %w", numbersPath, err))
	// }
	// fmt.Println("OK.")

	// // 2) largefile.txt
	// fmt.Printf("Generando %s con %d líneas (cada una repite %d veces el texto base)...\n",
	// 	textPath, textIters, textRepeats)
	// if err := genLargeTextFile(textPath, textIters, textRepeats); err != nil {
	// 	fatal(fmt.Errorf("falló generar %s: %w", textPath, err))
	// }
	// fmt.Println("OK.")

	// 3) sales_data.csv
	fmt.Printf("Generando %s con %d registros de ventas...\n",
		salesCSVPath, salesRecords)
	if err := genSalesCSV(salesCSVPath, salesRecords); err != nil {
		fatal(fmt.Errorf("falló generar %s: %w", salesCSVPath, err))
	}
	fmt.Println("OK.")

	// 4) catalog_data.csv
	fmt.Printf("Generando %s con %d productos...\n",
		catalogCSVPath, catalogRecords)
	if err := genCatalogCSV(catalogCSVPath, catalogRecords); err != nil {
		fatal(fmt.Errorf("falló generar %s: %w", catalogCSVPath, err))
	}
	fmt.Println("OK.")
}

func validateRanges(minNum, maxNum, lines, textIters, textRepeats int, binBytes int) error {
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

// genSalesCSV genera un archivo CSV con datos de ventas realistas
func genSalesCSV(path string, records int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	const bufSize = 1 << 20 // 1 MiB
	w := bufio.NewWriterSize(f, bufSize)
	defer w.Flush()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Header CSV
	if _, err := w.WriteString("id,product_id,customer_id,quantity,unit_price,total_amount,sale_date,region\n"); err != nil {
		return err
	}

	// Datos de ejemplo
	regions := []string{"Norte", "Sur", "Este", "Oeste", "Centro"}
	baseDate := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	for i := 0; i < records; i++ {
		saleID := i + 1
		productID := r.Intn(catalogRecords) + 1        // ProductID entre 1-5000
		customerID := r.Intn(50000) + 1       // CustomerID entre 1-50000
		quantity := r.Intn(10) + 1            // Cantidad entre 1-10
		unitPrice := r.Float64()*99.99 + 0.01 // Precio entre 0.01-100.00
		totalAmount := float64(quantity) * unitPrice

		// Fecha aleatoria en 2023
		daysOffset := r.Intn(365)
		saleDate := baseDate.AddDate(0, 0, daysOffset)
		region := regions[r.Intn(len(regions))]

		line := fmt.Sprintf("%d,%d,%d,%d,%.2f,%.2f,%s,%s\n",
			saleID, productID, customerID, quantity, unitPrice, totalAmount,
			saleDate.Format("2006-01-02"), region)

		if _, err := w.WriteString(line); err != nil {
			return err
		}
	}
	return w.Flush()
}

// genCatalogCSV genera un archivo CSV con datos del catálogo de productos
func genCatalogCSV(path string, records int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	const bufSize = 1 << 20 // 1 MiB
	w := bufio.NewWriterSize(f, bufSize)
	defer w.Flush()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Header CSV
	if _, err := w.WriteString("id,name,category,brand,price,stock_quantity,description,supplier_id\n"); err != nil {
		return err
	}

	// Datos de ejemplo
	categories := []string{"Electrónicos", "Ropa", "Hogar", "Deportes", "Libros", "Juguetes", "Belleza", "Automóvil", "Alimentos", "Muebles", "Herramientas", "Jardín", "Mascotas", "Música", "Fotografía"}
	brands := []string{"TechCorp", "StyleMax", "HomeComfort", "SportPro", "ReadMore", "FunTime", "GlowUp", "AutoBest", "FreshFood", "FurniturePro", "ToolMaster", "GreenGarden", "PetCare", "SoundWave", "PhotoPro", "InnovateTech", "FashionHub", "SmartHome", "PowerSports", "ClassicReads"}
	prefixes := []string{"Super", "Ultra", "Mega", "Pro", "Premium", "Deluxe", "Smart", "Eco", "Quantum", "Cyber", "Hyper", "Turbo", "Nano", "Crystal", "Golden", "Silver", "Royal", "Elite", "Mighty", "Rapid"}
	suffixes := []string{"Plus", "Max", "Elite", "Advanced", "Premium", "Standard", "Lite", "Classic", "Pro", "Ultra", "X", "2.0", "3D", "Wireless", "Smart", "Digital", "Compact", "Deluxe", "Express", "Infinity"}

	for i := 0; i < records; i++ {
		productID := i + 1

		// Generar nombre de producto
		prefix := prefixes[r.Intn(len(prefixes))]
		suffix := suffixes[r.Intn(len(suffixes))]
		productName := fmt.Sprintf("%s Producto %s", prefix, suffix)

		category := categories[r.Intn(len(categories))]
		brand := brands[r.Intn(len(brands))]
		price := r.Float64()*999.99 + 0.01 // Precio entre 0.01-1000.00
		stock := r.Intn(1000)              // Stock entre 0-999
		description := fmt.Sprintf("Descripción detallada del %s de la marca %s", productName, brand)
		supplierID := r.Intn(500) + 1 // SupplierID entre 1-500

		// Escapar comillas en la descripción
		description = strings.ReplaceAll(description, "\"", "\"\"")

		line := fmt.Sprintf("%d,\"%s\",%s,%s,%.2f,%d,\"%s\",%d\n",
			productID, productName, category, brand, price, stock, description, supplierID)

		if _, err := w.WriteString(line); err != nil {
			return err
		}
	}
	return w.Flush()
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "ERROR:", err)
	os.Exit(1)
}
