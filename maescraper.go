package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/jackc/pgx/v5"
)

const (
	apiURL = "https://www.mae.com.ar/mercados/forex/api/LeerForexPrecios"
)

// ForexData represents the structure of the API response data
type ForexData struct {
	Fecha      string  `json:"Fecha"`
	Titulo     string  `json:"Titulo"`
	Rueda      string  `json:"Rueda"`
	Monto      string  `json:"Monto"`
	Cotizacion float64 `json:"Cotizacion"`
	Hora       string  `json:"Hora"`
}

func main() {
	fmt.Println("---------------------------------------------")
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	fmt.Printf("Iniciando maeScraper a las: %s\n", currentTime)

	forexData := fetchForexData()
	if forexData != nil {
		saveToDatabase(forexData)
	} else {
		fmt.Println("Data fetching failed.")
	}

	currentTime = time.Now().Format("2006-01-02 15:04:05")
	fmt.Printf("Proceso finalizado a las: %s\n", currentTime)
	fmt.Println("---------------------------------------------")
}

func fetchForexData() *dataframe.DataFrame {
	// Fetch data from API
	resp, err := http.Get(apiURL)
	if err != nil || resp.StatusCode != 200 {
		fmt.Println("Failed to fetch data from API.")
		return nil
	}
	defer resp.Body.Close()

	// Parse JSON response
	var result struct {
		Data []ForexData `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Println("Failed to decode JSON.")
		return nil
	}
	if len(result.Data) == 0 {
		fmt.Println("No data received from API.")
		return nil
	}

	// Create DataFrame from API data
	df := dataframe.LoadStructs(result.Data)

	// Process 'Fecha' (date) as strings
	var dates []string
	for _, fecha := range df.Col("Fecha").Records() {
		t, err := time.Parse("060102", fecha) // 'yymmdd' format
		if err != nil {
			dates = append(dates, "") // Handle invalid dates
		} else {
			dates = append(dates, t.Format("2006-01-02"))
		}
	}

	// Extract currency details from 'Titulo' using regex
	pattern := regexp.MustCompile(`(?P<currency_out>[A-Z]+)\s*/\s*(?P<currency_in>[A-Z]+)\s*(?P<settle>\d+)\s*(?P<settle_date>\d{6})`)
	var currencyOut, currencyIn, settle, settleDate []interface{} // Use interface{} to allow nil
	for _, titulo := range df.Col("Titulo").Records() {
		matches := pattern.FindStringSubmatch(titulo)
		if len(matches) == 5 {
			currencyOut = append(currencyOut, matches[1])
			currencyIn = append(currencyIn, matches[2])
			settle = append(settle, matches[3])         // Keep as string for now, convert to int later
			settleDate = append(settleDate, matches[4]) // Keep as string for now, convert to date later
		} else {
			currencyOut = append(currencyOut, nil)
			currencyIn = append(currencyIn, nil)
			settle = append(settle, nil)
			settleDate = append(settleDate, nil)
		}
	}

	// Process 'Monto' (convert to float)
	var montos []float64
	for _, monto := range df.Col("Monto").Records() {
		m := strings.ReplaceAll(monto, ",", "")
		f, err := strconv.ParseFloat(m, 64)
		if err != nil {
			montos = append(montos, 0.0)
		} else {
			montos = append(montos, f)
		}
	}

	// Process 'Hora' (convert to time)
	var horas []string
	for _, hora := range df.Col("Hora").Records() {
		t, err := time.Parse("15:04:05", hora) // 'HH:MM:SS' format
		if err != nil {
			horas = append(horas, "00:00:00")
		} else {
			horas = append(horas, t.Format("15:04:05"))
		}
	}

	// Create a new DataFrame with processed data using series.New
	newDF := dataframe.New(
		series.New(dates, series.String, "date"),
		series.New(df.Col("Rueda").Records(), series.String, "rueda"),
		series.New(df.Col("Titulo").Records(), series.String, "instrumento"),
		series.New(currencyOut, series.String, "currency_out"),
		series.New(currencyIn, series.String, "currency_in"),
		series.New(settle, series.String, "settle"),
		series.New(settleDate, series.String, "settle_date"),
		series.New(montos, series.Float, "monto"),
		series.New(df.Col("Cotizacion").Float(), series.Float, "cotizacion"),
		series.New(horas, series.String, "hora"),
	)

	return &newDF
}

func saveToDatabase(df *dataframe.DataFrame) {
	if df == nil || df.Nrow() == 0 {
		fmt.Println("No data to save.")
		return
	}

	// Load environment variables
	dbUser := os.Getenv("POSTGRES_USER")
	dbPassword := os.Getenv("POSTGRES_PASSWORD")
	dbHost := os.Getenv("POSTGRES_HOST")
	dbPort := os.Getenv("POSTGRES_PORT")
	if dbPort == "" {
		dbPort = "5432"
	}
	dbName := os.Getenv("POSTGRES_DB")

	// Connect to PostgreSQL
	connStr := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s", dbUser, dbPassword, dbHost, dbPort, dbName)
	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer conn.Close(context.Background())

	// Check last inserted date
	var lastDate time.Time
	err = conn.QueryRow(context.Background(), "SELECT MAX(date) FROM public.forex3").Scan(&lastDate)
	if err != nil && err != pgx.ErrNoRows {
		log.Printf("Failed to query last date: %v\n", err)
	}

	// Filter new data and prepare rows
	var rowsToInsert [][]any
	successfulInserts := 0
	for i := 0; i < df.Nrow(); i++ {
		dateStr := df.Col("date").Val(i).(string)
		date, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			continue // Skip invalid dates
		}

		// Handle settle (integer, nullable)
		var settleVal interface{}
		settleStr, settleOk := df.Col("settle").Val(i).(string)
		if settleOk && settleStr != "" {
			settleInt, err := strconv.Atoi(settleStr)
			if err != nil {
				log.Printf("Invalid settle value '%s' at row %d, using NULL", settleStr, i)
				settleVal = nil
			} else {
				settleVal = settleInt
			}
		} else {
			settleVal = nil
		}

		// Handle settle_date (date, nullable)
		var settleDateVal interface{}
		settleDateStr, settleDateOk := df.Col("settle_date").Val(i).(string)
		if settleDateOk && settleDateStr != "" {
			settleDateTime, err := time.Parse("060102", settleDateStr) // Parse 'yymmdd' to time.Time
			if err != nil {
				log.Printf("Invalid settle_date value '%s' at row %d, using NULL", settleDateStr, i)
				settleDateVal = nil
			} else {
				settleDateVal = settleDateTime
			}
		} else {
			settleDateVal = nil
		}

		if lastDate.IsZero() || date.After(lastDate) {
			rowsToInsert = append(rowsToInsert, []any{
				date,
				df.Col("rueda").Val(i),
				df.Col("instrumento").Val(i),
				df.Col("currency_out").Val(i),
				df.Col("currency_in").Val(i),
				settleVal,     // int or nil
				settleDateVal, // time.Time or nil
				df.Col("monto").Val(i),
				df.Col("cotizacion").Val(i),
				df.Col("hora").Val(i),
			})
		}
	}

	if len(rowsToInsert) == 0 {
		fmt.Println("No new data to insert.")
		return
	}

	// Insert data into database
	query := `
		INSERT INTO public.forex3 (date, rueda, instrumento, currency_out, currency_in, settle, settle_date, monto, cotizacion, hora)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`
	_, err = conn.Prepare(context.Background(), "insert_forex", query)
	if err != nil {
		log.Printf("Failed to prepare statement: %v\n", err)
		return
	}

	for _, row := range rowsToInsert {
		_, err := conn.Exec(context.Background(), "insert_forex", row...)
		if err != nil {
			log.Printf("Failed to insert row: %v\n", err)
		} else {
			successfulInserts++
		}
	}

	fmt.Printf("Inserted %d rows into forex3 table.\n", successfulInserts)
}
