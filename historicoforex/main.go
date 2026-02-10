package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	apiBaseURL = "https://api.marketdata.mae.com.ar/api/mercado/titulo/historicoforex"
)

// HistoricoResponse represents a date group in the API response
type HistoricoResponse struct {
	Fecha   string        `json:"fecha"`
	Volumen float64       `json:"volumen"`
	Details []ForexDetail `json:"details"`
}

// ForexDetail represents a single record within a date group
type ForexDetail struct {
	Fecha            string  `json:"fecha"`
	Ticker           string  `json:"ticker"`
	Descripcion      string  `json:"descripcion"`
	Moneda           string  `json:"moneda"`
	Plazo            string  `json:"plazo"`
	CodigoPlazo      string  `json:"codigoPlazo"`
	Segmento         string  `json:"segmento"`
	CodigoSegmento   string  `json:"codigoSegmento"`
	Volumen          float64 `json:"volumen"`
	Monto            float64 `json:"monto"`
	Minimo           float64 `json:"minimo"`
	Maximo           float64 `json:"maximo"`
	Ultimo           float64 `json:"ultimo"`
	Variacion        float64 `json:"variacion"`
	TipoEmision      string  `json:"tipoEmision"`
	PrecioCierre     float64 `json:"precioCierre"`
	FechaLiquidacion string  `json:"fechaLiquidacion"`
	UltimaTasa       float64 `json:"ultimaTasa"`
	CierreAnterior   float64 `json:"cierreAnterior"`
	OpenInterest     int     `json:"openInterest"`
}

// deriveCurrencyOut extracts the short currency code from the ticker.
func deriveCurrencyOut(ticker string) string {
	return strings.TrimSuffix(ticker, "$T")
}

// deriveCurrencyIn maps the moneda field to the old-style currency_in code.
func deriveCurrencyIn(moneda string) string {
	if moneda == "T" {
		return "ART"
	}
	return moneda
}

// deriveRueda maps the segmento field to the old-style rueda code.
func deriveRueda(segmento string) string {
	switch segmento {
	case "Minorista":
		return "CAM2"
	case "Mayorista":
		return "CAM1"
	default:
		return segmento
	}
}

// buildInstrumento builds the instrumento string in the old format.
func buildInstrumento(currencyOut, currencyIn, plazo string) string {
	return fmt.Sprintf("%s / %s %s", currencyOut, currencyIn, plazo)
}

func main() {
	fmt.Println("---------------------------------------------")
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	fmt.Printf("Iniciando historicoForex a las: %s\n", currentTime)

	// Connect to PostgreSQL
	conn := connectDB()
	defer conn.Close(context.Background())

	// Get last date in forex table
	today := time.Now().Truncate(24 * time.Hour)
	lastDate := getLastDate(conn)

	fmt.Printf("Last date in DB: %s\n", lastDate.Format("2006-01-02"))
	fmt.Printf("Today: %s\n", today.Format("2006-01-02"))

	if !lastDate.IsZero() && !lastDate.Before(today) {
		fmt.Println("Database is up to date. Nothing to do.")
		fmt.Println("---------------------------------------------")
		return
	}

	// Calculate date range: lastDate + 1 day to today
	var fechaDesde time.Time
	if lastDate.IsZero() {
		fechaDesde = today
	} else {
		fechaDesde = lastDate.AddDate(0, 0, 1)
	}
	fechaHasta := today

	fmt.Printf("Fetching data from %s to %s\n", fechaDesde.Format("2006-01-02"), fechaHasta.Format("2006-01-02"))

	// Fetch data from API
	data := fetchHistoricoForex(fechaDesde, fechaHasta)
	if data == nil {
		fmt.Println("Data fetching failed.")
		fmt.Println("---------------------------------------------")
		return
	}

	// Count total details
	totalDetails := 0
	for _, day := range data {
		totalDetails += len(day.Details)
	}
	fmt.Printf("Received %d days with %d total records.\n", len(data), totalDetails)

	if totalDetails == 0 {
		fmt.Println("No new data to insert.")
		fmt.Println("---------------------------------------------")
		return
	}

	// Insert into database
	inserted := insertData(conn, data)

	currentTime = time.Now().Format("2006-01-02 15:04:05")
	fmt.Printf("Inserted %d rows into forex table.\n", inserted)
	fmt.Printf("Proceso finalizado a las: %s\n", currentTime)
	fmt.Println("---------------------------------------------")
}

func connectDB() *pgx.Conn {
	dbUser := os.Getenv("POSTGRES_USER")
	dbPassword := os.Getenv("POSTGRES_PASSWORD")
	dbHost := os.Getenv("POSTGRES_HOST")
	dbPort := os.Getenv("POSTGRES_PORT")
	if dbPort == "" {
		dbPort = "5432"
	}
	dbName := os.Getenv("POSTGRES_DB")

	connStr := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s", dbUser, dbPassword, dbHost, dbPort, dbName)
	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	fmt.Println("Connected to database.")
	return conn
}

func getLastDate(conn *pgx.Conn) time.Time {
	var lastDate time.Time
	err := conn.QueryRow(context.Background(), "SELECT COALESCE(MAX(date), '1900-01-01') FROM public.forex").Scan(&lastDate)
	if err != nil {
		log.Printf("Failed to query last date: %v\n", err)
		return time.Time{}
	}
	return lastDate
}

func fetchHistoricoForex(desde, hasta time.Time) []HistoricoResponse {
	oTitulo := fmt.Sprintf(`{"fechaDesde":"%s","fechaHasta":"%s"}`,
		desde.Format("2006-01-02"),
		hasta.Format("2006-01-02"),
	)

	apiURL := fmt.Sprintf("%s?oTitulo=%s", apiBaseURL, url.QueryEscape(oTitulo))

	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		log.Printf("Failed to create request: %v", err)
		return nil
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; MAEScraper/1.0)")

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Failed to fetch data from API: %v", err)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("API returned status %d: %s", resp.StatusCode, string(body))
		return nil
	}

	var data []HistoricoResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		log.Printf("Failed to decode JSON: %v", err)
		return nil
	}

	return data
}

func insertData(conn *pgx.Conn, data []HistoricoResponse) int {
	query := `
		INSERT INTO public.forex (
			date, rueda, instrumento, currency_out, currency_in, settle, settle_date,
			monto, cotizacion, hora,
			descripcion, tipo_emision, codigo_segmento, codigo_plazo, moneda,
			precio_ultimo, ultima_tasa, precio_cierre_anterior,
			precio_minimo, precio_maximo, open_interest, variacion, monto_acumulado
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
		          $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)`

	_, err := conn.Prepare(context.Background(), "insert_forex", query)
	if err != nil {
		log.Printf("Failed to prepare statement: %v\n", err)
		return 0
	}

	inserted := 0
	for _, day := range data {
		for _, d := range day.Details {
			fecha, err := time.Parse("2006-01-02T15:04:05", d.Fecha)
			if err != nil {
				log.Printf("Invalid fecha '%s': %v", d.Fecha, err)
				continue
			}

			currencyOut := deriveCurrencyOut(d.Ticker)
			currencyIn := deriveCurrencyIn(d.Moneda)
			rueda := deriveRueda(d.Segmento)
			instrumento := buildInstrumento(currencyOut, currencyIn, d.Plazo)

			var settleVal *int
			if d.Plazo != "" {
				s, err := strconv.Atoi(d.Plazo)
				if err == nil {
					settleVal = &s
				}
			}

			var settleDateVal *time.Time
			if d.FechaLiquidacion != "" && d.FechaLiquidacion != "0001-01-01T00:00:00" {
				t, err := time.Parse("2006-01-02T15:04:05", d.FechaLiquidacion)
				if err == nil {
					settleDateVal = &t
				}
			}

			_, err = conn.Exec(context.Background(), "insert_forex",
				fecha,            // date
				rueda,            // rueda (CAM1/CAM2)
				instrumento,      // instrumento (e.g. "USB / ART 000")
				currencyOut,      // currency_out
				currencyIn,       // currency_in
				settleVal,        // settle (plazo as int)
				settleDateVal,    // settle_date
				d.Volumen,        // monto (API: volumen)
				d.PrecioCierre,   // cotizacion (API: precioCierre)
				nil,              // hora (not available)
				d.Descripcion,    // descripcion
				d.TipoEmision,    // tipo_emision
				d.CodigoSegmento, // codigo_segmento
				d.CodigoPlazo,    // codigo_plazo
				d.Moneda,         // moneda
				d.Ultimo,         // precio_ultimo
				d.UltimaTasa,     // ultima_tasa
				d.CierreAnterior, // precio_cierre_anterior
				d.Minimo,         // precio_minimo
				d.Maximo,         // precio_maximo
				d.OpenInterest,   // open_interest
				d.Variacion,      // variacion
				d.Monto,          // monto_acumulado (API: monto)
			)
			if err != nil {
				log.Printf("Failed to insert row (ticker=%s, fecha=%s): %v\n", d.Ticker, d.Fecha, err)
			} else {
				inserted++
			}
		}
	}

	return inserted
}
