package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	apiURL = "https://api.mae.com.ar/MarketData/v1/mercado/cotizaciones/forex"
)

// ForexData represents the structure of the new API response
type ForexData struct {
	Fecha                string  `json:"fecha"`
	Ticker               string  `json:"ticker"`
	Descripcion          string  `json:"descripcion"`
	TipoEmision          string  `json:"tipoEmision"`
	Segmento             string  `json:"segmento"`
	CodigoSegmento       string  `json:"codigoSegmento"`
	Plazo                string  `json:"plazo"`
	CodigoPlazo          string  `json:"codigoPlazo"`
	Moneda               string  `json:"moneda"`
	FechaLiquidacion     string  `json:"fechaLiquidacion"`
	VolumenAcumulado     int     `json:"volumenAcumulado"`
	MontoAcumulado       float64 `json:"montoAcumulado"`
	PrecioUltimo         float64 `json:"precioUltimo"`
	UltimaTasa           float64 `json:"ultimaTasa"`
	PrecioCierreAnterior float64 `json:"precioCierreAnterior"`
	PrecioMinimo         float64 `json:"precioMinimo"`
	PrecioMaximo         float64 `json:"precioMaximo"`
	OpenInterest         int     `json:"openInterest"`
	PrecioCierre         float64 `json:"precioCierre"`
	Variacion            float64 `json:"variacion"`
}

// deriveCurrencyOut extracts the short currency code from the ticker.
// Tickers ending in "$T" get it stripped: "USB$T" -> "USB", "MB$T" -> "MB"
// Other tickers are kept as-is: "USMEP" -> "USMEP", "UBMEP" -> "UBMEP"
func deriveCurrencyOut(ticker string) string {
	return strings.TrimSuffix(ticker, "$T")
}

// deriveCurrencyIn maps the moneda field to the old-style currency_in code.
// "T" (pesos transferencia) -> "ART"
// Other values are returned as-is as fallback.
func deriveCurrencyIn(moneda string) string {
	switch moneda {
	case "T":
		return "ART"
	default:
		return moneda
	}
}

// deriveRueda maps the segmento field to the old-style rueda code.
// "Minorista" -> "CAM2", "Mayorista" -> "CAM1"
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

// buildInstrumento builds the instrumento string in the old format:
// "CURRENCY_OUT / CURRENCY_IN PLAZO" e.g. "USB / ART 000"
func buildInstrumento(currencyOut, currencyIn, plazo string) string {
	return fmt.Sprintf("%s / %s %s", currencyOut, currencyIn, plazo)
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

func fetchForexData() []ForexData {
	apiKey := os.Getenv("MAE_API_KEY")
	if apiKey == "" {
		log.Fatal("MAE_API_KEY environment variable not set")
	}

	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		log.Printf("Failed to create request: %v", err)
		return nil
	}
	req.Header.Set("x-api-key", apiKey)

	client := &http.Client{Timeout: 30 * time.Second}
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

	// The new API returns a flat JSON array: [{ ... }, { ... }]
	var data []ForexData
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		log.Printf("Failed to decode JSON: %v", err)
		return nil
	}

	if len(data) == 0 {
		fmt.Println("No data received from API.")
		return nil
	}

	fmt.Printf("Received %d records from API.\n", len(data))
	return data
}

func saveToDatabase(data []ForexData) {
	if len(data) == 0 {
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
	err = conn.QueryRow(context.Background(), "SELECT MAX(date) FROM public.forex").Scan(&lastDate)
	if err != nil && err != pgx.ErrNoRows {
		log.Printf("Failed to query last date: %v\n", err)
	}

	// Prepare insert statement
	// Existing columns: date, rueda, instrumento, currency_out, currency_in, settle, settle_date, monto, cotizacion, hora
	// New columns: descripcion, tipo_emision, codigo_segmento, codigo_plazo, moneda, monto_acumulado,
	//              precio_ultimo, ultima_tasa, precio_cierre_anterior, precio_minimo, precio_maximo,
	//              open_interest, variacion
	query := `
		INSERT INTO public.forex (
			date, rueda, instrumento, currency_out, currency_in, settle, settle_date, monto, cotizacion, hora,
			descripcion, tipo_emision, codigo_segmento, codigo_plazo, moneda, monto_acumulado,
			precio_ultimo, ultima_tasa, precio_cierre_anterior, precio_minimo, precio_maximo,
			open_interest, variacion
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
		          $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)`

	_, err = conn.Prepare(context.Background(), "insert_forex", query)
	if err != nil {
		log.Printf("Failed to prepare statement: %v\n", err)
		return
	}

	successfulInserts := 0
	skipped := 0
	for _, d := range data {
		// Parse fecha - format: "2024-11-15T00:00:00"
		fecha, err := time.Parse("2006-01-02T15:04:05", d.Fecha)
		if err != nil {
			log.Printf("Invalid fecha '%s': %v", d.Fecha, err)
			continue
		}

		// Skip records already in the database
		if !lastDate.IsZero() && !fecha.After(lastDate) {
			skipped++
			continue
		}

		// Derive currency codes, rueda and instrumento
		currencyOut := deriveCurrencyOut(d.Ticker)
		currencyIn := deriveCurrencyIn(d.Moneda)
		rueda := deriveRueda(d.Segmento)
		instrumento := buildInstrumento(currencyOut, currencyIn, d.Plazo)

		// Parse settle (plazo) to integer
		var settleVal *int
		if d.Plazo != "" {
			s, err := strconv.Atoi(d.Plazo)
			if err == nil {
				settleVal = &s
			}
		}

		// Parse fecha_liquidacion (nullable - "0001-01-01T00:00:00" means no date)
		var settleDateVal *time.Time
		if d.FechaLiquidacion != "" && d.FechaLiquidacion != "0001-01-01T00:00:00" {
			t, err := time.Parse("2006-01-02T15:04:05", d.FechaLiquidacion)
			if err == nil {
				settleDateVal = &t
			}
		}

		_, err = conn.Exec(context.Background(), "insert_forex",
			// Existing columns
			fecha,                // date
			rueda,                // rueda (CAM1/CAM2)
			instrumento,          // instrumento (e.g. "USB / ART 000")
			currencyOut,          // currency_out (parsed from descripcion)
			currencyIn,           // currency_in (parsed from descripcion)
			settleVal,            // settle (plazo as int)
			settleDateVal,        // settle_date (fecha_liquidacion)
			d.VolumenAcumulado,   // monto (API: volumenAcumulado)
			d.PrecioCierre,       // cotizacion
			nil,                  // hora (not available in new API)
			// New columns
			d.Descripcion,          // descripcion
			d.TipoEmision,          // tipo_emision
			d.CodigoSegmento,       // codigo_segmento
			d.CodigoPlazo,          // codigo_plazo
			d.Moneda,               // moneda
			d.MontoAcumulado,       // monto_acumulado
			d.PrecioUltimo,         // precio_ultimo
			d.UltimaTasa,           // ultima_tasa
			d.PrecioCierreAnterior, // precio_cierre_anterior
			d.PrecioMinimo,         // precio_minimo
			d.PrecioMaximo,         // precio_maximo
			d.OpenInterest,         // open_interest
			d.Variacion,            // variacion
		)
		if err != nil {
			log.Printf("Failed to insert row (ticker=%s, fecha=%s): %v\n", d.Ticker, d.Fecha, err)
		} else {
			successfulInserts++
		}
	}

	if skipped > 0 {
		fmt.Printf("Skipped %d records already in database.\n", skipped)
	}
	fmt.Printf("Inserted %d rows into forex table.\n", successfulInserts)
}
