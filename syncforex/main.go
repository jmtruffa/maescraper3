package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	fmt.Println("---------------------------------------------")
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	fmt.Printf("Iniciando syncForex a las: %s\n", currentTime)

	// Connect to local PostgreSQL (source: forex3) - POSTGRES_*
	localConn := connectDB(
		os.Getenv("POSTGRES_USER"),
		os.Getenv("POSTGRES_PASSWORD"),
		os.Getenv("POSTGRES_HOST"),
		envOrDefault("POSTGRES_PORT", "5432"),
		os.Getenv("POSTGRES_DB"),
		"local",
	)
	defer localConn.Close(context.Background())

	// Connect to Google Cloud PostgreSQL (destination: forex) - GCLOUD_POSTGRES_*
	cloudConn := connectDB(
		os.Getenv("GCLOUD_POSTGRES_USER"),
		os.Getenv("GCLOUD_POSTGRES_PASSWORD"),
		os.Getenv("GCLOUD_POSTGRES_HOST"),
		envOrDefault("GCLOUD_POSTGRES_PORT", "15432"),
		os.Getenv("GCLOUD_POSTGRES_DB"),
		"gcloud",
	)
	defer cloudConn.Close(context.Background())

	// Get last date in cloud forex
	var lastDate time.Time
	err := cloudConn.QueryRow(context.Background(), "SELECT COALESCE(MAX(date), '1900-01-01') FROM public.forex").Scan(&lastDate)
	if err != nil {
		log.Fatalf("Failed to query last date from cloud: %v", err)
	}
	fmt.Printf("Last date in cloud forex: %s\n", lastDate.Format("2006-01-02"))

	// Read new rows from local forex
	query := `
		SELECT date, rueda, instrumento, currency_out, currency_in, settle, settle_date,
		       monto, cotizacion, hora, descripcion, tipo_emision, codigo_segmento,
		       codigo_plazo, moneda, monto_acumulado, precio_ultimo, ultima_tasa,
		       precio_cierre_anterior, precio_minimo, precio_maximo, open_interest, variacion
		FROM public.forex
		WHERE date > $1
		ORDER BY date`

	rows, err := localConn.Query(context.Background(), query, lastDate)
	if err != nil {
		log.Fatalf("Failed to query local forex3: %v", err)
	}
	defer rows.Close()

	// Insert into cloud forex
	insertQuery := `
		INSERT INTO public.forex (
			date, rueda, instrumento, currency_out, currency_in, settle, settle_date,
			monto, cotizacion, hora, descripcion, tipo_emision, codigo_segmento,
			codigo_plazo, moneda, monto_acumulado, precio_ultimo, ultima_tasa,
			precio_cierre_anterior, precio_minimo, precio_maximo, open_interest, variacion
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)`

	_, err = cloudConn.Prepare(context.Background(), "insert_forex_cloud", insertQuery)
	if err != nil {
		log.Fatalf("Failed to prepare insert statement: %v", err)
	}

	inserted := 0
	for rows.Next() {
		var (
			date                                              time.Time
			rueda, instrumento, currencyOut, currencyIn       *string
			settle                                            *int
			settleDate                                        *time.Time
			monto, cotizacion                                 *float64
			hora                                              *string
			descripcion, tipoEmision, codigoSegmento          *string
			codigoPlazo, moneda                               *string
			montoAcumulado, precioUltimo, ultimaTasa           *float64
			precioCierreAnterior, precioMinimo, precioMaximo   *float64
			openInterest                                      *int
			variacion                                         *float64
		)

		err := rows.Scan(
			&date, &rueda, &instrumento, &currencyOut, &currencyIn,
			&settle, &settleDate, &monto, &cotizacion, &hora,
			&descripcion, &tipoEmision, &codigoSegmento, &codigoPlazo, &moneda,
			&montoAcumulado, &precioUltimo, &ultimaTasa,
			&precioCierreAnterior, &precioMinimo, &precioMaximo,
			&openInterest, &variacion,
		)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		_, err = cloudConn.Exec(context.Background(), "insert_forex_cloud",
			date, rueda, instrumento, currencyOut, currencyIn,
			settle, settleDate, monto, cotizacion, hora,
			descripcion, tipoEmision, codigoSegmento, codigoPlazo, moneda,
			montoAcumulado, precioUltimo, ultimaTasa,
			precioCierreAnterior, precioMinimo, precioMaximo,
			openInterest, variacion,
		)
		if err != nil {
			log.Printf("Failed to insert row (date=%s): %v", date.Format("2006-01-02"), err)
		} else {
			inserted++
		}
	}

	if rows.Err() != nil {
		log.Printf("Row iteration error: %v", rows.Err())
	}

	fmt.Printf("Synced %d rows from local forex to cloud forex.\n", inserted)
	currentTime = time.Now().Format("2006-01-02 15:04:05")
	fmt.Printf("Proceso finalizado a las: %s\n", currentTime)
	fmt.Println("---------------------------------------------")
}

func connectDB(user, password, host, port, dbName, label string) *pgx.Conn {
	connStr := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s", user, password, host, port, dbName)
	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		log.Fatalf("Unable to connect to %s database: %v", label, err)
	}
	fmt.Printf("Connected to %s database.\n", label)
	return conn
}

func envOrDefault(key, defaultVal string) string {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	return val
}
