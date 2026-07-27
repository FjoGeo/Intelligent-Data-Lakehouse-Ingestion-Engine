package main

import (
    "context"
    "encoding/csv"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net/http"
    "os"
    "path/filepath"
    "strings"
    "time"

    "github.com/minio/minio-go/v7"
    "github.com/minio/minio-go/v7/pkg/credentials"
)

const (
    defaultMinioEndpoint        = "localhost:9000"
    defaultMinioAccessKeyID     = "adminuser"
    defaultMinioSecretAccessKey = "adminpassword"
    defaultMinioBucketName      = "finance-data"

    defaultYahooSymbol        = "AAPL"
    defaultAlphaVantageSymbol = "MSFT"
    defaultAlphaVantageAPIKey = "demo"
)

var (
    minioEndpoint        = getEnv("MINIO_ENDPOINT", defaultMinioEndpoint)
    minioAccessKeyID     = getEnv("MINIO_ACCESS_KEY_ID", defaultMinioAccessKeyID)
    minioSecretAccessKey = getEnv("MINIO_SECRET_ACCESS_KEY", defaultMinioSecretAccessKey)
    minioBucketName      = getEnv("MINIO_BUCKET", defaultMinioBucketName)
    minioUseSSL          = getEnvBool("MINIO_USE_SSL", false)

    yahooSymbol        = getEnv("YAHOO_SYMBOL", defaultYahooSymbol)
    alphavantageSymbol = getEnv("ALPHAVANTAGE_SYMBOL", defaultAlphaVantageSymbol)
    alphavantageAPIKey = getEnv("ALPHAVANTAGE_API_KEY", defaultAlphaVantageAPIKey)
)

type YahooQuoteResponse struct {
    Chart struct {
        Result []struct {
            Meta struct {
                Symbol string `json:"symbol"`
            } `json:"meta"`
            Timestamp []int64 `json:"timestamp"`
            Indicators struct {
                Quote []struct {
                    Open  []float64 `json:"open"`
                    High  []float64 `json:"high"`
                    Low   []float64 `json:"low"`
                    Close []float64 `json:"close"`
                    Volume []int64 `json:"volume"`
                } `json:"quote"`
            } `json:"indicators"`
        } `json:"result"`
    } `json:"chart"`
}

type AlphaVantageResponse struct {
    TimeSeriesDaily map[string]struct {
        Open  string `json:"1. open"`
        High  string `json:"2. high"`
        Low   string `json:"3. low"`
        Close string `json:"4. close"`
        Volume string `json:"5. volume"`
    } `json:"Time Series (Daily)"`
}

func main() {
    ctx := context.Background()

    client, err := minio.New(minioEndpoint, &minio.Options{
        Creds:  credentials.NewStaticV4(minioAccessKeyID, minioSecretAccessKey, ""),
        Secure: minioUseSSL,
    })
    if err != nil {
        log.Fatalf("failed to initialize MinIO client: %v", err)
    }

    if err := ensureBucket(ctx, client, minioBucketName); err != nil {
        log.Fatalf("failed to ensure bucket: %v", err)
    }

    sources := []struct {
        name       string
        symbol     string
        objectName string
        fetch      func(string) (string, error)
    }{
        {
            name:       "Yahoo Finance",
            symbol:     yahooSymbol,
            objectName: fmt.Sprintf("yahoo-%s.csv", strings.ToLower(yahooSymbol)),
            fetch:      fetchYahooFinanceCSV,
        },
        {
            name:       "AlphaVantage",
            symbol:     alphavantageSymbol,
            objectName: fmt.Sprintf("alphavantage-%s.csv", strings.ToLower(alphavantageSymbol)),
            fetch:      fetchAlphaVantageCSV,
        },
    }

    for _, source := range sources {
        log.Printf("Fetching %s for symbol %s...", source.name, source.symbol)
        filePath, err := source.fetch(source.symbol)
        if err != nil {
            log.Fatalf("failed to fetch %s data: %v", source.name, err)
        }

        if err := uploadObject(ctx, client, filePath, source.objectName); err != nil {
            log.Fatalf("failed to upload %s CSV: %v", source.name, err)
        }
    }

    log.Println("Successfully uploaded finance CSV files to MinIO bucket:", minioBucketName)
}

func ensureBucket(ctx context.Context, client *minio.Client, bucketName string) error {
    exists, err := client.BucketExists(ctx, bucketName)
    if err != nil {
        return err
    }
    if exists {
        return nil
    }
    return client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
}

func fetchYahooFinanceCSV(symbol string) (string, error) {
    url := fmt.Sprintf("https://query1.finance.yahoo.com/v8/finance/chart/%s?interval=1d&range=1mo", symbol)
    resp, err := http.Get(url)
    if err != nil {
        return "", err
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        bodyBytes, _ := io.ReadAll(resp.Body)
        return "", fmt.Errorf("yahoo finance request failed: %s %s", resp.Status, string(bodyBytes))
    }

    var data YahooQuoteResponse
    if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
        return "", err
    }

    if len(data.Chart.Result) == 0 {
        return "", fmt.Errorf("yahoo finance response missing result")
    }

    result := data.Chart.Result[0]
    if len(result.Timestamp) == 0 || len(result.Indicators.Quote) == 0 {
        return "", fmt.Errorf("yahoo finance response missing quote data")
    }

    filePath := filepath.Join(os.TempDir(), fmt.Sprintf("yahoo-%s.csv", strings.ToLower(symbol)))
    file, err := os.Create(filePath)
    if err != nil {
        return "", err
    }
    defer file.Close()

    writer := csv.NewWriter(file)
    defer writer.Flush()

    writer.Write([]string{"symbol", "timestamp", "open", "high", "low", "close", "volume"})
    quote := result.Indicators.Quote[0]
    for i, ts := range result.Timestamp {
        if i >= len(quote.Open) || i >= len(quote.High) || i >= len(quote.Low) || i >= len(quote.Close) || i >= len(quote.Volume) {
            continue
        }
        if quote.Open[i] == 0 && quote.Close[i] == 0 && quote.Volume[i] == 0 {
            continue
        }
        row := []string{
            symbol,
            time.Unix(ts, 0).UTC().Format(time.RFC3339),
            fmt.Sprintf("%.6f", quote.Open[i]),
            fmt.Sprintf("%.6f", quote.High[i]),
            fmt.Sprintf("%.6f", quote.Low[i]),
            fmt.Sprintf("%.6f", quote.Close[i]),
            fmt.Sprintf("%d", quote.Volume[i]),
        }
        writer.Write(row)
    }

    return filePath, nil
}

func fetchAlphaVantageCSV(symbol string) (string, error) {
    url := fmt.Sprintf("https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=%s&outputsize=compact&apikey=%s", symbol, alphavantageAPIKey)
    resp, err := http.Get(url)
    if err != nil {
        return "", err
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        bodyBytes, _ := io.ReadAll(resp.Body)
        return "", fmt.Errorf("alphavantage request failed: %s %s", resp.Status, string(bodyBytes))
    }

    var data AlphaVantageResponse
    if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
        return "", err
    }

    if len(data.TimeSeriesDaily) == 0 {
        return "", fmt.Errorf("alphavantage response missing time series data")
    }

    filePath := filepath.Join(os.TempDir(), fmt.Sprintf("alphavantage-%s.csv", strings.ToLower(symbol)))
    file, err := os.Create(filePath)
    if err != nil {
        return "", err
    }
    defer file.Close()

    writer := csv.NewWriter(file)
    defer writer.Flush()

    writer.Write([]string{"symbol", "date", "open", "high", "low", "close", "volume"})
    for date, entry := range data.TimeSeriesDaily {
        writer.Write([]string{
            symbol,
            date,
            entry.Open,
            entry.High,
            entry.Low,
            entry.Close,
            entry.Volume,
        })
    }

    return filePath, nil
}

func uploadObject(ctx context.Context, client *minio.Client, filePath, objectName string) error {
    file, err := os.Open(filePath)
    if err != nil {
        return err
    }
    defer file.Close()

    stat, err := file.Stat()
    if err != nil {
        return err
    }

    _, err = client.PutObject(ctx, minioBucketName, objectName, file, stat.Size(), minio.PutObjectOptions{ContentType: "text/csv"})
    return err
}

func getEnv(key, defaultValue string) string {
    if value := os.Getenv(key); value != "" {
        return value
    }
    return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
    value := os.Getenv(key)
    if value == "" {
        return defaultValue
    }
    switch strings.ToLower(value) {
    case "1", "true", "yes", "on":
        return true
    default:
        return false
    }
}
