package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gregdavisd/coinspot_go/coinspot"
)

//
// CSV Stream Abstraction
//

// CSVStream defines the interface for CSV output destinations
type CSVStream interface {
	WriteHeader(headers []string) error
	WriteRow(row []string) error
	Flush() error
	Close() error
}

// StdoutCSVStream writes CSV to os.Stdout
type StdoutCSVStream struct {
	writer *csv.Writer
}

func NewStdoutCSVStream() *StdoutCSVStream {
	return &StdoutCSVStream{writer: csv.NewWriter(os.Stdout)}
}

func (s *StdoutCSVStream) WriteHeader(headers []string) error { return s.writer.Write(headers) }
func (s *StdoutCSVStream) WriteRow(row []string) error        { return s.writer.Write(row) }
func (s *StdoutCSVStream) Flush() error                       { s.writer.Flush(); return s.writer.Error() }
func (s *StdoutCSVStream) Close() error                       { return s.Flush() }

// FileCSVStream writes CSV to a file
type FileCSVStream struct {
	writer *csv.Writer
	file   *os.File
}

func NewFileCSVStream(path string) (*FileCSVStream, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create CSV file: %w", err)
	}
	return &FileCSVStream{
		writer: csv.NewWriter(f),
		file:   f,
	}, nil
}

func (f *FileCSVStream) WriteHeader(headers []string) error { return f.writer.Write(headers) }
func (f *FileCSVStream) WriteRow(row []string) error        { return f.writer.Write(row) }
func (f *FileCSVStream) Flush() error                       { f.writer.Flush(); return f.writer.Error() }
func (f *FileCSVStream) Close() error {
	if err := f.Flush(); err != nil {
		return err
	}
	return f.file.Close()
}

//
// Response to CSV Converters
//

func exportLatestPrices(stream CSVStream, resp *coinspot.LatestPricesResponse) error {
	if err := stream.WriteHeader([]string{"coin", "bid", "ask", "last"}); err != nil {
		return err
	}
	for coin, p := range resp.Prices {
		if err := stream.WriteRow([]string{
			coin, fmt.Sprintf("%.8f", p.Bid), fmt.Sprintf("%.8f", p.Ask), fmt.Sprintf("%.8f", p.Last),
		}); err != nil {
			return err
		}
	}
	return stream.Flush()
}

func exportLatestCoinPrices(stream CSVStream, resp *coinspot.LatestCoinPricesResponse, coin string) error {
	return stream.WriteRow([]string{coin, fmt.Sprintf("%.8f", resp.Prices.Bid), fmt.Sprintf("%.8f", resp.Prices.Ask), fmt.Sprintf("%.8f", resp.Prices.Last)})
}

func exportLatestCoinMarketPrices(stream CSVStream, resp *coinspot.LatestCoinMarketPricesResponse, coin, market string) error {
	return stream.WriteRow([]string{coin, market, fmt.Sprintf("%.8f", resp.Prices.Bid), fmt.Sprintf("%.8f", resp.Prices.Ask), fmt.Sprintf("%.8f", resp.Prices.Last)})
}

func exportLatestPrice(stream CSVStream, resp *coinspot.LatestPriceResponse) error {
	return stream.WriteRow([]string{resp.Market, fmt.Sprintf("%.8f", resp.Rate)})
}

func exportOpenOrders(stream CSVStream, resp *coinspot.OpenOrdersResponse) error {
	if err := stream.WriteHeader([]string{"side", "amount", "rate", "total", "coin", "market"}); err != nil {
		return err
	}
	for _, o := range resp.BuyOrders {
		if err := stream.WriteRow([]string{"buy", fmt.Sprintf("%.8f", o.Amount), fmt.Sprintf("%.8f", o.Rate), fmt.Sprintf("%.8f", o.Total), o.Coin, o.Market}); err != nil {
			return err
		}
	}
	for _, o := range resp.SellOrders {
		if err := stream.WriteRow([]string{"sell", fmt.Sprintf("%.8f", o.Amount), fmt.Sprintf("%.8f", o.Rate), fmt.Sprintf("%.8f", o.Total), o.Coin, o.Market}); err != nil {
			return err
		}
	}
	return stream.Flush()
}

func exportOpenOrdersMarket(stream CSVStream, resp *coinspot.OpenOrdersMarketResponse) error {
	return exportOpenOrders(stream, (*coinspot.OpenOrdersResponse)(resp))
}

func exportCompletedOrders(stream CSVStream, resp *coinspot.CompletedOrdersResponse) error {
	if err := stream.WriteHeader([]string{"side", "amount", "rate", "total", "coin", "market", "solddate"}); err != nil {
		return err
	}
	for _, o := range resp.BuyOrders {
		if err := stream.WriteRow([]string{"buy", fmt.Sprintf("%.8f", o.Amount), fmt.Sprintf("%.8f", o.Rate), fmt.Sprintf("%.8f", o.Total), o.Coin, o.Market, o.SoldDate}); err != nil {
			return err
		}
	}
	for _, o := range resp.SellOrders {
		if err := stream.WriteRow([]string{"sell", fmt.Sprintf("%.8f", o.Amount), fmt.Sprintf("%.8f", o.Rate), fmt.Sprintf("%.8f", o.Total), o.Coin, o.Market, o.SoldDate}); err != nil {
			return err
		}
	}
	return stream.Flush()
}

func exportCompletedOrdersMarket(stream CSVStream, resp *coinspot.CompletedOrdersMarketResponse) error {
	return exportCompletedOrders(stream, (*coinspot.CompletedOrdersResponse)(resp))
}

func exportCompletedOrdersSummary(stream CSVStream, resp *coinspot.CompletedOrdersSummaryResponse) error {
	if err := stream.WriteHeader([]string{"side", "amount", "rate", "total", "coin", "market", "solddate"}); err != nil {
		return err
	}
	for _, o := range resp.Orders {
		if err := stream.WriteRow([]string{"unknown", fmt.Sprintf("%.8f", o.Amount), fmt.Sprintf("%.8f", o.Rate), fmt.Sprintf("%.8f", o.Total), o.Coin, o.Market, o.SoldDate}); err != nil {
			return err
		}
	}
	return stream.Flush()
}

func exportCompletedOrdersSummaryMarket(stream CSVStream, resp *coinspot.CompletedOrdersSummaryMarketResponse) error {
	return exportCompletedOrdersSummary(stream, (*coinspot.CompletedOrdersSummaryResponse)(resp))
}

//
// Public API Test Runner
//

type PublicAPITester struct {
	client *coinspot.Client
	stream CSVStream
	ctx    context.Context
}

func NewPublicAPITester(cfg coinspot.Config, stream CSVStream, ctx context.Context) *PublicAPITester {
	return &PublicAPITester{
		client: coinspot.NewClient(cfg),
		stream: stream,
		ctx:    ctx,
	}
}

func (t *PublicAPITester) RunAll() error {
	pub := t.client.PublicClient()
	tests := []struct {
		name string
		fn   func() error
	}{
		{"Latest Prices", func() error {
			resp, err := pub.GetLatestPrices(t.ctx)
			if err != nil {
				return err
			}
			return exportLatestPrices(t.stream, resp)
		}},
		{"Latest Coin Prices (BTC)", func() error {
			resp, err := pub.GetLatestCoinPrices(t.ctx, "BTC")
			if err != nil {
				return err
			}
			return exportLatestCoinPrices(t.stream, resp, "BTC")
		}},
		{"Latest Coin Market Prices (BTC/USDT)", func() error {
			resp, err := pub.GetLatestCoinMarketPrices(t.ctx, "BTC", "USDT")
			if err != nil {
				return err
			}
			return exportLatestCoinMarketPrices(t.stream, resp, "BTC", "USDT")
		}},
		{"Latest Buy Price (BTC)", func() error {
			resp, err := pub.GetLatestBuyPrice(t.ctx, "BTC")
			if err != nil {
				return err
			}
			return exportLatestPrice(t.stream, resp)
		}},
		{"Latest Buy Price Market (BTC/USDT)", func() error {
			resp, err := pub.GetLatestBuyPriceMarket(t.ctx, "BTC", "USDT")
			if err != nil {
				return err
			}
			return exportLatestPrice(t.stream, resp)
		}},
		{"Latest Sell Price (BTC)", func() error {
			resp, err := pub.GetLatestSellPrice(t.ctx, "BTC")
			if err != nil {
				return err
			}
			return exportLatestPrice(t.stream, resp)
		}},
		{"Latest Sell Price Market (BTC/USDT)", func() error {
			resp, err := pub.GetLatestSellPriceMarket(t.ctx, "BTC", "USDT")
			if err != nil {
				return err
			}
			return exportLatestPrice(t.stream, resp)
		}},
		{"Open Orders (BTC)", func() error {
			resp, err := pub.GetOpenOrders(t.ctx, "BTC")
			if err != nil {
				return err
			}
			return exportOpenOrders(t.stream, resp)
		}},
		{"Open Orders Market (BTC/USDT)", func() error {
			resp, err := pub.GetOpenOrdersMarket(t.ctx, "BTC", "USDT")
			if err != nil {
				return err
			}
			return exportOpenOrdersMarket(t.stream, resp)
		}},
		{"Completed Orders (BTC)", func() error {
			resp, err := pub.GetCompletedOrders(t.ctx, "BTC")
			if err != nil {
				return err
			}
			return exportCompletedOrders(t.stream, resp)
		}},
		{"Completed Orders Market (BTC/USDT)", func() error {
			resp, err := pub.GetCompletedOrdersMarket(t.ctx, "BTC", "USDT")
			if err != nil {
				return err
			}
			return exportCompletedOrdersMarket(t.stream, resp)
		}},
		{"Completed Orders Summary (BTC)", func() error {
			resp, err := pub.GetCompletedOrdersSummary(t.ctx, "BTC")
			if err != nil {
				return err
			}
			return exportCompletedOrdersSummary(t.stream, resp)
		}},
		{"Completed Orders Summary Market (BTC/USDT)", func() error {
			resp, err := pub.GetCompletedOrdersSummaryMarket(t.ctx, "BTC", "USDT")
			if err != nil {
				return err
			}
			return exportCompletedOrdersSummaryMarket(t.stream, resp)
		}},
	}

	failedTests := 0
	for _, tc := range tests {
		fmt.Printf("▶ Running: %s\n", tc.name)
		if err := tc.fn(); err != nil {
			log.Printf("⚠ Failed %s: %v\n", tc.name, err)
			failedTests++
			continue // Continue testing other endpoints
		}
		fmt.Printf("✅ Completed: %s\n\n", tc.name)
	}

	if failedTests > 0 {
		return fmt.Errorf("%d test(s) failed", failedTests)
	}
	return nil
}

// Add to main.go

type PrivateAPITester struct {
	client    *coinspot.Client
	stream    CSVStream
	ctx       context.Context
	apiKey    string
	secretKey string
}

func NewPrivateAPITester(cfg coinspot.Config, stream CSVStream, ctx context.Context, apiKey, secretKey string) *PrivateAPITester {
	return &PrivateAPITester{
		client:    coinspot.NewClient(cfg),
		stream:    stream,
		ctx:       ctx,
		apiKey:    apiKey,
		secretKey: secretKey,
	}
}

func (t *PrivateAPITester) RunROTests() error {
	ro := t.client.ReadOnlyClient() // Uses /api/v2/ro
	tests := []struct {
		name string
		fn   func() error
	}{
		{"RO Status", func() error {
			resp, err := ro.ROCheckStatus(t.ctx, t.apiKey, t.secretKey)
			if err != nil {
				return err
			}
			return t.stream.WriteRow([]string{"ro_status", resp.Status})
		}},
		{"RO Balances", func() error {
			resp, err := ro.ROGetBalances(t.ctx, t.apiKey, t.secretKey)
			if err != nil {
				return err
			}
			if err := t.stream.WriteHeader([]string{"coin", "balance", "audbalance", "rate"}); err != nil {
				return err
			}
			for coin, b := range resp.Balances {
				if err := t.stream.WriteRow([]string{coin, fmt.Sprintf("%.8f", b.Balance), fmt.Sprintf("%.8f", b.AudBalance), fmt.Sprintf("%.8f", b.Rate)}); err != nil {
					return err
				}
			}
			return t.stream.Flush()
		}},
		{"RO Open Market Orders", func() error {
			resp, err := ro.ROGetMyOpenMarketOrders(t.ctx, t.apiKey, t.secretKey, "", "")
			if err != nil {
				return err
			}
			if err := t.stream.WriteHeader([]string{"side", "id", "coin", "market", "amount", "rate", "total", "created"}); err != nil {
				return err
			}
			for _, o := range resp.BuyOrders {
				if err := t.stream.WriteRow([]string{"buy", o.ID, o.Coin, o.Market, fmt.Sprintf("%.8f", o.Amount), fmt.Sprintf("%.8f", o.Rate), fmt.Sprintf("%.8f", o.Total), o.Created}); err != nil {
					return err
				}
			}
			for _, o := range resp.SellOrders {
				if err := t.stream.WriteRow([]string{"sell", o.ID, o.Coin, o.Market, fmt.Sprintf("%.8f", o.Amount), fmt.Sprintf("%.8f", o.Rate), fmt.Sprintf("%.8f", o.Total), o.Created}); err != nil {
					return err
				}
			}
			return t.stream.Flush()
		}},
	}

	for _, tc := range tests {
		fmt.Printf("▶ RO Test: %s\n", tc.name)
		if err := tc.fn(); err != nil {
			log.Printf("⚠ Failed %s: %v\n", tc.name, err)
		} else {
			fmt.Printf("✅ Completed %s\n\n", tc.name)
		}
	}
	return nil
}

func main() {
	// Option 1: Stream to stdout
	stream := NewStdoutCSVStream()
	defer stream.Close()

	// Option 2: Stream to file (uncomment to use)
	// fileStream, err := NewFileCSVStream("coinspot_pub_api_test.csv")
	// if err != nil { log.Fatal(err) }
	// defer fileStream.Close()
	// stream = fileStream

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	tester := NewPublicAPITester(coinspot.Config{BaseURL: "www.coinspot.com.au", RateLimitPerMin: 990}, stream, ctx)

	// Add timeout to prevent hanging on network issues
	defer cancel()

	if err := tester.RunAll(); err != nil {
		log.Printf("❌ Test run failed: %v", err)
		os.Exit(1)
	}
	os.Exit(0)
}
