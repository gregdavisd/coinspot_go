package coinspot

//	CoinSpot API Client for Go
// see: https://www.coinspot.com.au/v2/api

// API Architecture & Tiers
// The API is divided into three distinct tiers, each with specific routing and access requirements:
//
// Public API (/pubapi/v2): GET-only, unauthenticated. Provides real-time market data (latest prices, buy/sell rates, order book snapshots for top 20–100 orders).
// Full Access API (/api/v2): POST-only, requires standard API key. Enables trading operations (market/instant buys/sells/swaps, order placement/editing, cancellation, and withdrawals).
// Read-Only API (/api/v2/ro): POST-only, requires dedicated RO key. Provides account visibility (balances, order history, deposits/withdrawals, affiliate/referral payments) without executing trades.
//
// Authentication & Security
// Headers: All POST requests must include key (API key) and sign (HMAC-SHA512 signature).
// Signature Mechanism: sign is computed by HMAC-SHA512 hashing the form-encoded POST body using the secret key.
// Nonce Requirement: A strictly increasing integer must be included in every POST request to prevent replay attacks.
// Error Format: Non-200 responses or API errors return {"status":"error", "message":"..."}. Successful responses return {"status":"ok"} alongside payload data.
//
// Data & Payload Constraints
// Format: All requests/responses are JSON. Dates/times are UTC ISO 8601.
// Precision Limits: Crypto amounts support up to 8 decimal places; AUD amounts support 2 decimal places.
// Optional Parameters: Many endpoints support filtering (e.g., markettype, startdate/enddate, limit). Defaults are clearly documented (e.g., limit defaults to 200, max 500 for history endpoints).
// Network Dependencies: Withdrawal endpoints require network-specific details (paymentid, fee, minsend) fetched via /my/coin/withdraw/senddetails.
//
// Operational Constraints & Best Practices
// Rate Limiting: Hard cap of 1,000 requests per minute across all endpoints.
// Cancellation Warning: Frequent cancel requests may trigger automated rate limiting. Editing open orders is recommended for price adjustments.
// Timeouts & Retries: The documentation does not mandate client-side retry logic, but notes that network or server errors may occur, implying robust client handling is necessary.
// Key Management: API keys are tied to registered accounts. RO keys are separate and restricted to read operations.
//
// Implementation Context
// Directly implements the documented specifications while extending them with production-grade reliability: thread-safe nonce generation, client-side rate limiting, exponential backoff retries, and type-safe JSON unmarshaling.
// This API is designed for deterministic, secure, and high-throughput interactions, with clear boundaries between public data access, authenticated trading, and account monitoring.

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Global Thread-Safe Strict Nonce Incrementor
var (
	nonceMu sync.Mutex
	nonce   int64 = 0
)

// nextNonce returns a strictly increasing, thread-safe nonce.
func nextNonce() int64 {
	nonceMu.Lock()
	defer nonceMu.Unlock()
	nonce++
	return nonce
}

// RetryConfig holds retry policy settings for HTTP requests.
type RetryConfig struct {
	MaxRetries     int           // Max attempts before failing (0 = no retry)
	BaseDelay      time.Duration // Initial delay (e.g., 500ms)
	MaxDelay       time.Duration // Cap delay (e.g., 10s)
	RetryableCodes []int         // HTTP codes to retry on (default: 429, 500-504)
}

// Config holds the client initialization parameters.
type Config struct {
	// BaseURL is the domain without protocol. e.g., "www.coinspot.com.au"
	BaseURL         string
	RateLimitPerMin int64 // 0 disables rate limiting
	RetryConfig     RetryConfig
}

// Client represents the CoinSpot API client.
// API keys are NOT stored here. They are passed per-call for multi-account support.
type Client struct {
	HTTPClient  *http.Client
	BaseURL     string
	RateLimiter *rateLimiter
	RetryConfig RetryConfig
}

// rateLimiter enforces a fixed delay between consecutive requests across all goroutines.
type rateLimiter struct {
	mu          sync.Mutex
	nextAllowed time.Time
	interval    time.Duration
}

// newRateLimiter creates a pacer that ensures at least `interval` seconds between requests.
func newRateLimiter(ratePerMinute int64) *rateLimiter {
	if ratePerMinute <= 0 {
		return nil
	}
	return &rateLimiter{
		nextAllowed: time.Now(),
		interval:    time.Duration(float64(time.Minute) / float64(ratePerMinute)),
	}

}

// wait blocks until the configured interval has passed since the last request.
// Respects context cancellation.
func (rl *rateLimiter) wait(ctx context.Context) error {
	if rl == nil {
		return nil
	}

	rl.mu.Lock()
	now := time.Now()
	var sleep time.Duration

	if now.Before(rl.nextAllowed) {
		sleep = rl.nextAllowed.Sub(now)
		// Apply 0-10% jitter to desynchronize wake-up times
		if sleep > 0 {
			maxJitter := time.Duration(float64(sleep) * 0.1)
			jitter := time.Duration(rand.Int63n(int64(maxJitter)))
			sleep += jitter
		}
	}

	// Update nextAllowed based on captured 'now' while holding lock
	rl.nextAllowed = now.Add(rl.interval)
	rl.mu.Unlock()

	if sleep > 0 {
		select {
		case <-time.After(sleep):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// NewClient creates a new CoinSpot API client with the given configuration.
// It automatically prepends "https://" to the domain if no scheme is provided.
// Retry configuration defaults are applied if not specified.
func NewClient(cfg Config) *Client {
	baseURL := cfg.BaseURL
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "https://" + baseURL
	}

	// Set sensible defaults for retry config
	if cfg.RetryConfig.MaxRetries == 0 {
		cfg.RetryConfig.MaxRetries = 3
	}
	if cfg.RetryConfig.BaseDelay == 0 {
		cfg.RetryConfig.BaseDelay = 500 * time.Millisecond
	}
	if cfg.RetryConfig.MaxDelay == 0 {
		cfg.RetryConfig.MaxDelay = 10 * time.Second
	}
	if len(cfg.RetryConfig.RetryableCodes) == 0 {
		cfg.RetryConfig.RetryableCodes = []int{429, 500, 502, 503, 504}
	}

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	return &Client{
		HTTPClient: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		},
		BaseURL:     baseURL,
		RateLimiter: newRateLimiter(cfg.RateLimitPerMin),
		RetryConfig: cfg.RetryConfig,
	}
}

// PublicClient returns a client configured for the Public API (GET requests, no auth).
func (c *Client) PublicClient() *Client {
	return &Client{
		HTTPClient:  c.HTTPClient,
		BaseURL:     c.BaseURL + "/pubapi/v2",
		RateLimiter: c.RateLimiter,
		RetryConfig: c.RetryConfig,
	}
}

// ReadOnlyClient returns a client configured for the Read-Only API (POST requests, requires auth).
func (c *Client) ReadOnlyClient() *Client {
	return &Client{
		HTTPClient:  c.HTTPClient,
		BaseURL:     c.BaseURL + "/api/v2/ro",
		RateLimiter: c.RateLimiter,
		RetryConfig: c.RetryConfig,
	}
}

// TradeClient returns a client configured for the trading API, performs transactions on Coinspot (POST requests, requires auth).
func (c *Client) TradeClient() *Client {
	return &Client{
		HTTPClient:  c.HTTPClient,
		BaseURL:     c.BaseURL + "/api/v2",
		RateLimiter: c.RateLimiter,
		RetryConfig: c.RetryConfig,
	}
}

//
// HTTP Helpers
//

func (c *Client) doRequest(ctx context.Context, method, path string, params url.Values, apiKey, secretKey string) ([]byte, error) {
	var lastErr error
	var lastStatusCode int

	for attempt := 0; attempt <= c.RetryConfig.MaxRetries; attempt++ {
		// Enforce client-side rate limiting
		if err := c.RateLimiter.wait(ctx); err != nil {
			return nil, err
		}

		var req *http.Request
		var err error

		if method == http.MethodGet {
			queryStr := params.Encode()
			if queryStr != "" {
				path += "?" + queryStr
			}
			req, err = http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
		} else {
			// Copy params to avoid modifying caller's url.Values
			postParams := url.Values{}
			for k, v := range params {
				postParams[k] = append([]string{}, v...)
			}
			postParams.Set("nonce", fmt.Sprintf("%d", nextNonce()))
			req, err = http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, bytes.NewBufferString(postParams.Encode()))
			if err != nil {
				return nil, err
			}
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("key", apiKey)
			req.Header.Set("sign", signData(secretKey, postParams))
		}
		if err != nil {
			return nil, err
		}

		resp, err := c.HTTPClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("request failed: %w", err)
			lastStatusCode = 0
		} else {
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				lastErr = fmt.Errorf("failed to read response: %w", err)
				lastStatusCode = 0
			} else {
				lastStatusCode = resp.StatusCode
				if lastStatusCode >= 500 || lastStatusCode == 429 {
					lastErr = fmt.Errorf("server error %d: %s", lastStatusCode, string(body))
				} else if lastStatusCode != http.StatusOK {
					lastErr = fmt.Errorf("API request failed with status %d: %s", lastStatusCode, string(body))
				} else {
					return body, nil
				}
			}
		}

		// Retry logic
		if attempt < c.RetryConfig.MaxRetries && isRetryableStatusCode(lastStatusCode, c.RetryConfig.RetryableCodes) {
			delay := c.calculateBackoff(attempt)
			if err := waitWithContext(ctx, delay); err != nil {
				return nil, err
			}
			continue
		}
		return nil, lastErr
	}
	return nil, lastErr
}

func isRetryableStatusCode(code int, codes []int) bool {
	if code == 0 {
		return true // Network errors are retryable
	}
	for _, c := range codes {
		if c == code {
			return true
		}
	}
	return false
}

func (c *Client) calculateBackoff(attempt int) time.Duration {
	d := c.RetryConfig.BaseDelay
	if attempt > 0 {
		d = c.RetryConfig.BaseDelay << uint(attempt) // Exponential: 1x, 2x, 4x...
	}
	if d > c.RetryConfig.MaxDelay {
		d = c.RetryConfig.MaxDelay
	}
	// Add 0-50% jitter to prevent thundering herd
	jitter := time.Duration(rand.Int63n(int64(d) / 2))
	return d + jitter
}

func waitWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// signData computes HMAC-SHA512 of the form-encoded parameters.
func signData(secretKey string, data url.Values) string {
	h := hmac.New(sha512.New, []byte(secretKey))
	h.Write([]byte(data.Encode()))
	return hex.EncodeToString(h.Sum(nil))
}

// decodeResponse unmarshals JSON into a typesafe struct and checks for API errors.
func decodeResponse[T any](ctx context.Context, path string, params url.Values, apiKey, secretKey string, c *Client) (*T, error) {
	body, err := c.doRequest(ctx, http.MethodPost, path, params, apiKey, secretKey)
	if err != nil {
		return nil, err
	}

	var apiResp struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	if apiResp.Status == "error" {
		return nil, fmt.Errorf("API error: %s: %w", apiResp.Message, io.ErrUnexpectedEOF)
	}

	var result T
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %w", err)
	}
	return &result, nil
}

// decodePublicResponse is for GET requests (Public API)
func decodePublicResponse[T any](ctx context.Context, path string, params url.Values, c *Client) (*T, error) {
	body, err := c.doRequest(ctx, http.MethodGet, path, params, "", "")
	if err != nil {
		return nil, err
	}

	var apiResp struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	if apiResp.Status == "error" {
		return nil, fmt.Errorf("API error: %s", apiResp.Message)
	}

	var result T
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %w", err)
	}
	return &result, nil
}

// stringAsFloat64 handles JSON unmarshaling of numeric values that may come as strings or numbers.
type stringAsFloat64 float64

func (f *stringAsFloat64) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}

	switch value := v.(type) {
	case float64:
		*f = stringAsFloat64(value)
	case string:
		if value == "" {
			*f = 0
		} else {
			var parsed float64
			_, err := fmt.Sscanf(value, "%f", &parsed)
			if err != nil {
				return fmt.Errorf("cannot parse %q as float64: %w", value, err)
			}
			*f = stringAsFloat64(parsed)
		}
	default:
		return fmt.Errorf("cannot unmarshal %T into StringAsFloat64", v)
	}
	return nil
}

//
// Response Structs (Typesafe)
//

// Public API Responses
type LatestPricesResponse struct {
	Status  string              `json:"status"`
	Message string              `json:"message"`
	Prices  map[string]PriceObj `json:"prices"`
}

type LatestCoinPricesResponse struct {
	Status  string   `json:"status"`
	Message string   `json:"message"`
	Prices  PriceObj `json:"prices"`
}

type LatestCoinMarketPricesResponse struct {
	Status  string   `json:"status"`
	Message string   `json:"message"`
	Prices  PriceObj `json:"prices"`
}

type LatestPriceResponse struct {
	Status  string          `json:"status"`
	Message string          `json:"message"`
	Rate    stringAsFloat64 `json:"rate"`
	Market  string          `json:"market"`
}

type OpenOrdersResponse struct {
	Status     string     `json:"status"`
	Message    string     `json:"message"`
	BuyOrders  []OrderObj `json:"buyorders"`
	SellOrders []OrderObj `json:"sellorders"`
}

type OpenOrdersMarketResponse struct {
	Status     string     `json:"status"`
	Message    string     `json:"message"`
	BuyOrders  []OrderObj `json:"buyorders"`
	SellOrders []OrderObj `json:"sellorders"`
}

type CompletedOrdersResponse struct {
	Status     string     `json:"status"`
	Message    string     `json:"message"`
	BuyOrders  []OrderObj `json:"buyorders"`
	SellOrders []OrderObj `json:"sellorders"`
}

type CompletedOrdersMarketResponse struct {
	Status     string     `json:"status"`
	Message    string     `json:"message"`
	BuyOrders  []OrderObj `json:"buyorders"`
	SellOrders []OrderObj `json:"sellorders"`
}

type CompletedOrdersSummaryResponse struct {
	Status  string     `json:"status"`
	Message string     `json:"message"`
	Orders  []OrderObj `json:"orders"`
}

type CompletedOrdersSummaryMarketResponse struct {
	Status  string     `json:"status"`
	Message string     `json:"message"`
	Orders  []OrderObj `json:"orders"`
}

type PriceObj struct {
	Bid  stringAsFloat64 `json:"bid"`
	Ask  stringAsFloat64 `json:"ask"`
	Last stringAsFloat64 `json:"last"`
}

type OrderObj struct {
	Amount   stringAsFloat64 `json:"amount"`
	Rate     stringAsFloat64 `json:"rate"`
	Total    stringAsFloat64 `json:"total"`
	Coin     string          `json:"coin"`
	Market   string          `json:"market"`
	SoldDate string          `json:"solddate,omitempty"`
}

// Private & Read-Only API Responses
type StatusResponse struct {
	Status string `json:"status"`
}

type DepositAddressResponse struct {
	Status   string       `json:"status"`
	Message  string       `json:"message"`
	Networks []NetworkObj `json:"networks"`
}

type NetworkObj struct {
	Name    string `json:"name"`
	Network string `json:"network"`
	Address string `json:"address"`
	Memo    string `json:"memo"`
}

type CoinListResponse struct {
	Status   string   `json:"status"`
	Message  string   `json:"message"`
	CoinList []string `json:"coinlist"`
}

type QuoteResponse struct {
	Status  string          `json:"status"`
	Message string          `json:"message"`
	Rate    stringAsFloat64 `json:"rate"`
}

type OrderResponse struct {
	Status  string          `json:"status"`
	Message string          `json:"message"`
	Coin    string          `json:"coin"`
	Market  string          `json:"market"`
	Amount  stringAsFloat64 `json:"amount"`
	Rate    stringAsFloat64 `json:"rate"`
	ID      string          `json:"id"`
}

type EditOrderResponse struct {
	Status  string          `json:"status"`
	Message string          `json:"message"`
	Updated bool            `json:"updated"`
	Coin    string          `json:"coin"`
	Rate    stringAsFloat64 `json:"rate"`
	NewRate stringAsFloat64 `json:"newrate"`
	Amount  stringAsFloat64 `json:"amount"`
	Total   stringAsFloat64 `json:"total"`
	ID      string          `json:"id"`
}

type BuyNowResponse struct {
	Status  string          `json:"status"`
	Message string          `json:"message"`
	Coin    string          `json:"coin"`
	Market  string          `json:"market"`
	Amount  stringAsFloat64 `json:"amount"`
	Total   stringAsFloat64 `json:"total"`
}

type SellNowResponse struct {
	Status  string          `json:"status"`
	Message string          `json:"message"`
	Coin    string          `json:"coin"`
	Market  string          `json:"market"`
	Amount  stringAsFloat64 `json:"amount"`
	Rate    stringAsFloat64 `json:"rate"`
	Total   stringAsFloat64 `json:"total"`
}

type SwapNowResponse struct {
	Status  string          `json:"status"`
	Message string          `json:"message"`
	Coin    string          `json:"coin"`
	Market  string          `json:"market"`
	Amount  stringAsFloat64 `json:"amount"`
	Rate    stringAsFloat64 `json:"rate"`
	Total   stringAsFloat64 `json:"total"`
}

type CancelResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

type WithdrawDetailsResponse struct {
	Status   string        `json:"status"`
	Message  string        `json:"message"`
	Networks []WithdrawNet `json:"networks"`
}

type WithdrawNet struct {
	Network   string          `json:"network"`
	PaymentID string          `json:"paymentid"`
	Fee       stringAsFloat64 `json:"fee"`
	MinSend   stringAsFloat64 `json:"minsend"`
	Default   bool            `json:"default"`
}

type WithdrawResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

type BalancesResponse struct {
	Status   string             `json:"status"`
	Message  string             `json:"message"`
	Balances map[string]Balance `json:"balances"`
}

type Balance struct {
	Balance    stringAsFloat64 `json:"balance"`
	Available  stringAsFloat64 `json:"available"`
	AudBalance stringAsFloat64 `json:"audbalance"`
	Rate       stringAsFloat64 `json:"rate"`
}

type BalanceResponse struct {
	Status  string             `json:"status"`
	Message string             `json:"message"`
	Balance map[string]Balance `json:"balance"`
}

type MarketOrdersResponse struct {
	Status     string           `json:"status"`
	Message    string           `json:"message"`
	BuyOrders  []MarketOrderObj `json:"buyorders"`
	SellOrders []MarketOrderObj `json:"sellorders"`
}

type MarketOrderObj struct {
	ID      string          `json:"id"`
	Coin    string          `json:"coin"`
	Market  string          `json:"market"`
	Amount  stringAsFloat64 `json:"amount"`
	Created string          `json:"created"`
	Rate    stringAsFloat64 `json:"rate"`
	Total   stringAsFloat64 `json:"total"`
}

type LimitOrderObj struct {
	ID      string          `json:"id"`
	Coin    string          `json:"coin"`
	Market  string          `json:"market"`
	Rate    stringAsFloat64 `json:"rate"`
	Amount  stringAsFloat64 `json:"amount"`
	Created string          `json:"created"`
	Type    string          `json:"type"`
}

type OpenLimitOrdersResponse struct {
	Status     string          `json:"status"`
	Message    string          `json:"message"`
	BuyOrders  []LimitOrderObj `json:"buyorders"`
	SellOrders []LimitOrderObj `json:"sellorders"`
}

type OrderHistoryResponse struct {
	Status     string         `json:"status"`
	Message    string         `json:"message"`
	BuyOrders  []HistoryOrder `json:"buyorders"`
	SellOrders []HistoryOrder `json:"sellorders"`
}

type HistoryOrder struct {
	ID          string          `json:"id"`
	Coin        string          `json:"coin"`
	Type        string          `json:"type"`
	Market      string          `json:"market"`
	Rate        stringAsFloat64 `json:"rate"`
	Amount      stringAsFloat64 `json:"amount"`
	Total       stringAsFloat64 `json:"total"`
	SoldDate    string          `json:"solddate"`
	AudFeeExGst stringAsFloat64 `json:"audfeeExGst"`
	AudGst      stringAsFloat64 `json:"audGst"`
	AudTotal    stringAsFloat64 `json:"audtotal"`
	Otc         *bool           `json:"otc"`
}

type SendReceiveHistoryResponse struct {
	Status              string  `json:"status"`
	Message             string  `json:"message"`
	SendTransactions    []TxObj `json:"sendtransactions"`
	ReceiveTransactions []TxObj `json:"receivetransactions"`
}

type TxObj struct {
	Timestamp string          `json:"timestamp"`
	Amount    stringAsFloat64 `json:"amount"`
	Coin      string          `json:"coin"`
	Address   string          `json:"address"`
	Aud       stringAsFloat64 `json:"aud"`
	SendFee   stringAsFloat64 `json:"sendfee"`
	From      string          `json:"from"`
}

type DepositHistoryResponse struct {
	Status   string       `json:"status"`
	Message  string       `json:"message"`
	Deposits []DepositObj `json:"deposits"`
}

type DepositObj struct {
	Amount    stringAsFloat64 `json:"amount"`
	Created   string          `json:"created"`
	Status    string          `json:"status"`
	Type      string          `json:"type"`
	Reference string          `json:"reference"`
}

type WithdrawalHistoryResponse struct {
	Status      string        `json:"status"`
	Message     string        `json:"message"`
	Withdrawals []WithdrawObj `json:"withdrawals"`
}

type WithdrawObj struct {
	Amount  stringAsFloat64 `json:"amount"`
	Created string          `json:"created"`
	Status  string          `json:"status"`
}

type PaymentResponse struct {
	Status   string       `json:"status"`
	Message  string       `json:"message"`
	Payments []PaymentObj `json:"payments"`
}

type PaymentObj struct {
	Amount    stringAsFloat64 `json:"amount"`
	Month     string          `json:"month"`
	Coin      string          `json:"coin"`
	AudAmt    stringAsFloat64 `json:"audamount"`
	Timestamp string          `json:"timestamp"`
}

//
// Public API Methods (GET)
//

func (c *Client) GetLatestPrices(ctx context.Context) (*LatestPricesResponse, error) {
	return decodePublicResponse[LatestPricesResponse](ctx, "/latest", url.Values{}, c)
}

// GetLatestCoinPrices returns the latest prices for a specific coin.
func (c *Client) GetLatestCoinPrices(ctx context.Context, coinType string) (*LatestCoinPricesResponse, error) {
	return decodePublicResponse[LatestCoinPricesResponse](ctx, fmt.Sprintf("/latest/%s", coinType), url.Values{}, c)
}

// GetLatestCoinMarketPrices returns the latest prices for a specific coin and market pair.
func (c *Client) GetLatestCoinMarketPrices(ctx context.Context, coinType, marketType string) (*LatestCoinMarketPricesResponse, error) {
	return decodePublicResponse[LatestCoinMarketPricesResponse](ctx, fmt.Sprintf("/latest/%s/%s", coinType, marketType), url.Values{}, c)
}

// GetLatestBuyPrice returns the latest buy price for a specific coin.
func (c *Client) GetLatestBuyPrice(ctx context.Context, coinType string) (*LatestPriceResponse, error) {
	return decodePublicResponse[LatestPriceResponse](ctx, fmt.Sprintf("/buyprice/%s", coinType), url.Values{}, c)
}

// GetLatestBuyPriceMarket returns the latest buy price for a specific coin and market pair.
func (c *Client) GetLatestBuyPriceMarket(ctx context.Context, coinType, marketType string) (*LatestPriceResponse, error) {
	return decodePublicResponse[LatestPriceResponse](ctx, fmt.Sprintf("/buyprice/%s/%s", coinType, marketType), url.Values{}, c)
}

// GetLatestSellPrice returns the latest sell price for a specific coin.
func (c *Client) GetLatestSellPrice(ctx context.Context, coinType string) (*LatestPriceResponse, error) {
	return decodePublicResponse[LatestPriceResponse](ctx, fmt.Sprintf("/sellprice/%s", coinType), url.Values{}, c)
}

// GetLatestSellPriceMarket returns the latest sell price for a specific coin and market pair.
func (c *Client) GetLatestSellPriceMarket(ctx context.Context, coinType, marketType string) (*LatestPriceResponse, error) {
	return decodePublicResponse[LatestPriceResponse](ctx, fmt.Sprintf("/sellprice/%s/%s", coinType, marketType), url.Values{}, c)
}

// GetOpenOrders returns open buy and sell orders for a specific coin.
func (c *Client) GetOpenOrders(ctx context.Context, coinType string) (*OpenOrdersResponse, error) {
	return decodePublicResponse[OpenOrdersResponse](ctx, fmt.Sprintf("/orders/open/%s", coinType), url.Values{}, c)
}

// GetOpenOrdersMarket returns open buy and sell orders for a specific coin and market pair.
func (c *Client) GetOpenOrdersMarket(ctx context.Context, coinType, marketType string) (*OpenOrdersMarketResponse, error) {
	return decodePublicResponse[OpenOrdersMarketResponse](ctx, fmt.Sprintf("/orders/open/%s/%s", coinType, marketType), url.Values{}, c)
}

// GetCompletedOrders returns completed buy and sell orders for a specific coin.
func (c *Client) GetCompletedOrders(ctx context.Context, coinType string) (*CompletedOrdersResponse, error) {
	return decodePublicResponse[CompletedOrdersResponse](ctx, fmt.Sprintf("/orders/completed/%s", coinType), url.Values{}, c)
}

// GetCompletedOrdersMarket returns completed buy and sell orders for a specific coin and market pair.
func (c *Client) GetCompletedOrdersMarket(ctx context.Context, coinType, marketType string) (*CompletedOrdersMarketResponse, error) {
	return decodePublicResponse[CompletedOrdersMarketResponse](ctx, fmt.Sprintf("/orders/completed/%s/%s", coinType, marketType), url.Values{}, c)
}

// GetCompletedOrdersSummary returns a summary of completed orders for a specific coin.
func (c *Client) GetCompletedOrdersSummary(ctx context.Context, coinType string) (*CompletedOrdersSummaryResponse, error) {
	return decodePublicResponse[CompletedOrdersSummaryResponse](ctx, fmt.Sprintf("/orders/summary/completed/%s", coinType), url.Values{}, c)
}

// GetCompletedOrdersSummaryMarket returns a summary of completed orders for a specific coin and market pair.
func (c *Client) GetCompletedOrdersSummaryMarket(ctx context.Context, coinType, marketType string) (*CompletedOrdersSummaryMarketResponse, error) {
	return decodePublicResponse[CompletedOrdersSummaryMarketResponse](ctx, fmt.Sprintf("/orders/summary/completed/%s/%s", coinType, marketType), url.Values{}, c)
}

//
// Private API Methods (POST)
//

// CheckStatus checks if the API connection is working correctly.
func (c *Client) CheckStatus(ctx context.Context, apiKey, secretKey string) (*StatusResponse, error) {
	return decodeResponse[StatusResponse](ctx, "/status", url.Values{}, apiKey, secretKey, c)
}

// GetDepositAddress returns the deposit address for a specific coin type.
func (c *Client) GetDepositAddress(ctx context.Context, apiKey, secretKey string, coinType string) (*DepositAddressResponse, error) {
	return decodeResponse[DepositAddressResponse](ctx, "/my/coin/deposit", url.Values{"cointype": {coinType}}, apiKey, secretKey, c)
}

// GetBuyNowCoinList returns the list of coins available for immediate buy (BuyNow).
func (c *Client) GetBuyNowCoinList(ctx context.Context, apiKey, secretKey string) (*CoinListResponse, error) {
	return decodeResponse[CoinListResponse](ctx, "/my/buy/now/coinlist", url.Values{}, apiKey, secretKey, c)
}

// GetSellNowCoinList returns the list of coins available for immediate sell (SellNow).
func (c *Client) GetSellNowCoinList(ctx context.Context, apiKey, secretKey string) (*CoinListResponse, error) {
	return decodeResponse[CoinListResponse](ctx, "/my/sell/now/coinlist", url.Values{}, apiKey, secretKey, c)
}

// GetBuyNowQuote returns a quote for immediate purchase of a coin.
func (c *Client) GetBuyNowQuote(ctx context.Context, apiKey, secretKey string, coinType string, amount float64, amountType string) (*QuoteResponse, error) {
	return decodeResponse[QuoteResponse](ctx, "/quote/buy/now", url.Values{
		"cointype": {coinType}, "amount": {fmt.Sprintf("%.8f", amount)}, "amounttype": {amountType},
	}, apiKey, secretKey, c)
}

// GetSellNowQuote returns a quote for immediate sale of a coin.
func (c *Client) GetSellNowQuote(ctx context.Context, apiKey, secretKey string, coinType string, amount float64, amountType string) (*QuoteResponse, error) {
	return decodeResponse[QuoteResponse](ctx, "/quote/sell/now", url.Values{
		"cointype": {coinType}, "amount": {fmt.Sprintf("%.8f", amount)}, "amounttype": {amountType},
	}, apiKey, secretKey, c)
}

// GetSwapNowQuote returns a quote for immediate swap of one coin for another.
func (c *Client) GetSwapNowQuote(ctx context.Context, apiKey, secretKey string, coinTypeSell, coinTypeBuy string, amount float64) (*QuoteResponse, error) {
	return decodeResponse[QuoteResponse](ctx, "/quote/swap/now", url.Values{
		"cointypesell": {coinTypeSell}, "cointypebuy": {coinTypeBuy}, "amount": {fmt.Sprintf("%.8f", amount)},
	}, apiKey, secretKey, c)
}

// PlaceMarketBuy places a buy order at a specific market price.
func (c *Client) PlaceMarketBuy(ctx context.Context, apiKey, secretKey string, coinType string, amount, rate float64, marketType string) (*OrderResponse, error) {
	p := url.Values{"cointype": {coinType}, "amount": {fmt.Sprintf("%.8f", amount)}, "rate": {fmt.Sprintf("%.8f", rate)}}
	if marketType != "" {
		p.Set("markettype", marketType)
	} else {
		p.Set("markettype", "AUD")
	}
	return decodeResponse[OrderResponse](ctx, "/my/buy", p, apiKey, secretKey, c)
}

// EditOpenMarketBuy edits an open market buy order with a new rate.
func (c *Client) EditOpenMarketBuy(ctx context.Context, apiKey, secretKey string, coinType, orderID string, currentRate, newRate float64) (*EditOrderResponse, error) {
	return decodeResponse[EditOrderResponse](ctx, "/my/buy/edit", url.Values{
		"cointype": {coinType}, "id": {orderID}, "rate": {fmt.Sprintf("%.8f", currentRate)}, "newrate": {fmt.Sprintf("%.8f", newRate)},
	}, apiKey, secretKey, c)
}

// PlaceBuyNow places an immediate buy order with optional configuration.
func (c *Client) PlaceBuyNow(ctx context.Context, apiKey, secretKey string, coinType string, amountType string, amount float64, opts ...BuyNowOpt) (*BuyNowResponse, error) {
	p := url.Values{"cointype": {coinType}, "amounttype": {amountType}, "amount": {fmt.Sprintf("%.8f", amount)}}
	for _, o := range opts {
		o(p)
	}
	return decodeResponse[BuyNowResponse](ctx, "/my/buy/now", p, apiKey, secretKey, c)
}

// PlaceMarketSell places a sell order at a specific market price.
func (c *Client) PlaceMarketSell(ctx context.Context, apiKey, secretKey string, coinType string, amount, rate float64, marketType string) (*OrderResponse, error) {
	p := url.Values{"cointype": {coinType}, "amount": {fmt.Sprintf("%.8f", amount)}, "rate": {fmt.Sprintf("%.8f", rate)}}
	if marketType != "" {
		p.Set("markettype", marketType)
	} else {
		p.Set("markettype", "AUD")
	}
	return decodeResponse[OrderResponse](ctx, "/my/sell", p, apiKey, secretKey, c)
}

// EditOpenMarketSell edits an open market sell order with a new rate.
func (c *Client) EditOpenMarketSell(ctx context.Context, apiKey, secretKey string, coinType, orderID string, currentRate, newRate float64) (*EditOrderResponse, error) {
	return decodeResponse[EditOrderResponse](ctx, "/my/sell/edit", url.Values{
		"cointype": {coinType}, "id": {orderID}, "rate": {fmt.Sprintf("%.8f", currentRate)}, "newrate": {fmt.Sprintf("%.8f", newRate)},
	}, apiKey, secretKey, c)
}

// PlaceSellNow places an immediate sell order with optional configuration.
func (c *Client) PlaceSellNow(ctx context.Context, apiKey, secretKey string, coinType string, amountType string, amount float64, opts ...SellNowOpt) (*SellNowResponse, error) {
	p := url.Values{"cointype": {coinType}, "amounttype": {amountType}, "amount": {fmt.Sprintf("%.8f", amount)}}
	for _, o := range opts {
		o(p)
	}
	return decodeResponse[SellNowResponse](ctx, "/my/sell/now", p, apiKey, secretKey, c)
}

// PlaceSwapNow places an immediate swap order of one coin for another with optional configuration.
func (c *Client) PlaceSwapNow(ctx context.Context, apiKey, secretKey string, coinTypeSell, coinTypeBuy string, amount float64, opts ...SwapNowOpt) (*SwapNowResponse, error) {
	p := url.Values{"cointypesell": {coinTypeSell}, "cointypebuy": {coinTypeBuy}, "amount": {fmt.Sprintf("%.8f", amount)}}
	for _, o := range opts {
		o(p)
	}
	return decodeResponse[SwapNowResponse](ctx, "/my/swap/now", p, apiKey, secretKey, c)
}

// CancelBuyOrder cancels a specific open buy order.
func (c *Client) CancelBuyOrder(ctx context.Context, apiKey, secretKey string, orderID string) (*CancelResponse, error) {
	return decodeResponse[CancelResponse](ctx, "/my/buy/cancel", url.Values{"id": {orderID}}, apiKey, secretKey, c)
}

// CancelAllBuyOrders cancels all open buy orders, optionally filtered by coin.
func (c *Client) CancelAllBuyOrders(ctx context.Context, apiKey, secretKey string, coin string) (*CancelResponse, error) {
	p := url.Values{}
	if coin != "" {
		p.Set("coin", coin)
	}
	return decodeResponse[CancelResponse](ctx, "/my/buy/cancel/all", p, apiKey, secretKey, c)
}

// CancelSellOrder cancels a specific open sell order.
func (c *Client) CancelSellOrder(ctx context.Context, apiKey, secretKey string, orderID string) (*CancelResponse, error) {
	return decodeResponse[CancelResponse](ctx, "/my/sell/cancel", url.Values{"id": {orderID}}, apiKey, secretKey, c)
}

// CancelAllSellOrders cancels all open sell orders, optionally filtered by coin.
func (c *Client) CancelAllSellOrders(ctx context.Context, apiKey, secretKey string, coin string) (*CancelResponse, error) {
	p := url.Values{}
	if coin != "" {
		p.Set("coin", coin)
	}
	return decodeResponse[CancelResponse](ctx, "/my/sell/cancel/all", p, apiKey, secretKey, c)
}

// GetWithdrawDetails returns withdrawal details and network fees for a specific coin.
func (c *Client) GetWithdrawDetails(ctx context.Context, apiKey, secretKey string, coinType string) (*WithdrawDetailsResponse, error) {
	return decodeResponse[WithdrawDetailsResponse](ctx, "/my/coin/withdraw/senddetails", url.Values{"cointype": {coinType}}, apiKey, secretKey, c)
}

// SendWithdraw sends a cryptocurrency withdrawal to the specified address.
func (c *Client) SendWithdraw(ctx context.Context, apiKey, secretKey string, coinType string, amount, address string, opts ...WithdrawOpt) (*WithdrawResponse, error) {
	p := url.Values{"cointype": {coinType}, "amount": {amount}, "address": {address}}
	for _, o := range opts {
		o(p)
	}
	return decodeResponse[WithdrawResponse](ctx, "/my/coin/withdraw/send", p, apiKey, secretKey, c)
}

//
// Read-Only API Methods (POST)
//

// ROCheckStatus checks if the read-only API connection is working correctly.
func (c *Client) ROCheckStatus(ctx context.Context, apiKey, secretKey string) (*StatusResponse, error) {
	return decodeResponse[StatusResponse](ctx, "/status", url.Values{}, apiKey, secretKey, c)
}

// ROGetOpenMarketOrders returns open buy and sell market orders, optionally filtered by market.
func (c *Client) ROGetOpenMarketOrders(ctx context.Context, apiKey, secretKey string, coinType, marketType string) (*MarketOrdersResponse, error) {
	p := url.Values{"cointype": {coinType}}
	if marketType != "" {
		p.Set("markettype", marketType)
	}
	return decodeResponse[MarketOrdersResponse](ctx, "/orders/market/open", p, apiKey, secretKey, c)
}

// ROGetCompletedMarketOrders returns completed market orders with optional date range and limit filters.
func (c *Client) ROGetCompletedMarketOrders(ctx context.Context, apiKey, secretKey string, coinType, marketType, startDate, endDate string, limit int) (*MarketOrdersResponse, error) {
	p := url.Values{"cointype": {coinType}}
	if marketType != "" {
		p.Set("markettype", marketType)
	}
	if startDate != "" {
		p.Set("startdate", startDate)
	}
	if endDate != "" {
		p.Set("enddate", endDate)
	}
	if limit > 0 {
		p.Set("limit", fmt.Sprintf("%d", limit))
	}
	return decodeResponse[MarketOrdersResponse](ctx, "/orders/market/completed", p, apiKey, secretKey, c)
}

// ROGetBalances returns account balances for all coins.
func (c *Client) ROGetBalances(ctx context.Context, apiKey, secretKey string) (*BalancesResponse, error) {
	return decodeResponse[BalancesResponse](ctx, "/my/balances", url.Values{}, apiKey, secretKey, c)
}

// ROGetBalance returns the balance for a specific coin.
func (c *Client) ROGetBalance(ctx context.Context, apiKey, secretKey string, coinType, available string) (*BalanceResponse, error) {
	return decodeResponse[BalanceResponse](ctx, fmt.Sprintf("/my/balance/%s", coinType), url.Values{"available": {available}}, apiKey, secretKey, c)
}

// ROGetMyOpenMarketOrders returns user's open market orders, optionally filtered by coin and market.
func (c *Client) ROGetMyOpenMarketOrders(ctx context.Context, apiKey, secretKey string, coinType, marketType string) (*MarketOrdersResponse, error) {
	p := url.Values{}
	if coinType != "" {
		p.Set("cointype", coinType)
	}
	if marketType != "" {
		p.Set("markettype", marketType)
	}
	return decodeResponse[MarketOrdersResponse](ctx, "/my/orders/market/open", p, apiKey, secretKey, c)
}

// ROGetMyOpenLimitOrders returns user's open limit orders for a specific coin.
func (c *Client) ROGetMyOpenLimitOrders(ctx context.Context, apiKey, secretKey string, coinType string) (*OpenLimitOrdersResponse, error) {
	return decodeResponse[OpenLimitOrdersResponse](ctx, "/my/orders/limit/open", url.Values{"cointype": {coinType}}, apiKey, secretKey, c)
}

// ROGetOrderHistory returns order history with optional date range and limit filters.
func (c *Client) ROGetOrderHistory(ctx context.Context, apiKey, secretKey string, coinType, marketType, startDate, endDate string, limit int) (*OrderHistoryResponse, error) {
	p := url.Values{}
	if coinType != "" {
		p.Set("cointype", coinType)
	}
	if marketType != "" {
		p.Set("markettype", marketType)
	}
	if startDate != "" {
		p.Set("startdate", startDate)
	}
	if endDate != "" {
		p.Set("enddate", endDate)
	}
	if limit > 0 {
		p.Set("limit", fmt.Sprintf("%d", limit))
	}
	return decodeResponse[OrderHistoryResponse](ctx, "/my/orders/completed", p, apiKey, secretKey, c)
}

// ROGetMarketOrderHistory returns market order history with optional date range and limit filters.
func (c *Client) ROGetMarketOrderHistory(ctx context.Context, apiKey, secretKey string, coinType, marketType, startDate, endDate string, limit int) (*OrderHistoryResponse, error) {
	p := url.Values{}
	if coinType != "" {
		p.Set("cointype", coinType)
	}
	if marketType != "" {
		p.Set("markettype", marketType)
	}
	if startDate != "" {
		p.Set("startdate", startDate)
	}
	if endDate != "" {
		p.Set("enddate", endDate)
	}
	if limit > 0 {
		p.Set("limit", fmt.Sprintf("%d", limit))
	}
	return decodeResponse[OrderHistoryResponse](ctx, "/my/orders/market/completed", p, apiKey, secretKey, c)
}

// ROGetSendReceiveHistory returns send and receive transaction history with optional date range filter.
func (c *Client) ROGetSendReceiveHistory(ctx context.Context, apiKey, secretKey string, startDate, endDate string) (*SendReceiveHistoryResponse, error) {
	p := url.Values{}
	if startDate != "" {
		p.Set("startdate", startDate)
	}
	if endDate != "" {
		p.Set("enddate", endDate)
	}
	return decodeResponse[SendReceiveHistoryResponse](ctx, "/my/sendreceive", p, apiKey, secretKey, c)
}

// ROGetDepositHistory returns deposit history with optional date range filter.
func (c *Client) ROGetDepositHistory(ctx context.Context, apiKey, secretKey string, startDate, endDate string) (*DepositHistoryResponse, error) {
	p := url.Values{}
	if startDate != "" {
		p.Set("startdate", startDate)
	}
	if endDate != "" {
		p.Set("enddate", endDate)
	}
	return decodeResponse[DepositHistoryResponse](ctx, "/my/deposits", p, apiKey, secretKey, c)
}

// ROGetWithdrawalHistory returns withdrawal history with optional date range filter.
func (c *Client) ROGetWithdrawalHistory(ctx context.Context, apiKey, secretKey string, startDate, endDate string) (*WithdrawalHistoryResponse, error) {
	p := url.Values{}
	if startDate != "" {
		p.Set("startdate", startDate)
	}
	if endDate != "" {
		p.Set("enddate", endDate)
	}
	return decodeResponse[WithdrawalHistoryResponse](ctx, "/my/withdrawals", p, apiKey, secretKey, c)
}

// ROGetAffiliatePayments returns affiliate payment history.
func (c *Client) ROGetAffiliatePayments(ctx context.Context, apiKey, secretKey string) (*PaymentResponse, error) {
	return decodeResponse[PaymentResponse](ctx, "/my/affiliatepayments", url.Values{}, apiKey, secretKey, c)
}

// ROGetReferralPayments returns referral payment history.
func (c *Client) ROGetReferralPayments(ctx context.Context, apiKey, secretKey string) (*PaymentResponse, error) {
	return decodeResponse[PaymentResponse](ctx, "/my/referralpayments", url.Values{}, apiKey, secretKey, c)
}

//
// Functional Options (Optional Configuration)
//

// BuyNowOpt is a functional option for configuring BuyNow orders.
type BuyNowOpt func(url.Values)

// WithRate sets the rate/price limit for a BuyNow order.
func WithRate(rate float64) BuyNowOpt {
	return func(p url.Values) { p.Set("rate", fmt.Sprintf("%.8f", rate)) }
}

// WithThreshold sets the acceptable price threshold for a BuyNow order.
func WithThreshold(threshold float64) BuyNowOpt {
	return func(p url.Values) { p.Set("threshold", fmt.Sprintf("%.8f", threshold)) }
}

// WithDirection sets the price direction preference for a BuyNow order.
func WithDirection(dir string) BuyNowOpt {
	return func(p url.Values) { p.Set("direction", dir) }
}

// SellNowOpt is a functional option for configuring SellNow orders.
type SellNowOpt func(url.Values)

// WithSellRate sets the rate/price limit for a SellNow order.
func WithSellRate(rate float64) SellNowOpt {
	return func(p url.Values) { p.Set("rate", fmt.Sprintf("%.8f", rate)) }
}

// WithSellThreshold sets the acceptable price threshold for a SellNow order.
func WithSellThreshold(threshold float64) SellNowOpt {
	return func(p url.Values) { p.Set("threshold", fmt.Sprintf("%.8f", threshold)) }
}

// WithSellDirection sets the price direction preference for a SellNow order.
func WithSellDirection(dir string) SellNowOpt {
	return func(p url.Values) { p.Set("direction", dir) }
}

// SwapNowOpt is a functional option for configuring SwapNow orders.
type SwapNowOpt func(url.Values)

// WithSwapRate sets the rate/price limit for a SwapNow order.
func WithSwapRate(rate float64) SwapNowOpt {
	return func(p url.Values) { p.Set("rate", fmt.Sprintf("%.8f", rate)) }
}

// WithSwapThreshold sets the acceptable price threshold for a SwapNow order.
func WithSwapThreshold(threshold float64) SwapNowOpt {
	return func(p url.Values) { p.Set("threshold", fmt.Sprintf("%.8f", threshold)) }
}

// WithSwapDirection sets the price direction preference for a SwapNow order.
func WithSwapDirection(dir string) SwapNowOpt {
	return func(p url.Values) { p.Set("direction", dir) }
}

// WithdrawOpt is a functional option for configuring withdrawal requests.
type WithdrawOpt func(url.Values)

// WithEmailConfirm sets whether email confirmation is required for the withdrawal.
func WithEmailConfirm(emailConfirm string) WithdrawOpt {
	return func(p url.Values) { p.Set("emailconfirm", emailConfirm) }
}

// WithNetwork sets the network to use for the withdrawal.
func WithNetwork(network string) WithdrawOpt {
	return func(p url.Values) { p.Set("network", network) }
}

// WithPaymentID sets the payment ID for the withdrawal.
func WithPaymentID(paymentID string) WithdrawOpt {
	return func(p url.Values) { p.Set("paymentid", paymentID) }
}
