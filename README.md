# CoinSpot API Go Client Guide

This guide explains how to configure, instance, and use the CoinSpot API client. The client is designed to be thread-safe and handles rate limiting, retries, and authentication internally.

## 1. Configuration and Instancing

The `Client` is initialized using a `Config` struct. You only need to create **one** main client instance for your application; you can then derive specific clients for Public, Trade, or Read-Only access from that single instance.

### Basic Setup

```
package main

import (
    "context"
    "log"
    "time"
    "github.com/gregdavisd/coinspot_go/coinspot"
)

func main() {
    ctx := context.Background()

    // 1. Define configuration
    cfg := coinspot.Config{
        BaseURL:         "www.coinspot.com.au",
        RateLimitPerMin: 1000, // Matches CoinSpot's hard cap
        RetryConfig: coinspot.RetryConfig{
            MaxRetries: 3,
            BaseDelay:  500 * time.Millisecond,
            MaxDelay:   10 * time.Second,
        },
    }

    // 2. Create the master client
    client := coinspot.NewClient(cfg)

    // Now you can derive specific clients for different tiers
}
```

## 2. Using the Public API

The Public API is for market data. It requires **no authentication** and uses `GET` requests.

```
func publicExample(ctx context.Context, client *coinspot.Client) {
    // Derive the public client
    pub := client.PublicClient()

    // Get all latest prices
    prices, err := pub.GetLatestPrices(ctx)
    if err != nil {
        log.Fatalf("Error fetching prices: %v", err)
    }
    log.Printf("BTC Bid: %v", prices.Prices["btc"].Bid)

    // Get a specific buy price for a coin
    buyPrice, err := pub.GetLatestBuyPrice(ctx, "BTC")
    if err != nil {
        log.Printf("Error: %v", err)
    }
    log.Printf("Current BTC Buy Rate: %v", buyPrice.Rate)
}
```

## 3. Using the Full Access API (Trading)

The Trading API is for executing orders and withdrawals. It requires a **Standard API Key** and uses `POST` requests.

**Note:** API keys are passed directly into the method calls, not stored in the client, allowing a single client to manage multiple accounts.

```
func tradeExample(ctx context.Context, client *coinspot.Client) {
    trade := client.TradeClient()
    apiKey := "your_api_key"
    secretKey := "your_secret_key"

    // Example 1: Place a Market Buy Order
    order, err := trade.PlaceMarketBuy(ctx, apiKey, secretKey, "BTC", 0.001, 60000.00, "AUD")
    if err != nil {
        log.Printf("Order failed: %v", err)
        return
    }
    log.Printf("Order placed! ID: %s", order.ID)

    // Example 2: Place a "Buy Now" order with Functional Options
    // Using WithRate and WithThreshold for price protection
    buyNow, err := trade.PlaceBuyNow(
        ctx,
        apiKey,
        secretKey,
        "BTC",
        "aud",
        100.00,
        coinspot.WithRate(60000.00),
        coinspot.WithThreshold(1.0), // 1% threshold
    )
    if err != nil {
        log.Printf("BuyNow failed: %v", err)
    }
    log.Printf("Bought amount: %v", buyNow.Amount)
}
```

## 4. Using the Read-Only API

The Read-Only API provides account balances and history. It requires a **Read-Only API Key**.

```
func readOnlyExample(ctx context.Context, client *coinspot.Client) {
    ro := client.ReadOnlyClient()
    apiKey := "your_ro_api_key"
    secretKey := "your_ro_secret_key"

    // Get all account balances
    balances, err := ro.ROGetBalances(ctx, apiKey, secretKey)
    if err != nil {
        log.Printf("Error fetching balances: %v", err)
        return
    }

    for coin, bal := range balances.Balances {
        log.Printf("Coin: %s | Balance: %v | AUD Value: %v", coin, bal.Balance, bal.AudBalance)
    }
}
```

## Summary Table for Developers

| Tier          | Client Method       | Auth Required  | HTTP Method | Common Use Case                     |
| ------------- | ------------------- | -------------- | ----------- | ----------------------------------- |
| **Public**    | `.PublicClient()`   | No             | `GET`       | Market prices, Open order books     |
| **Full**      | `.TradeClient()`    | Yes (Full Key) | `POST`      | Market Buy/Sell, Swaps, Withdrawals |
| **Read-Only** | `.ReadOnlyClient()` | Yes (RO Key)   | `POST`      | Account Balances, Trade History     |

### Key Technical Reminders

1. **Thread Safety:** You can safely call any of the methods above from multiple goroutines. The client manages the `nonce` and `rate limiting` using internal mutexes.

2. **Rate Limits:** If you set `RateLimitPerMin: 1000`, the client will automatically pause execution (sleep) between requests to ensure you never hit the CoinSpot server-side limit.

3. **Error Handling:** Always check the `error` return value. The client automatically converts API `{"status":"error"}` responses into Go errors.

4. **Contexts:** Always pass a valid `context.Context`. This allows you to set timeouts for your API calls (e.g., using `context.WithTimeout`).

### 1. API Implementation Accuracy

The code accurately maps the documented endpoints and requirements:

- **Architecture Tiers:** The implementation creates three distinct client configurations (`PublicClient`, `ReadOnlyClient`, and `TradeClient`) matching the documentation's three tiers (`/pubapi/v2`, `/api/v2/ro`, and `/api/v2`).

- **HTTP Methods:**
  - The **Public API** correctly uses `GET` requests via `decodePublicResponse`.

  - The **Trade and Read-Only APIs** correctly use `POST` requests via `decodeResponse`.

- **Authentication:**
  - **Headers:** `doRequest` correctly implements the `key` and `sign` headers.

  - **Signature:** The `signData` function correctly uses **HMAC-SHA512** on the form-encoded POST body using the secret key.

  - **Nonce:** The `nextNonce()` function ensures a strictly increasing integer is added to every POST request.

- **Data Precision:** The code respects the API's precision limits (e.g., using `fmt.Sprintf("%.8f", amount)`) to ensure crypto values do not exceed 8 decimal places.

- **Type Safety:** The use of `stringAsFloat64` is a robust addition; it handles cases where the API might return numeric values as strings, preventing unmarshaling errors.

### 2. Thread Safety

The implementation is fully thread-safe:

- **Nonce Generation:** The global `nonce` is protected by `nonceMu sync.Mutex`, ensuring that multiple goroutines cannot generate the same nonce or cause a race condition.

- **Rate Limiting:** The `rateLimiter` uses a `sync.Mutex` to protect the `nextAllowed` timestamp, ensuring that concurrent requests across different threads are sequenced correctly.

- **HTTP Client:** The use of `http.Client` is thread-safe by design in Go.

### 3. Rate Limiting Across Threads

The rate limiting is implemented globally and effectively:

- **Global Pacing:** The `rateLimiter` calculates a fixed interval based on the `RateLimitPerMin` (e.g., 60s/1000≈60ms60s/1000≈60ms per request).

- **Cross-Thread Enforcement:** Because the `Client` holds a pointer to the `rateLimiter`, and the `wait()` method is called inside `doRequest` before every single API call, all threads sharing that client instance are subject to the same pacing.

- **Jitter:** The implementation includes 0–10%0–10% jitter in the `wait` function, which is a best practice to prevent "thundering herd" issues where multiple threads wake up at the exact same millisecond.

### 4. Additional Production-Grade Features

The implementation goes beyond the basic documentation to provide higher reliability:

- **Exponential Backoff:** It implements a retry mechanism for `429` (Too Many Requests) and `5xx` (Server Error) status codes, which is critical for stability in crypto trading.

- **Context Support:** Every method accepts a `context.Context`, allowing the caller to cancel requests or implement timeouts.

- **Functional Options:** The use of `BuyNowOpt`, `SellNowOpt`, etc., allows for a clean, extensible API for optional parameters like `threshold` and `direction`.

### Summary Table

|    Requirement    |       Documented        |         Implemented in `api.go`         |   Status    |
| :---------------: | :---------------------: | :-------------------------------------: | :---------: |
|  **Public API**   |     GET /pubapi/v2      | `PublicClient` → `decodePublicResponse` |  ✅ Match   |
|   **Trade API**   |      POST /api/v2       |    `TradeClient` → `decodeResponse`     |  ✅ Match   |
|    **RO API**     |     POST /api/v2/ro     |   `ReadOnlyClient` → `decodeResponse`   |  ✅ Match   |
|     **Auth**      |    HMAC-SHA512 + Key    |        `signData` → `doRequest`         |  ✅ Match   |
|     **Nonce**     |   Strictly Increasing   |        `nonceMu` + `nextNonce()`        |  ✅ Match   |
|  **Rate Limit**   |      1,000 req/min      |      `rateLimiter` + `sync.Mutex`       |  ✅ Match   |
| **Thread Safety** | Not specified (implied) |    Mutexes on Nonce and Rate Limiter    | ✅ Verified |
