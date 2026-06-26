package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gregdavisd/coinspot_go/coinspot"
)

// Config holds the robot configuration and tuning parameters.
type Config struct {
	BaseURL      string        // e.g., "www.coinspot.com.au"
	APIKey       string        // Full Access API Key
	SecretKey    string        // Full Access API Secret
	CoinType     string        // e.g., "BTC"
	GridStepPct  float64       // Grid spacing in percentage (e.g., 1.0 for 1%)
	TradeSizePct float64       // Percentage of current balance to trade per grid level (e.g., 10.0)
	PollInterval time.Duration // How often to poll for order status (default: 1m)
}

// GridRobot implements a dynamic 1% grid trading bot for BTC/AUD.
type GridRobot struct {
	cfg        Config
	client     *coinspot.Client // Trade API
	roClient   *coinspot.Client // Read-Only API
	pubClient  *coinspot.Client // Public API
	lastBuyID  string
	lastSellID string
}

// NewGridRobot initializes the grid trading robot.
func NewGridRobot(cfg Config) *GridRobot {

	// make sure baseurl has "demo" in it	for testing, otherwise abort
	if !strings.Contains(cfg.BaseURL, "demo") {
		log.Fatal("❌ BaseURL must contain 'demo' for safety. Aborting.")
	}

	// Initialize clients with sensible rate limits
	limiter := int64(990) // Leave buffer for other API calls

	tradeClient := coinspot.NewClient(coinspot.Config{
		BaseURL:         cfg.BaseURL,
		RateLimitPerMin: limiter,
	}).TradeClient()

	roClient := coinspot.NewClient(coinspot.Config{
		BaseURL:         cfg.BaseURL,
		RateLimitPerMin: limiter,
	}).ReadOnlyClient()

	pubClient := coinspot.NewClient(coinspot.Config{
		BaseURL:         cfg.BaseURL,
		RateLimitPerMin: limiter,
	}).PublicClient()

	// Apply safe defaults
	if cfg.GridStepPct <= 0 {
		cfg.GridStepPct = 1.0
	}
	if cfg.TradeSizePct <= 0 {
		cfg.TradeSizePct = 10.0 // 10% of balance per grid step is standard practice
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 1 * time.Minute
	}

	return &GridRobot{
		cfg:       cfg,
		client:    tradeClient,
		roClient:  roClient,
		pubClient: pubClient,
	}
}

// Run starts the grid robot loop. Blocks until context is cancelled.
func (r *GridRobot) Run(ctx context.Context) error {
	log.Println("🤖 Grid Robot initialized. Starting main loop...")
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("🛑 Grid Robot stopped by context cancellation.")
			return nil
		case <-ticker.C:
			if err := r.runCycle(ctx); err != nil {
				log.Printf("⚠️ Cycle error: %v\n", err)
				// Continue loop on transient errors; only exit on fatal context cancellation
			}
		}
	}
}

// runCycle executes one iteration of the grid logic.
func (r *GridRobot) runCycle(ctx context.Context) error {
	// 1. Fetch balances
	btcBal, audBal, err := r.getBalances(ctx)
	if err != nil {
		return fmt.Errorf("balance fetch failed: %w", err)
	}

	// 2. Initial deposit if no BTC
	if btcBal < 0.00000001 {
		log.Println("📥 No BTC balance detected. Placing instant Buy Now order...")
		if err := r.placeInitialBuy(ctx, audBal); err != nil {
			return fmt.Errorf("initial deposit failed: %w", err)
		}
		// Allow network propagation & balance update
		time.Sleep(3 * time.Second)
		btcBal, audBal, err = r.getBalances(ctx)
		if err != nil || btcBal < 0.00000001 {
			return fmt.Errorf("initial deposit verification failed")
		}
		log.Printf("✅ Initial deposit complete. BTC: %.8f, AUD: %.2f\n", btcBal, audBal)
		// 5. Setup new grid
		if err := r.setupGrid(ctx, btcBal, audBal); err != nil {
			return fmt.Errorf("grid setup failed: %w", err)
		}
	}

	// 3. Check if previous grid orders were filled
	if r.lastSellID != "" || r.lastBuyID != "" {
		if r.ordersWereFilled(ctx) {
			log.Println("📊 Grid order hit! Recalculating grid immediately...")

			// 4. Cancel any lingering grid orders (enforce max 1 buy & 1 sell)
			if err := r.cancelGridOrders(ctx); err != nil {
				log.Printf("⚠️ Warning: failed to cancel previous orders: %v\n", err)
			}

			// 5. Setup new grid
			if err := r.setupGrid(ctx, btcBal, audBal); err != nil {
				return fmt.Errorf("grid shift failed: %w", err)
			}
		}
	}

	log.Println("✅ Grid refreshed. Waiting for next cycle...")
	return nil
}

// getBalances fetches available BTC and AUD balances.
func (r *GridRobot) getBalances(ctx context.Context) (btcBal, audBal float64, err error) {
	btcResp, err := r.roClient.ROGetBalance(ctx, r.cfg.APIKey, r.cfg.SecretKey, r.cfg.CoinType, "no")
	if err != nil {
		return 0, 0, fmt.Errorf("BTC balance fetch: %w", err)
	}
	btcBal = float64(btcResp.Balance[r.cfg.CoinType].Balance)

	audResp, err := r.roClient.ROGetBalance(ctx, r.cfg.APIKey, r.cfg.SecretKey, "AUD", "no")
	if err != nil {
		return 0, 0, fmt.Errorf("AUD balance fetch: %w", err)
	}
	audBal = float64(audResp.Balance["AUD"].Balance)

	return btcBal, audBal, nil
}

// placeInitialBuy uses a "Buy Now" order to seed the grid with all available AUD.
func (r *GridRobot) placeInitialBuy(ctx context.Context, audAmount float64) error {
	resp, err := r.client.PlaceBuyNow(ctx, r.cfg.APIKey, r.cfg.SecretKey, r.cfg.CoinType, "AUD", audAmount)
	if err != nil {
		return fmt.Errorf("buy now execution: %w", err)
	}
	log.Printf("📥 Buy Now executed: %.8f %s for %.2f AUD\n", resp.Amount, r.cfg.CoinType, audAmount)
	return nil
}

// cancelGridOrders removes the last placed buy/sell orders to enforce the 1x1 constraint.
func (r *GridRobot) cancelGridOrders(ctx context.Context) error {
	if r.lastSellID != "" {
		_, err := r.client.CancelSellOrder(ctx, r.cfg.APIKey, r.cfg.SecretKey, r.lastSellID)
		if err != nil {
			log.Printf("⚠️ Cancel sell %s failed: %v", r.lastSellID, err)
		} else {
			log.Printf("🗑️ Cancelled sell order %s", r.lastSellID)
			r.lastSellID = ""
		}
	}
	if r.lastBuyID != "" {
		_, err := r.client.CancelBuyOrder(ctx, r.cfg.APIKey, r.cfg.SecretKey, r.lastBuyID)
		if err != nil {
			log.Printf("⚠️ Cancel buy %s failed: %v", r.lastBuyID, err)
		} else {
			log.Printf("🗑️ Cancelled buy order %s", r.lastBuyID)
			r.lastBuyID = ""
		}
	}
	return nil
}

// setupGrid calculates prices/amounts and places the new grid orders.
func (r *GridRobot) setupGrid(ctx context.Context, btcBal, audBal float64) error {
	// Get live market price
	priceResp, err := r.pubClient.GetLatestCoinPrices(ctx, r.cfg.CoinType)
	if err != nil {
		return fmt.Errorf("price fetch: %w", err)
	}
	currentPrice := float64(priceResp.Prices.Last)
	if currentPrice <= 0 {
		return fmt.Errorf("invalid market price: %f", currentPrice)
	}

	// Calculate grid levels
	step := r.cfg.GridStepPct / 100.0
	buyPrice := currentPrice * (1.0 - step)
	sellPrice := currentPrice * (1.0 + step)

	// Calculate trade amounts based on current balance
	tradeSize := r.cfg.TradeSizePct / 100.0
	sellAmount := btcBal * tradeSize
	buyAmountAUD := audBal * tradeSize
	buyAmountBTC := buyAmountAUD / buyPrice

	// Enforce minimum practical amounts (CoinSpot typically requires > 0.0001)
	if sellAmount < 0.0001 {
		sellAmount = 0.0001
	}
	if buyAmountBTC < 0.0001 {
		buyAmountBTC = 0.0001
	}

	// Place Sell Order (Limit at +1%)
	sellResp, err := r.client.PlaceMarketSell(ctx, r.cfg.APIKey, r.cfg.SecretKey, r.cfg.CoinType, sellAmount, sellPrice, "AUD")
	if err != nil {
		return fmt.Errorf("place sell order: %w", err)
	}
	r.lastSellID = sellResp.ID
	log.Printf("📤 Sell Grid: %.8f BTC @ %.2f AUD (ID: %s)", sellAmount, sellPrice, r.lastSellID)

	// Place Buy Order (Limit at -1%)
	buyResp, err := r.client.PlaceMarketBuy(ctx, r.cfg.APIKey, r.cfg.SecretKey, r.cfg.CoinType, buyAmountBTC, buyPrice, "AUD")
	if err != nil {
		return fmt.Errorf("place buy order: %w", err)
	}
	r.lastBuyID = buyResp.ID
	log.Printf("📥 Buy Grid: %.8f BTC @ %.2f AUD (ID: %s)", buyAmountBTC, buyPrice, r.lastBuyID)

	return nil
}

// ordersWereFilled checks if the previously stored grid orders are no longer open.
func (r *GridRobot) ordersWereFilled(ctx context.Context) bool {
	openResp, err := r.roClient.ROGetMyOpenMarketOrders(ctx, r.cfg.APIKey, r.cfg.SecretKey, "", "")
	if err != nil {
		log.Printf("⚠️ Failed to check open orders: %v", err)
		return false
	}

	foundBuy := false
	foundSell := false

	for _, o := range openResp.BuyOrders {
		if o.ID == r.lastBuyID {
			foundBuy = true
			break
		}
	}
	for _, o := range openResp.SellOrders {
		if o.ID == r.lastSellID {
			foundSell = true
			break
		}
	}

	return !foundBuy || !foundSell
}
