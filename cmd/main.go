package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"orderbook/internal/collector"
	"orderbook/internal/config"
	"orderbook/internal/database"
	"orderbook/internal/exchange"
	"orderbook/internal/factory"
	"orderbook/internal/orderbook"

	"github.com/shopspring/decimal"
)

func main() {
	// Parse command line flags
	var symbol = flag.String("symbol", "BTCUSDT", "Trading symbol to monitor")
	var logInterval = flag.Duration("log-interval", 10*time.Second, "Interval for logging orderbook stats")
	var dbEnabled = flag.Bool("db-enabled", true, "Enable database storage")
	var dbInterval = flag.Duration("db-interval", 20*time.Second, "Interval for database storage")
	flag.Parse()

	// Set up signal handling
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	log.Printf("Starting multi-exchange orderbook monitor for %s", *symbol)
	log.Printf("Log interval: %v", *logInterval)
	if *dbEnabled {
		log.Printf("Database storage enabled with interval: %v", *dbInterval)
	}

	runMultiExchange(*symbol, *logInterval, *dbEnabled, *dbInterval, interrupt)
}

type orderbookWithName struct {
	name string
	ob   *orderbook.OrderBook
}

const (
	colorReset   = "\033[0m"
	colorYellow  = "\033[33m"
	colorGreen   = "\033[32m"
	colorRed     = "\033[31m"
	colorMagenta = "\033[35m"
	colorBold    = "\033[1m"
)

func getExchangeNames() []exchange.ExchangeName {
	return []exchange.ExchangeName{
		exchange.Binancef,
		exchange.Binance,
		exchange.Bybitf,
		exchange.Bybit,
		exchange.Kraken,
		exchange.OKX,
		exchange.Coinbase,
		exchange.Asterdexf,
		exchange.BingX,
		exchange.Hyperliquidf,
	}
}

func runMultiExchange(initialSymbol string, logInterval time.Duration, dbEnabled bool, dbInterval time.Duration, interrupt chan os.Signal) {
	ctx := context.Background()
	orderbooksMap := make(map[string]*orderbook.OrderBook)
	var obMutex sync.Mutex
	currentSymbol := initialSymbol

	// Initialize database client and collector if enabled
	var dbClient database.SupabaseAPIClient
	var dataCollector *collector.Collector
	if dbEnabled {
		// Get Supabase configuration from environment
		baseURL, apiKey := getSupabaseConfig()

		// Create API client
		dbClient = *database.NewSupabaseAPIClient(baseURL, apiKey)

		// Test API connection
		if err := dbClient.TestConnection(); err != nil {
			log.Fatalf("Supabase API connection test failed: %v", err)
		}
		log.Println("Supabase API connection established successfully")

		// Create data collector
		dataCollector = collector.NewCollector(&dbClient, currentSymbol, dbInterval)

		// Start data collection in background
		go dataCollector.Start(ctx)
	}

	// Main loop to handle symbol changes
	for {
		log.Printf("Starting exchanges for symbol: %s", currentSymbol)

		// Start all exchanges with current symbol
		done := make(chan struct{})
		exchangesDone := make(chan struct{})

		go func() {
			startExchangesForSymbol(ctx, currentSymbol, orderbooksMap, &obMutex, logInterval, dataCollector, done, interrupt)
			close(exchangesDone)
		}()

		// Wait for interrupt
		<-interrupt
		log.Println("Interrupt received, shutting down...")
		close(done)
		<-exchangesDone
		log.Println("All exchanges closed. Goodbye!")
		return
	}
}

func startExchangesForSymbol(ctx context.Context, symbol string, orderbooksMap map[string]*orderbook.OrderBook, obMutex *sync.Mutex, logInterval time.Duration, dataCollector *collector.Collector, done chan struct{}, interrupt chan os.Signal) {
	cfg := config.NewMultiExchange(buildExchangeConfigs(symbol))

	var wg sync.WaitGroup
	orderbooks := make([]*orderbookWithName, 0, len(cfg.Exchanges))

	// Create an orderbook for each exchange
	for _, exConfig := range cfg.Exchanges {
		wg.Add(1)
		go func(exCfg config.ExchangeConfig) {
			defer wg.Done()

			log.Printf("[%s] Starting connection...", exCfg.Name)

			// Create exchange-specific orderbook
			ob := orderbook.New()

			// Create exchange instance
			ex, err := factory.NewExchange(factory.ExchangeConfig{
				Name:   exCfg.Name,
				Symbol: exCfg.Symbol,
			})
			if err != nil {
				log.Printf("[%s] Failed to create exchange: %v", exCfg.Name, err)
				return
			}

			// Connect
			if err := ex.Connect(ctx); err != nil {
				log.Printf("[%s] Failed to connect: %v", exCfg.Name, err)
				return
			}
			defer ex.Close()

			// Get snapshot
			snapshot, err := ex.GetSnapshot(ctx)
			if err != nil {
				log.Printf("[%s] Failed to get snapshot: %v", exCfg.Name, err)
				return
			}

			if err := ob.LoadSnapshot(snapshot); err != nil {
				log.Printf("[%s] Failed to load snapshot: %v", exCfg.Name, err)
				return
			}

			// Process updates in background
			updatesDone := make(chan struct{})
			go func() {
				defer close(updatesDone)
				for update := range ex.Updates() {
					ob.HandleDepthUpdate(update)
				}
			}()

			// Reinitialization check
			go func() {
				ticker := time.NewTicker(cfg.App.ReinitCheckInterval)
				defer ticker.Stop()

				for {
					select {
					case <-ticker.C:
						ob.CheckAndReinitialize(func() (*exchange.Snapshot, error) {
							return ex.GetSnapshot(ctx)
						})
					case <-updatesDone:
						return
					case <-done:
						return
					case <-interrupt:
						return
					}
				}
			}()

			ob.ProcessBufferedEvents()
			log.Printf("[%s] Orderbook initialized", exCfg.Name)

			// Add orderbook to shared collections
			obMutex.Lock()
			orderbooks = append(orderbooks, &orderbookWithName{
				name: string(exCfg.Name),
				ob:   ob,
			})
			orderbooksMap[string(exCfg.Name)] = ob
			obMutex.Unlock()

			// Register orderbook with data collector if enabled
			if dataCollector != nil {
				dataCollector.RegisterOrderbook(string(exCfg.Name), ob)
			}

			// Wait for shutdown
			select {
			case <-updatesDone:
				log.Printf("[%s] Connection closed", exCfg.Name)
			case <-done:
				log.Printf("[%s] Shutting down...", exCfg.Name)
			case <-interrupt:
				log.Printf("[%s] Shutting down...", exCfg.Name)
			}

			// Unregister from data collector if enabled
			if dataCollector != nil {
				dataCollector.UnregisterOrderbook(string(exCfg.Name))
			}

			// Remove from map on shutdown
			obMutex.Lock()
			delete(orderbooksMap, string(exCfg.Name))
			obMutex.Unlock()
		}(exConfig)
	}

	// Centralized logging ticker
	go func() {
		ticker := time.NewTicker(logInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				obMutex.Lock()
				printCombinedStats(orderbooks)
				obMutex.Unlock()
			case <-done:
				return
			case <-interrupt:
				return
			}
		}
	}()

	wg.Wait()
}

func buildExchangeConfigs(symbol string) []config.ExchangeConfig {
	names := getExchangeNames()
	configs := make([]config.ExchangeConfig, len(names))
	for i, name := range names {
		configs[i] = config.ExchangeConfig{
			Name:   name,
			Symbol: symbol,
		}
	}
	return configs
}

func printCombinedStats(orderbooks []*orderbookWithName) {
	if len(orderbooks) == 0 {
		return
	}

	fmt.Println()

	for i, obn := range orderbooks {
		if !obn.ob.IsInitialized() {
			continue
		}

		stats := obn.ob.GetStats()
		midPrice := stats.BestBid.Add(stats.BestAsk).Div(decimal.NewFromInt(2))

		// print exchange name
		fmt.Printf("%s%s%s", colorBold, obn.name, colorReset)
		// Print exchange header
		fmt.Printf("  Mid: %s%10s%s │ Spread: %s%8s%s | BB: %s%10s%s │ BA: %s%10s%s\n",
			colorYellow, midPrice.StringFixed(2), colorReset,
			colorMagenta, stats.Spread.StringFixed(4), colorReset,
			colorGreen, stats.BestBid.StringFixed(2), colorReset,
			colorRed, stats.BestAsk.StringFixed(2), colorReset)

		// Print depth metrics
		fmt.Printf("  DEPTH 0.5%% Bids: %s%9s%s │ Asks: %s%9s%s │ Δ: %s%10s%s\n",
			colorGreen, stats.BidLiquidity05Pct.StringFixed(2), colorReset,
			colorRed, stats.AskLiquidity05Pct.StringFixed(2), colorReset,
			getDeltaColor(stats.DeltaLiquidity05Pct), stats.DeltaLiquidity05Pct.StringFixed(2), colorReset)

		fmt.Printf("  DEPTH 2%%:  Bids: %s%9s%s │ Asks: %s%9s%s │ Δ: %s%10s%s\n",
			colorGreen, stats.BidLiquidity2Pct.StringFixed(2), colorReset,
			colorRed, stats.AskLiquidity2Pct.StringFixed(2), colorReset,
			getDeltaColor(stats.DeltaLiquidity2Pct), stats.DeltaLiquidity2Pct.StringFixed(2), colorReset)

		fmt.Printf("  DEPTH 10%%  Bids: %s%9s%s │ Asks: %s%9s%s │ Δ: %s%10s%s\n",
			colorGreen, stats.BidLiquidity10Pct.StringFixed(2), colorReset,
			colorRed, stats.AskLiquidity10Pct.StringFixed(2), colorReset,
			getDeltaColor(stats.DeltaLiquidity10Pct), stats.DeltaLiquidity10Pct.StringFixed(2), colorReset)

		fmt.Printf("  TOTAL QTY: Bids: %s%9s%s │ Asks: %s%9s%s\n",
			colorGreen, stats.TotalBidsQty.StringFixed(2), colorReset,
			colorRed, stats.TotalAsksQty.StringFixed(2), colorReset)

		// Print separator between exchanges (but not after the last one)
		if i < len(orderbooks)-1 {
			fmt.Println()
		}
	}
}

func getDeltaColor(delta decimal.Decimal) string {
	if delta.GreaterThan(decimal.Zero) {
		return colorGreen
	} else if delta.LessThan(decimal.Zero) {
		return colorRed
	}
	return colorYellow
}

// getSupabaseConfig gets Supabase configuration from environment variables
func getSupabaseConfig() (string, string) {
	// Get Supabase URL and API key from environment
	baseURL := os.Getenv("SUPABASE_URL")
	apiKey := os.Getenv("SUPABASE_ANON_KEY")

	// Set defaults if not provided
	if baseURL == "" {
		baseURL = "https://qlcmrsbvdmyflllavyzc.supabase.co"
	}
	if apiKey == "" {
		log.Fatal("SUPABASE_ANON_KEY environment variable is required. Please set it with your Supabase anonymous key.")
	}

	log.Printf("Supabase API: %s", baseURL)
	return baseURL, apiKey
}
