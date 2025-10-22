package collector

import (
	"context"
	"log"
	"sync"
	"time"

	"orderbook/internal/database"
	"orderbook/internal/orderbook"
	"orderbook/internal/types"

	"github.com/shopspring/decimal"
)

// DatabaseClient interface for different database implementations
type DatabaseClient interface {
	InsertOrderbookSnapshot(snapshot *database.OrderbookSnapshotAPI) error
	InsertOrderbookSnapshotsBatch(snapshots []*database.OrderbookSnapshotAPI) error
	TestConnection() error
	Close() error
}

// Collector handles periodic data collection and storage
type Collector struct {
	dbClient   DatabaseClient
	orderbooks map[string]*orderbook.OrderBook
	mu         sync.RWMutex
	symbol     string
	interval   time.Duration
	enabled    bool
}

// NewCollector creates a new data collector
func NewCollector(dbClient DatabaseClient, symbol string, interval time.Duration) *Collector {
	return &Collector{
		dbClient:   dbClient,
		orderbooks: make(map[string]*orderbook.OrderBook),
		symbol:     symbol,
		interval:   interval,
		enabled:    true,
	}
}

// RegisterOrderbook registers an orderbook for data collection
func (c *Collector) RegisterOrderbook(exchange string, ob *orderbook.OrderBook) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.orderbooks[exchange] = ob
	log.Printf("[Collector] Registered orderbook for exchange: %s", exchange)
}

// UnregisterOrderbook removes an orderbook from data collection
func (c *Collector) UnregisterOrderbook(exchange string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.orderbooks, exchange)
	log.Printf("[Collector] Unregistered orderbook for exchange: %s", exchange)
}

// Start begins the data collection process
func (c *Collector) Start(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	log.Printf("[Collector] Starting data collection for %s every %v", c.symbol, c.interval)

	for {
		select {
		case <-ctx.Done():
			log.Println("[Collector] Data collection stopped")
			return
		case <-ticker.C:
			if c.enabled {
				c.collectAndStore()
			}
		}
	}
}

// SetEnabled enables or disables data collection
func (c *Collector) SetEnabled(enabled bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.enabled = enabled
	log.Printf("[Collector] Data collection %s", map[bool]string{true: "enabled", false: "disabled"}[enabled])
}

// collectAndStore collects data from all registered orderbooks and stores it
func (c *Collector) collectAndStore() {
	c.mu.RLock()
	orderbooks := make(map[string]*orderbook.OrderBook)
	for k, v := range c.orderbooks {
		orderbooks[k] = v
	}
	c.mu.RUnlock()

	if len(orderbooks) == 0 {
		log.Println("[Collector] No orderbooks registered, skipping collection")
		return
	}

	var snapshots []*database.OrderbookSnapshotAPI
	successCount := 0

	for exchange, ob := range orderbooks {
		if !ob.IsInitialized() {
			log.Printf("[Collector] Skipping %s - orderbook not initialized", exchange)
			continue
		}

		stats := ob.GetStats()
		snapshot := c.createSnapshot(exchange, stats, ob)
		snapshots = append(snapshots, snapshot)
		successCount++
	}

	if len(snapshots) > 0 {
		if err := c.dbClient.InsertOrderbookSnapshotsBatch(snapshots); err != nil {
			log.Printf("[Collector] Failed to insert batch of %d snapshots: %v", len(snapshots), err)
		} else {
			log.Printf("[Collector] Successfully stored %d snapshots", successCount)
		}
	} else {
		log.Println("[Collector] No valid snapshots to store")
	}
}

// createSnapshot creates a database snapshot from orderbook stats
func (c *Collector) createSnapshot(exchange string, stats types.Stats, ob *orderbook.OrderBook) *database.OrderbookSnapshotAPI {
	// Calculate mid price
	var midPrice *float64
	if !stats.BestBid.IsZero() && !stats.BestAsk.IsZero() && stats.BestAsk.GreaterThan(stats.BestBid) {
		mid := stats.BestBid.Add(stats.BestAsk).Div(decimal.NewFromInt(2))
		mp := mid.InexactFloat64()
		midPrice = &mp
	}

	// Convert decimal values to float64 pointers
	bestBid := stats.BestBid.InexactFloat64()
	bestAsk := stats.BestAsk.InexactFloat64()
	spread := stats.Spread.InexactFloat64()

	bidLiq05 := stats.BidLiquidity05Pct.InexactFloat64()
	askLiq05 := stats.AskLiquidity05Pct.InexactFloat64()
	bidLiq2 := stats.BidLiquidity2Pct.InexactFloat64()
	askLiq2 := stats.AskLiquidity2Pct.InexactFloat64()
	bidLiq10 := stats.BidLiquidity10Pct.InexactFloat64()
	askLiq10 := stats.AskLiquidity10Pct.InexactFloat64()
	totalBids := stats.TotalBidsQty.InexactFloat64()
	totalAsks := stats.TotalAsksQty.InexactFloat64()

	// Collect bid/ask data for logging (not uploaded to database)
	bidLevels := c.convertPriceLevels(ob.GetBids())
	askLevels := c.convertPriceLevels(ob.GetAsks())

	// Log orderbook data for debugging/monitoring (optional)
	log.Printf("[Collector] %s: %d bids, %d asks", exchange, len(bidLevels), len(askLevels))

	return &database.OrderbookSnapshotAPI{
		Exchange:          exchange,
		Symbol:            c.symbol,
		Timestamp:         time.Now(),
		BestBid:           &bestBid,
		BestAsk:           &bestAsk,
		MidPrice:          midPrice,
		Spread:            &spread,
		BidLiquidity05Pct: &bidLiq05,
		AskLiquidity05Pct: &askLiq05,
		BidLiquidity2Pct:  &bidLiq2,
		AskLiquidity2Pct:  &askLiq2,
		BidLiquidity10Pct: &bidLiq10,
		AskLiquidity10Pct: &askLiq10,
		TotalBidsQty:      &totalBids,
		TotalAsksQty:      &totalAsks,
	}
}

// convertPriceLevels converts orderbook price levels to a JSON-serializable format
func (c *Collector) convertPriceLevels(levels map[string]types.PriceLevel) []map[string]interface{} {
	var result []map[string]interface{}
	for price, level := range levels {
		result = append(result, map[string]interface{}{
			"price":    price,
			"quantity": level.Quantity.String(),
		})
	}
	return result
}

// GetStats returns collector statistics
func (c *Collector) GetStats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stats := map[string]interface{}{
		"symbol":           c.symbol,
		"interval":         c.interval.String(),
		"enabled":          c.enabled,
		"registered_count": len(c.orderbooks),
		"exchanges":        make([]string, 0, len(c.orderbooks)),
	}

	for exchange := range c.orderbooks {
		stats["exchanges"] = append(stats["exchanges"].([]string), exchange)
	}

	return stats
}
