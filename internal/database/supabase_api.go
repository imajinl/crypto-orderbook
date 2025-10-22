package database

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// SupabaseAPIClient handles database operations via Supabase API
type SupabaseAPIClient struct {
	baseURL string
	apiKey  string
	client  *http.Client
}

// NewSupabaseAPIClient creates a new API client
func NewSupabaseAPIClient(baseURL, apiKey string) *SupabaseAPIClient {
	return &SupabaseAPIClient{
		baseURL: baseURL,
		apiKey:  apiKey,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// OrderbookSnapshotAPI represents the API payload structure
type OrderbookSnapshotAPI struct {
	Exchange          string    `json:"exchange"`
	Symbol            string    `json:"symbol"`
	Timestamp         time.Time `json:"timestamp"`
	BestBid           *float64  `json:"best_bid"`
	BestAsk           *float64  `json:"best_ask"`
	MidPrice          *float64  `json:"mid_price"`
	Spread            *float64  `json:"spread"`
	BidLiquidity05Pct *float64  `json:"bid_liquidity_05_pct"`
	AskLiquidity05Pct *float64  `json:"ask_liquidity_05_pct"`
	BidLiquidity2Pct  *float64  `json:"bid_liquidity_2_pct"`
	AskLiquidity2Pct  *float64  `json:"ask_liquidity_2_pct"`
	BidLiquidity10Pct *float64  `json:"bid_liquidity_10_pct"`
	AskLiquidity10Pct *float64  `json:"ask_liquidity_10_pct"`
	TotalBidsQty      *float64  `json:"total_bids_qty"`
	TotalAsksQty      *float64  `json:"total_asks_qty"`
}

// InsertOrderbookSnapshot inserts a single snapshot via API
func (c *SupabaseAPIClient) InsertOrderbookSnapshot(snapshot *OrderbookSnapshotAPI) error {
	jsonData, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	url := fmt.Sprintf("%s/rest/v1/orderbook_snapshots", c.baseURL)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("apikey", c.apiKey)
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// InsertOrderbookSnapshotsBatch inserts multiple snapshots via API
func (c *SupabaseAPIClient) InsertOrderbookSnapshotsBatch(snapshots []*OrderbookSnapshotAPI) error {
	if len(snapshots) == 0 {
		return nil
	}

	jsonData, err := json.Marshal(snapshots)
	if err != nil {
		return fmt.Errorf("failed to marshal snapshots: %w", err)
	}

	url := fmt.Sprintf("%s/rest/v1/orderbook_snapshots", c.baseURL)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("apikey", c.apiKey)
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Prefer", "return=minimal")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// TestConnection tests the API connection
func (c *SupabaseAPIClient) TestConnection() error {
	url := fmt.Sprintf("%s/rest/v1/orderbook_snapshots?select=id&limit=1", c.baseURL)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("apikey", c.apiKey)
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// Close is a no-op for API client
func (c *SupabaseAPIClient) Close() error {
	return nil
}
