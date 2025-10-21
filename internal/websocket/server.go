package websocket

import (
	"encoding/json"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"

	"orderbook/internal/aggregation"
	"orderbook/internal/orderbook"
	"orderbook/internal/types"

	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
)

type MessageType string

const (
	MessageTypeOrderbook MessageType = "orderbook"
	MessageTypeStats     MessageType = "stats"
)

// ClientMessage represents messages sent from client to server
type ClientMessage struct {
	Type   string  `json:"type"`
	Tick   float64 `json:"tick,omitempty"`
	Symbol string  `json:"symbol,omitempty"`
}

type OrderbookMessage struct {
	Type      MessageType  `json:"type"`
	Exchange  string       `json:"exchange"`
	Bids      []PriceLevel `json:"bids"`
	Asks      []PriceLevel `json:"asks"`
	Timestamp int64        `json:"timestamp"`
}

type StatsMessage struct {
	Type                 MessageType `json:"type"`
	Exchange             string      `json:"exchange"`
	BestBid              string      `json:"bestBid"`
	BestAsk              string      `json:"bestAsk"`
	MidPrice             string      `json:"midPrice"`
	Spread               string      `json:"spread"`
	BidLiquidity05Pct    string      `json:"bidLiquidity05Pct"`
	AskLiquidity05Pct    string      `json:"askLiquidity05Pct"`
	DeltaLiquidity05Pct  string      `json:"deltaLiquidity05Pct"`
	BidLiquidity2Pct     string      `json:"bidLiquidity2Pct"`
	AskLiquidity2Pct     string      `json:"askLiquidity2Pct"`
	DeltaLiquidity2Pct   string      `json:"deltaLiquidity2Pct"`
	BidLiquidity10Pct    string      `json:"bidLiquidity10Pct"`
	AskLiquidity10Pct    string      `json:"askLiquidity10Pct"`
	DeltaLiquidity10Pct  string      `json:"deltaLiquidity10Pct"`
	TotalBidsQty         string      `json:"totalBidsQty"`
	TotalAsksQty         string      `json:"totalAsksQty"`
	TotalDelta           string      `json:"totalDelta"`
	Timestamp            int64       `json:"timestamp"`
}

type PriceLevel struct {
	Price      string `json:"price"`
	Quantity   string `json:"quantity"`
	Cumulative string `json:"cumulative"`
}

type Server struct {
	orderbooks   map[string]*orderbook.OrderBook
	port         string
	upgrader     websocket.Upgrader
	clients      map[*websocket.Conn]bool
	clientsMux   sync.RWMutex
	broadcast    chan interface{}
	aggregator   *aggregation.Aggregator
	tickMux      sync.RWMutex
	symbolChange chan string
}

func NewServer(orderbooks map[string]*orderbook.OrderBook, port string, symbolChange chan string) *Server {
	return &Server{
		orderbooks:   orderbooks,
		port:         port,
		clients:      make(map[*websocket.Conn]bool),
		broadcast:    make(chan interface{}, 100),
		aggregator:   aggregation.New(types.Tick1), // Default to 1.0 tick
		symbolChange: symbolChange,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	}
}

func (s *Server) Start() error {
	http.HandleFunc("/ws", s.handleWebSocket)

	go s.broadcastMessages()
	go s.startDataPush()

	log.Printf("WebSocket server starting on port %s", s.port)
	return http.ListenAndServe(":"+s.port, nil)
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}

	s.clientsMux.Lock()
	s.clients[conn] = true
	s.clientsMux.Unlock()

	log.Printf("New WebSocket client connected from %s", r.RemoteAddr)

	defer func() {
		s.clientsMux.Lock()
		delete(s.clients, conn)
		s.clientsMux.Unlock()
		conn.Close()
		log.Printf("WebSocket client disconnected")
	}()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			break
		}

		var clientMsg ClientMessage
		if err := json.Unmarshal(message, &clientMsg); err != nil {
			log.Printf("Error parsing client message: %v", err)
			continue
		}

		s.handleClientMessage(clientMsg)
	}
}

func (s *Server) handleClientMessage(msg ClientMessage) {
	switch msg.Type {
	case "set_tick":
		s.setTickLevel(msg.Tick)
	case "change_symbol":
		if msg.Symbol != "" {
			log.Printf("Symbol change request: %s", msg.Symbol)
			s.symbolChange <- msg.Symbol
		}
	default:
		log.Printf("Unknown message type: %s", msg.Type)
	}
}

func (s *Server) setTickLevel(tick float64) {
	tickLevel := types.TickLevel(tick)

	// Validate tick level
	validTick := false
	for _, available := range types.AvailableTickLevels {
		if available == tickLevel {
			validTick = true
			break
		}
	}

	if !validTick {
		log.Printf("Invalid tick level: %f, using default", tick)
		return
	}

	s.tickMux.Lock()
	s.aggregator.SetTickLevel(tickLevel)
	s.tickMux.Unlock()

	log.Printf("Tick level changed to: %f", tick)
}

func (s *Server) broadcastMessages() {
	for msg := range s.broadcast {
		s.clientsMux.RLock()
		for client := range s.clients {
			err := client.WriteJSON(msg)
			if err != nil {
				log.Printf("Error writing to client: %v", err)
				client.Close()
				s.clientsMux.Lock()
				delete(s.clients, client)
				s.clientsMux.Unlock()
			}
		}
		s.clientsMux.RUnlock()
	}
}

func (s *Server) startDataPush() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		s.clientsMux.RLock()
		hasClients := len(s.clients) > 0
		s.clientsMux.RUnlock()

		if !hasClients {
			continue
		}

		timestamp := time.Now().UnixMilli()

		for exchangeName, ob := range s.orderbooks {
			if !ob.IsInitialized() {
				continue
			}

			orderbookMsg := s.buildOrderbookMessage(exchangeName, ob, timestamp)
			s.broadcast <- orderbookMsg

			statsMsg := s.buildStatsMessage(exchangeName, ob, timestamp)
			s.broadcast <- statsMsg
		}
	}
}

func (s *Server) buildOrderbookMessage(exchange string, ob *orderbook.OrderBook, timestamp int64) OrderbookMessage {
	bidsMap := ob.GetBids()
	asksMap := ob.GetAsks()

	// Convert maps to slices of types.PriceLevel
	bidLevels := make([]types.PriceLevel, 0, len(bidsMap))
	for _, bid := range bidsMap {
		bidLevels = append(bidLevels, bid)
	}

	askLevels := make([]types.PriceLevel, 0, len(asksMap))
	for _, ask := range asksMap {
		askLevels = append(askLevels, ask)
	}

	// Apply aggregation
	s.tickMux.RLock()
	aggregatedBids := s.aggregator.AggregateBids(bidLevels)
	aggregatedAsks := s.aggregator.AggregateAsks(askLevels)
	s.tickMux.RUnlock()

	// Sort bids by price descending (highest first)
	sort.Slice(aggregatedBids, func(i, j int) bool {
		return aggregatedBids[i].Price.GreaterThan(aggregatedBids[j].Price)
	})

	// Sort asks by price ascending (lowest first)
	sort.Slice(aggregatedAsks, func(i, j int) bool {
		return aggregatedAsks[i].Price.LessThan(aggregatedAsks[j].Price)
	})

	// Convert bids to wire format with cumulative sums
	bids := make([]PriceLevel, 0, len(aggregatedBids))
	bidCumulative := decimal.Zero
	for _, bid := range aggregatedBids {
		bidCumulative = bidCumulative.Add(bid.Quantity)
		bids = append(bids, PriceLevel{
			Price:      bid.Price.String(),
			Quantity:   bid.Quantity.String(),
			Cumulative: bidCumulative.String(),
		})
	}

	// Convert asks to wire format with cumulative sums
	asks := make([]PriceLevel, 0, len(aggregatedAsks))
	askCumulative := decimal.Zero
	for _, ask := range aggregatedAsks {
		askCumulative = askCumulative.Add(ask.Quantity)
		asks = append(asks, PriceLevel{
			Price:      ask.Price.String(),
			Quantity:   ask.Quantity.String(),
			Cumulative: askCumulative.String(),
		})
	}

	return OrderbookMessage{
		Type:      MessageTypeOrderbook,
		Exchange:  exchange,
		Bids:      bids,
		Asks:      asks,
		Timestamp: timestamp,
	}
}

func (s *Server) buildStatsMessage(exchange string, ob *orderbook.OrderBook, timestamp int64) StatsMessage {
	stats := ob.GetStats()

	return StatsMessage{
		Type:                 MessageTypeStats,
		Exchange:             exchange,
		BestBid:              stats.BestBid.String(),
		BestAsk:              stats.BestAsk.String(),
		MidPrice:             stats.BestBid.Add(stats.BestAsk).Div(decimal.NewFromInt(2)).String(),
		Spread:               stats.Spread.String(),
		BidLiquidity05Pct:    stats.BidLiquidity05Pct.String(),
		AskLiquidity05Pct:    stats.AskLiquidity05Pct.String(),
		DeltaLiquidity05Pct:  stats.DeltaLiquidity05Pct.String(),
		BidLiquidity2Pct:     stats.BidLiquidity2Pct.String(),
		AskLiquidity2Pct:     stats.AskLiquidity2Pct.String(),
		DeltaLiquidity2Pct:   stats.DeltaLiquidity2Pct.String(),
		BidLiquidity10Pct:    stats.BidLiquidity10Pct.String(),
		AskLiquidity10Pct:    stats.AskLiquidity10Pct.String(),
		DeltaLiquidity10Pct:  stats.DeltaLiquidity10Pct.String(),
		TotalBidsQty:         stats.TotalBidsQty.String(),
		TotalAsksQty:         stats.TotalAsksQty.String(),
		TotalDelta:           stats.TotalDelta.String(),
		Timestamp:            timestamp,
	}
}
