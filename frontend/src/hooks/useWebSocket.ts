import { useEffect, useRef, useState } from 'react';
import type {
  WebSocketMessage,
  OrderbookData,
  StatsData,
} from '@/types';

export function useWebSocket(url: string) {
  const [orderbooks, setOrderbooks] = useState<OrderbookData>({});
  const [stats, setStats] = useState<StatsData>({});
  const [isConnected, setIsConnected] = useState(false);
  const [currentSymbol, setCurrentSymbol] = useState('BTCUSDT');
  const [isSwitchingSymbol, setIsSwitchingSymbol] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<number | undefined>(undefined);

  useEffect(() => {
    function connect() {
      const ws = new WebSocket(url);
      wsRef.current = ws;

      ws.onopen = () => {
        setIsConnected(true);
        console.log('WebSocket connected');
      };

      ws.onmessage = (event) => {
        const message: WebSocketMessage = JSON.parse(event.data);

        if (message.type === 'orderbook') {
          setOrderbooks((prev) => ({
            ...prev,
            [message.exchange]: {
              bids: message.bids,
              asks: message.asks,
            },
          }));
        } else if (message.type === 'stats') {
          setStats((prev) => ({
            ...prev,
            [message.exchange]: {
              bestBid: message.bestBid,
              bestAsk: message.bestAsk,
              midPrice: message.midPrice,
              spread: message.spread,
              bidLiquidity05Pct: message.bidLiquidity05Pct,
              askLiquidity05Pct: message.askLiquidity05Pct,
              deltaLiquidity05Pct: message.deltaLiquidity05Pct,
              bidLiquidity2Pct: message.bidLiquidity2Pct,
              askLiquidity2Pct: message.askLiquidity2Pct,
              deltaLiquidity2Pct: message.deltaLiquidity2Pct,
              bidLiquidity10Pct: message.bidLiquidity10Pct,
              askLiquidity10Pct: message.askLiquidity10Pct,
              deltaLiquidity10Pct: message.deltaLiquidity10Pct,
              totalBidsQty: message.totalBidsQty,
              totalAsksQty: message.totalAsksQty,
              totalDelta: message.totalDelta,
            },
          }));
        }
      };

      ws.onerror = (error) => {
        console.error('WebSocket error:', error);
      };

      ws.onclose = () => {
        setIsConnected(false);
        console.log('WebSocket disconnected, reconnecting in 3s...');
        reconnectTimeoutRef.current = window.setTimeout(connect, 3000);
      };
    }

    connect();

    return () => {
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, [url]);

  const setTickLevel = (tick: number) => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ type: 'set_tick', tick }));
    }
  };

  const setSymbol = (symbol: string) => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      setIsSwitchingSymbol(true);
      setOrderbooks({});
      setStats({});
      setCurrentSymbol(symbol);
      wsRef.current.send(JSON.stringify({ type: 'change_symbol', symbol }));

      setTimeout(() => {
        setIsSwitchingSymbol(false);
      }, 3000);
    }
  };

  return { orderbooks, stats, isConnected, currentSymbol, isSwitchingSymbol, setTickLevel, setSymbol };
}
