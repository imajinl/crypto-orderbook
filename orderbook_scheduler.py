#!/usr/bin/env python3
"""
Orderbook Data Collection Scheduler
Runs the Go application for different cryptocurrency symbols sequentially
"""

import subprocess
import time
import schedule
import logging
from datetime import datetime
import signal
import sys
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log'),
        logging.StreamHandler()
    ]
)

class OrderbookScheduler:
    def __init__(self):
        self.project_dir = "/Users/yakub/Desktop/Miscellaneous/Code/crypto-orderbook"
        self.go_path = "/opt/homebrew/bin/go"
        self.symbols = ["BTCUSDT", "SOLUSDT", "ETHUSDT", "BNBUSDT"]
        self.duration = 120  # 2 minutes per symbol
        self.running = True
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logging.info(f"Received signal {signum}, shutting down...")
        self.running = False
        sys.exit(0)
    
    def run_symbol(self, symbol):
        """Run the Go application for a specific symbol"""
        log_file = f"logs/orderbook_{symbol.lower()}.log"
        
        logging.info(f"Starting data collection for {symbol}")
        
        try:
            # Change to project directory
            os.chdir(self.project_dir)
            
            # Run the Go application
            cmd = [
                self.go_path, "run", "./cmd/main.go", 
                f"--symbol={symbol}"
            ]
            
            # Start the process
            process = subprocess.Popen(
                cmd,
                stdout=open(log_file, 'a'),
                stderr=subprocess.STDOUT,
                preexec_fn=os.setsid  # Create new process group
            )
            
            # Wait for the specified duration
            time.sleep(self.duration)
            
            # Terminate the process
            logging.info(f"Stopping data collection for {symbol}")
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            
            # Wait a bit for graceful shutdown
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logging.warning(f"Force killing {symbol} process")
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            
            logging.info(f"Completed data collection for {symbol}")
            
        except Exception as e:
            logging.error(f"Error running {symbol}: {e}")
    
    def collect_all_symbols(self):
        """Collect data for all symbols sequentially"""
        if not self.running:
            return
            
        logging.info("Starting data collection cycle")
        start_time = datetime.now()
        
        for symbol in self.symbols:
            if not self.running:
                break
            self.run_symbol(symbol)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logging.info(f"Completed data collection cycle in {duration:.1f} seconds")
    
    def start(self):
        """Start the scheduler"""
        logging.info("Starting Orderbook Scheduler")
        logging.info(f"Symbols: {', '.join(self.symbols)}")
        logging.info(f"Duration per symbol: {self.duration} seconds")
        logging.info(f"Total cycle time: {len(self.symbols) * self.duration} seconds")
        
        # Schedule to run every 8 minutes
        schedule.every(8).minutes.do(self.collect_all_symbols)
        
        # Run immediately on start
        self.collect_all_symbols()
        
        # Keep the scheduler running
        while self.running:
            schedule.run_pending()
            time.sleep(1)
        
        logging.info("Scheduler stopped")

if __name__ == "__main__":
    scheduler = OrderbookScheduler()
    scheduler.start()
