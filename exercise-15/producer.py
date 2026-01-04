"""
Electricity Price Producer
Monitors electricity price data file and sends updates to Kafka when prices change.
"""

import json
import time
import hashlib
from datetime import datetime
from kafka import KafkaProducer
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Configuration
KAFKA_TOPIC = 'electricity-prices'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
PRICE_FILE_PATH = '/Users/selashma20/Downloads/latest-prices.json'
CHECK_INTERVAL = 10  # seconds

class ElectricityPriceProducer:
    def __init__(self):
        """Initialize Kafka producer."""
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        self.last_file_hash = None
        print("✓ Kafka Producer initialized")
        print(f"✓ Monitoring file: {PRICE_FILE_PATH}")
        print(f"✓ Publishing to topic: {KAFKA_TOPIC}")
    
    def get_file_hash(self, filepath):
        """Calculate MD5 hash of file to detect changes."""
        try:
            with open(filepath, 'rb') as f:
                return hashlib.md5(f.read()).hexdigest()
        except FileNotFoundError:
            return None
    
    def read_prices(self):
        """Read electricity prices from JSON file."""
        try:
            with open(PRICE_FILE_PATH, 'r') as f:
                data = json.load(f)
                return data.get('prices', [])
        except FileNotFoundError:
            print(f"⚠ File not found: {PRICE_FILE_PATH}")
            return []
        except json.JSONDecodeError as e:
            print(f"⚠ JSON decode error: {e}")
            return []
    
    def send_price_update(self, price_data):
        """Send price update to Kafka."""
        try:
            # Add metadata
            message = {
                'timestamp': datetime.now().isoformat(),
                'price': price_data['price'],
                'startDate': price_data['startDate'],
                'endDate': price_data['endDate'],
                'source': 'electricity-price-monitor'
            }
            
            # Use startDate as key for partitioning
            key = price_data['startDate']
            
            # Send to Kafka
            future = self.producer.send(KAFKA_TOPIC, key=key, value=message)
            future.get(timeout=10)
            
            return True
        except Exception as e:
            print(f"✗ Error sending to Kafka: {e}")
            return False
    
    def check_for_updates(self):
        """Check if file has changed and send new prices."""
        current_hash = self.get_file_hash(PRICE_FILE_PATH)
        
        if current_hash is None:
            print(f"⚠ Cannot read file: {PRICE_FILE_PATH}")
            return
        
        if current_hash != self.last_file_hash:
            print(f"\n{'='*60}")
            print(f"📊 File changed detected at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*60}")
            
            prices = self.read_prices()
            
            if prices:
                print(f"✓ Found {len(prices)} price entries")
                
                # Send all prices to Kafka
                sent_count = 0
                for price in prices:
                    if self.send_price_update(price):
                        sent_count += 1
                
                print(f"✓ Successfully sent {sent_count}/{len(prices)} prices to Kafka")
                
                # Show sample prices
                print(f"\n📈 Latest Prices (first 5):")
                for i, price in enumerate(prices[:5], 1):
                    print(f"  {i}. {price['startDate'][:19]} - {price['price']:.3f} cents/kWh")
            else:
                print("⚠ No prices found in file")
            
            self.last_file_hash = current_hash
            print(f"{'='*60}\n")
    
    def run(self):
        """Main producer loop."""
        print(f"\n{'='*60}")
        print("Electricity Price Producer Started")
        print(f"{'='*60}")
        print(f"Monitoring: {PRICE_FILE_PATH}")
        print(f"Check interval: {CHECK_INTERVAL} seconds")
        print(f"Kafka topic: {KAFKA_TOPIC}")
        print(f"Press Ctrl+C to stop")
        print(f"{'='*60}\n")
        
        try:
            # Initial check
            self.check_for_updates()
            
            # Continuous monitoring
            while True:
                time.sleep(CHECK_INTERVAL)
                self.check_for_updates()
                
        except KeyboardInterrupt:
            print("\n\n✓ Producer stopped by user")
        except Exception as e:
            print(f"\n✗ Error: {e}")
        finally:
            self.producer.close()
            print("✓ Kafka producer closed")


if __name__ == "__main__":
    producer = ElectricityPriceProducer()
    producer.run()
