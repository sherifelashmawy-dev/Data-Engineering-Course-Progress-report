"""
Electricity Price Consumer
Reads electricity price updates from Kafka and saves to local CSV files.
"""

import json
import csv
import os
from datetime import datetime
from kafka import KafkaConsumer

# Configuration
KAFKA_TOPIC = 'electricity-prices'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_GROUP_ID = 'electricity-price-consumer'
OUTPUT_DIR = 'data'
CSV_FILENAME = 'electricity_prices.csv'

class ElectricityPriceConsumer:
    def __init__(self):
        """Initialize Kafka consumer and output files."""
        # Create output directory
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        self.csv_path = os.path.join(OUTPUT_DIR, CSV_FILENAME)
        
        # Initialize CSV file if it doesn't exist
        if not os.path.exists(self.csv_path):
            with open(self.csv_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['received_timestamp', 'price', 'startDate', 'endDate', 'source'])
            print(f"✓ Created new CSV file: {self.csv_path}")
        else:
            print(f"✓ Using existing CSV file: {self.csv_path}")
        
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',  # Read from beginning
            enable_auto_commit=True
        )
        
        print("✓ Kafka Consumer initialized")
        print(f"✓ Subscribed to topic: {KAFKA_TOPIC}")
        print(f"✓ Consumer group: {KAFKA_GROUP_ID}")
    
    def save_to_csv(self, message):
        """Save price message to CSV file."""
        try:
            with open(self.csv_path, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    message['timestamp'],
                    message['price'],
                    message['startDate'],
                    message['endDate'],
                    message['source']
                ])
            return True
        except Exception as e:
            print(f"✗ Error writing to CSV: {e}")
            return False
    
    def run(self):
        """Main consumer loop."""
        print(f"\n{'='*60}")
        print("Electricity Price Consumer Started")
        print(f"{'='*60}")
        print(f"Reading from topic: {KAFKA_TOPIC}")
        print(f"Saving to: {self.csv_path}")
        print(f"Press Ctrl+C to stop")
        print(f"{'='*60}\n")
        
        message_count = 0
        
        try:
            for message in self.consumer:
                data = message.value
                
                # Save to CSV
                if self.save_to_csv(data):
                    message_count += 1
                    
                    # Print every 50th message to avoid spam
                    if message_count % 50 == 0:
                        print(f"✓ Saved {message_count} messages")
                        print(f"  Latest: {data['startDate'][:19]} - {data['price']:.3f} cents/kWh")
                    elif message_count <= 5:
                        # Print first 5 messages
                        print(f"✓ [{message_count}] {data['startDate'][:19]} - {data['price']:.3f} cents/kWh")
                
        except KeyboardInterrupt:
            print(f"\n\n✓ Consumer stopped by user")
            print(f"✓ Total messages saved: {message_count}")
        except Exception as e:
            print(f"\n✗ Error: {e}")
        finally:
            self.consumer.close()
            print("✓ Kafka consumer closed")


if __name__ == "__main__":
    consumer = ElectricityPriceConsumer()
    consumer.run()
