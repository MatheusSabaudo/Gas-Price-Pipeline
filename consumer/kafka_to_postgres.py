#!/usr/bin/env python3
# ingestion/consumers/gas_price_consumer_simple.py

import json
import time
from confluent_kafka import Consumer
import psycopg2

# ==================== CONFIGURATION ====================

KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9093',
    'group.id': 'gas-price-consumer-group',
    'auto.offset.reset': 'earliest'
}

POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'gas-price',
    'user': 'admin',
    'password': 'admin'
}

TOPIC = 'gas-prices'

# ==================== MAIN ====================

def main():
    print("=" * 60)
    print("ITALIAN GAS PRICE CONSUMER")
    print("=" * 60)
    
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor()
        print("Connected to PostgreSQL")
    except Exception as e:
        print(f"Failed to connect to PostgreSQL: {e}")
        return
    
    # Create consumer
    try:
        consumer = Consumer(KAFKA_CONFIG)
        consumer.subscribe([TOPIC])
        print(f"Connected to Kafka, subscribed to '{TOPIC}'")
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return
    
    print("\nConsumer is running and waiting for messages...")
    print("Press Ctrl+C to stop\n")
    
    count = 0
    
    try:
        while True:
            # Poll for messages (wait up to 1 second)
            msg = consumer.poll(1.0)
            
            if msg is None:
                # No message, just continue the loop
                continue
            
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue
            
            # We got a message!
            try:
                # Parse the message
                record = json.loads(msg.value().decode('utf-8'))
                
                # Insert into PostgreSQL
                cur.execute("""
                    INSERT INTO gas_prices (
                        kafka_key, timestamp, date, time, hour, day_of_week, 
                        month, year, region_id, region_name, region_zone,
                        station_id, station_name, station_brand,
                        fuel_type_id, fuel_type_name, price, price_unit, currency,
                        is_weekend, is_rush_hour, is_anomaly, anomaly_type
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    record.get('kafka_key'), record.get('timestamp'), record.get('date'), record.get('time'),
                    record.get('hour'), record.get('day_of_week'), record.get('month'), record.get('year'),
                    record.get('region_id'), record.get('region_name'), record.get('region_zone'),
                    record.get('station_id'), record.get('station_name'), record.get('station_brand'),
                    record.get('fuel_type_id'), record.get('fuel_type_name'), record.get('price'),
                    record.get('price_unit'), record.get('currency'), record.get('is_weekend'),
                    record.get('is_rush_hour'), record.get('is_anomaly', 0), record.get('anomaly_type')
                ))
                
                conn.commit()
                count += 1
                print(f"Inserted #{count}: {record.get('region_name')} - {record.get('station_name')} - €{record.get('price')}")
                
            except Exception as e:
                print(f"Error processing message: {e}")
                conn.rollback()
    
    except KeyboardInterrupt:
        print(f"\n\nStopped by user")
        print(f"Total records inserted: {count}")
    
    finally:
        cur.close()
        conn.close()
        consumer.close()
        print("Connections closed")

if __name__ == "__main__":
    main()