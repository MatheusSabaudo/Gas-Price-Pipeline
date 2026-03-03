# italian_gas_producer.py
import json
import random
from datetime import datetime, timedelta
from confluent_kafka import Producer

# ==================== CONFIGURATION DATA ====================

KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9093',
    'enable.idempotence': False
}

REGIONI_ITALIANE = [
    # Nord Italia
    {'region_id': 'LOM', 'region_name': 'Lombardia', 'zona': 'Nord', 'base_price': 1.85, 'tax_factor': 1.02},
    {'region_id': 'VEN', 'region_name': 'Veneto', 'zona': 'Nord', 'base_price': 1.82, 'tax_factor': 1.01},
    {'region_id': 'PIE', 'region_name': 'Piemonte', 'zona': 'Nord', 'base_price': 1.80, 'tax_factor': 1.00},
    {'region_id': 'LIG', 'region_name': 'Liguria', 'zona': 'Nord', 'base_price': 1.88, 'tax_factor': 1.03},
    {'region_id': 'EMR', 'region_name': 'Emilia-Romagna', 'zona': 'Nord', 'base_price': 1.83, 'tax_factor': 1.01},
    {'region_id': 'LAZ', 'region_name': 'Lazio', 'zona': 'Centro', 'base_price': 1.84, 'tax_factor': 1.02},
    {'region_id': 'TOS', 'region_name': 'Toscana', 'zona': 'Centro', 'base_price': 1.83, 'tax_factor': 1.01},
    {'region_id': 'CAM', 'region_name': 'Campania', 'zona': 'Sud', 'base_price': 1.78, 'tax_factor': 0.98},
    {'region_id': 'PUG', 'region_name': 'Puglia', 'zona': 'Sud', 'base_price': 1.76, 'tax_factor': 0.97},
    {'region_id': 'SIC', 'region_name': 'Sicilia', 'zona': 'Isole', 'base_price': 1.82, 'tax_factor': 1.01},
    {'region_id': 'SAR', 'region_name': 'Sardegna', 'zona': 'Isole', 'base_price': 1.83, 'tax_factor': 1.02},
]

STAZIONI_ITALIA = [
    {'station_id': 'ENI001', 'station_name': 'Eni', 'brand': 'Eni', 'premium_factor': 1.02},
    {'station_id': 'Q8001', 'station_name': 'Q8', 'brand': 'Q8', 'premium_factor': 1.03},
    {'station_id': 'ESS001', 'station_name': 'Esso', 'brand': 'Esso', 'premium_factor': 1.02},
    {'station_id': 'TPL001', 'station_name': 'Tamoil', 'brand': 'Tamoil', 'premium_factor': 0.99},
    {'station_id': 'API001', 'station_name': 'API', 'brand': 'API', 'premium_factor': 0.98},
    {'station_id': 'IP001', 'station_name': 'IP', 'brand': 'IP', 'premium_factor': 1.00},
    {'station_id': 'CON001', 'station_name': 'Conad', 'brand': 'Conad', 'premium_factor': 0.95},
    {'station_id': 'COO001', 'station_name': 'Coop', 'brand': 'Coop', 'premium_factor': 0.95},
]

TIPI_CARBURANTE = [
    {'fuel_type_id': 'BEN', 'fuel_type': 'Benzina', 'multiplier': 1.00, 'unit': 'euro/litro'},
    {'fuel_type_id': 'DIE', 'fuel_type': 'Gasolio', 'multiplier': 0.95, 'unit': 'euro/litro'},
    {'fuel_type_id': 'SUP', 'fuel_type': 'Benzina Premium', 'multiplier': 1.15, 'unit': 'euro/litro'},
    {'fuel_type_id': 'GPL', 'fuel_type': 'GPL', 'multiplier': 0.55, 'unit': 'euro/litro'},
]

# ==================== GENERATION FUNCTIONS ====================

def generate_price(regione, stazione, carburante, timestamp):
    """Calculate price based on region, station, fuel type and time"""
    
    price = regione['base_price']
    price *= carburante['multiplier']
    price *= regione['tax_factor']
    price *= stazione['premium_factor']
    
    # Time adjustments
    month = timestamp.month
    hour = timestamp.hour
    
    # Seasonal
    if 6 <= month <= 8:  # Summer
        price *= random.uniform(1.05, 1.12)
    elif month in [12, 1, 2]:  # Winter
        price *= random.uniform(0.98, 1.02)
    
    # Daily pattern
    if 7 <= hour <= 9 or 17 <= hour <= 19:  # Rush hours
        price *= random.uniform(1.01, 1.03)
    elif 22 <= hour or hour <= 5:  # Night
        price *= random.uniform(0.97, 0.99)
    
    # Weekend
    if timestamp.weekday() >= 5:
        price *= random.uniform(1.00, 1.02)
    
    # Random noise
    price += random.gauss(0, 0.015)
    
    # Ensure reasonable range
    if carburante['fuel_type_id'] == 'GPL':
        return round(max(0.65, min(1.10, price)), 3)
    else:
        return round(max(1.55, min(2.35, price)), 3)


def generate_record(timestamp=None):
    """Generate a single gas price record"""
    
    if timestamp is None:
        timestamp = datetime.now()
    
    # Random selections
    regione = random.choice(REGIONI_ITALIANE)
    stazione = random.choice(STAZIONI_ITALIA)
    carburante = random.choice(TIPI_CARBURANTE)
    
    # Calculate price
    price = generate_price(regione, stazione, carburante, timestamp)
    
    # Create record
    record = {
        # Kafka key
        'kafka_key': f"{regione['region_id']}_{stazione['station_id']}_{carburante['fuel_type_id']}",
        
        # Timestamp
        'timestamp': timestamp.isoformat(),
        'date': timestamp.strftime('%Y-%m-%d'),
        'time': timestamp.strftime('%H:%M:%S'),
        'hour': timestamp.hour,
        'day_of_week': timestamp.strftime('%A'),
        'month': timestamp.month,
        'year': timestamp.year,
        
        # Location
        'region_id': regione['region_id'],
        'region_name': regione['region_name'],
        'region_zone': regione['zona'],
        'station_id': stazione['station_id'],
        'station_name': stazione['station_name'],
        'station_brand': stazione['brand'],
        
        # Fuel
        'fuel_type_id': carburante['fuel_type_id'],
        'fuel_type_name': carburante['fuel_type'],
        'price': price,
        'price_unit': carburante['unit'],
        'currency': 'EUR',
        
        # Context
        'is_weekend': 1 if timestamp.weekday() >= 5 else 0,
        'is_rush_hour': 1 if (7 <= timestamp.hour <= 9 or 17 <= timestamp.hour <= 19) else 0,
    }
    
    # Add anomaly (10% chance)
    if random.random() < 0.1:
        record['is_anomaly'] = 1
        if random.choice(['spike', 'drop']) == 'spike':
            record['price'] = round(record['price'] * 1.4, 3)
            record['anomaly_type'] = 'price_spike'
        else:
            record['price'] = round(record['price'] * 0.6, 3)
            record['anomaly_type'] = 'price_drop'
    else:
        record['is_anomaly'] = 0
        record['anomaly_type'] = None
    
    return record


def generate_batch(num_records, start_date=None, end_date=None):
    """Generate a batch of records"""
    
    records = []
    
    if start_date and end_date:
        # Historical data
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        days_range = (end - start).days
        
        for i in range(num_records):
            if i % 100 == 0:
                print(f"  Generating record {i}/{num_records}")
            random_days = random.randint(0, days_range)
            random_hours = random.randint(0, 23)
            random_minutes = random.randint(0, 59)
            timestamp = start + timedelta(days=random_days, hours=random_hours, minutes=random_minutes)
            records.append(generate_record(timestamp))
    else:
        # Recent data
        now = datetime.now()
        for i in range(num_records):
            if i % 100 == 0:
                print(f"  Generating record {i}/{num_records}")
            time_offset = random.randint(-86400, 0)  # Last 24 hours
            timestamp = now + timedelta(seconds=time_offset)
            records.append(generate_record(timestamp))
    
    # Sort by timestamp
    records.sort(key=lambda x: x['timestamp'])
    return records


# ==================== KAFKA FUNCTIONS ====================

def delivery_report(err, msg):
    """Kafka delivery callback"""
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Sent to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def send_to_kafka(records, bootstrap_servers, topic):
    """Send records to Kafka"""
    
    print(f"\nConnecting to Kafka at {bootstrap_servers}...")
    
    # Configure producer
    producer_config = {
        'bootstrap.servers': bootstrap_servers,
        'enable.idempotence': False
    }
    producer = Producer(producer_config)
    
    success = 0
    failed = 0
    
    print(f"Sending {len(records)} records to topic '{topic}'...")
    
    for i, record in enumerate(records, 1):
        try:
            # Send to Kafka
            producer.produce(
                topic=topic,
                key=record['kafka_key'],
                value=json.dumps(record, ensure_ascii=False),
                callback=delivery_report
            )
            producer.poll(0)  # Trigger callbacks
            success += 1
            
            # Progress
            if i % 10 == 0:  # Show progress every 10 records
                print(f"  Progress: {i}/{len(records)} sent")
                
        except Exception as e:
            print(f"Failed to send record {i}: {e}")
            failed += 1
    
    # Flush
    print("  Flushing remaining messages...")
    producer.flush()
    print(f"\nComplete: {success} sent, {failed} failed")
    
    return success, failed


# ==================== MAIN ====================

def main():
    print("=" * 60)
    print("ITALIAN GAS PRICE PRODUCER")
    print("=" * 60)
    
    # Get configuration from user
    print("\nCONFIGURATION")
    
    # Data settings
    num_records = int(input("Number of records to generate [1000]: ") or "1000")
    
    print("\nDate range (press Enter for current/last 24h):")
    start_date = input("Start date (YYYY-MM-DD) [optional]: ") or None
    end_date = input("End date (YYYY-MM-DD) [optional]: ") or None
    
    if start_date and not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    # Output options
    preview = input("Preview first 5 records? (y/n) [n]: ").lower() == 'y'
    
    print("\n" + "=" * 60)
    print("GENERATING DATA...")
    print("=" * 60)
    
    # Generate data
    records = generate_batch(num_records, start_date, end_date)
    print(f"\nGenerated {len(records)} records")
    
    if start_date and end_date:
        print(f"Date range: {records[0]['date']} to {records[-1]['date']}")
    
    # Preview
    if preview:
        print("\nFirst 5 records:")
        for i, r in enumerate(records[:5], 1):
            print(f"\n{i}. {r['timestamp']}")
            print(f"   Location: {r['region_name']} - {r['station_name']}")
            print(f"   Fuel: {r['fuel_type_name']} - €{r['price']} {r['price_unit']}")
            print(f"   Anomaly: {'⚠️ YES' if r['is_anomaly'] else 'No'}")
    
    # Send to Kafka
    send_now = input("\nSend to Kafka now? (y/n) [y]: ").lower() != 'n'
    
    if send_now:
        topic = "gas-prices"
        bootstrap_servers = KAFKA_CONFIG['bootstrap.servers']
        
        send_to_kafka(records, bootstrap_servers, topic)
    else:
        print("\nSkipped Kafka sending")
    
    print("\nDone!")


if __name__ == "__main__":
    main()