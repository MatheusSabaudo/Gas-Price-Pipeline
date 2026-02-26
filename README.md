# Gas-Price-Pipeline
Real-time natural gas price monitoring pipeline built with Kafka, Python, and PostgreSQL. Simulates price data, detects anomalies using moving averages, and stores results for analysis. Demonstrates streaming data concepts, exactly-once processing, and basic ETL patterns.

Objective:
Build a simple data pipeline that ingests, processes, and stores simulated natural gas price data.

Requirements:
Part 1: Data Ingestion

- Create a Python script that generates simulated gas price data (timestamp, price, volume)
- Send this data to a Kafka topic named gas-prices

Part 2: Data Processing

- Create a consumer that reads from the Kafka topic
- Calculate a simple 5-minute moving average of gas prices
- Flag any price that moves more than 5% from the moving average as an "anomaly"

Part 3: Storage

- Store both raw prices and anomalies in PostgreSQL
- Design appropriate tables for this data

Part 4: Orchestration (Optional but bonus)

- Create an Airflow DAG that runs a daily summary of:
- Average price for the day
- Number of anomalies detected
- Total volume

Deliverables:
1. Working code with clear instructions to run it
2. Brief explanation of your design decisions
3. Diagram of your pipeline architecture

Time Expected: 3-4 hours
Tech Stack: Kafka, PostgreSQL, Python (Airflow optional)

Evaluation Criteria:
- Code quality and organization
- Proper use of Kafka (topics, producers, consumers)
- Database schema design
- Error handling
- Documentation
