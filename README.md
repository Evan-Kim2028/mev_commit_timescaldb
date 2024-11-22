# mev-commit-timescaldb

A data pipeline for collecting and analyzing MEV-Commit protocol data using TimescaleDB.

## Overview

This project implements a data pipeline that:
1. Collects event data from the MEV-Commit protocol
2. Stores it in TimescaleDB
3. Creates materialized views for efficient querying
4. Exposes the data via PostgREST API

## Architecture

### Data Pipeline
The pipeline consists of two main components:
1. **Main Event Pipeline** (`main.py`): Collects protocol events from both MEV-Commit and validator contracts
2. **L1 Transaction Pipeline** (`fetch_l1_txs.py`): Fetches corresponding L1 transaction data

### Materialized Views
The system maintains several materialized views for efficient querying:

1. **openedcommitmentstoredall**
   - Consolidates data from both v1 and v2 of opened commitments
   - Primary view for tracking all opened commitments
   - Indexed by `commitmentIndex` and `blocknumber`

2. **preconf_txs** (in api schema)
   - Combines data from multiple tables for preconfirmation transactions
   - Includes commitment details, L1 transaction data, and processed status
   - Calculates decay multipliers and ETH values
   - Indexed by `commitmentIndex` and `hash`

## Setup and Installation

### Prerequisites
- Docker and Docker Compose
- `.env` file with required environment variables

### Environment Variables
Create a `.env` file with the following variables:
```
DB_NAME=mev_commit_testnet
DB_USER=postgres
DB_PASSWORD=your_password
DB_PORT=5432
DB_HOST=localhost

# PostgREST Configuration
PGRST_DB_URI=postgres://postgres:your_password@timescaledb:5432/mev_commit_testnet
PGRST_DB_SCHEMA=api
PGRST_DB_ANON_ROLE=web_anon
PGRST_SERVER_PORT=3000
```

### Building and Running

1. Build and start the containers:
```bash
docker compose build
docker compose up -d
```

2. Check container status:
```bash
docker compose ps
```

3. View logs:
```bash
# All containers
docker compose logs -f

# Specific container
docker compose logs -f app
```

### Database Management

#### Connect to TimescaleDB
1. Connect to docker container:
```bash
docker exec -it mev_timescaledb /bin/bash
```

2. Connect to database:
```bash
psql -U postgres -d mev_commit_testnet
```

#### Reset Database
1. Connect to default database:
```bash
psql -U postgres -d postgres
```

2. Terminate existing connections:
```sql
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'mev_commit_testnet';
```

3. Drop and recreate database:
```sql
DROP DATABASE mev_commit_testnet;
CREATE DATABASE mev_commit_testnet;
```

## API Access

The PostgREST API is available at `http://localhost:3003` and provides RESTful access to the database views and tables.

Example queries:
```bash
# Get recent preconfirmation transactions
curl "http://localhost:3003/preconf_txs?order=block_number.desc&limit=10"

# Get commitments for specific bidder
curl "http://localhost:3003/preconf_txs?bidder=eq.0x..."
```

## Development

### Project Structure
```
├── pipeline/
│   ├── db.py           # Database operations
│   ├── events.py       # Event definitions
│   ├── queries.py      # Data fetching logic
│   └── materialized_views.py  # View management
├── main.py             # Main event pipeline
├── fetch_l1_txs.py     # L1 transaction pipeline
├── docker-compose.yml
└── Dockerfile
```

### Adding New Events
1. Define event configuration in `pipeline/events.py`
2. Add event to appropriate config in `main.py`
3. Update materialized views if needed in `pipeline/materialized_views.py`