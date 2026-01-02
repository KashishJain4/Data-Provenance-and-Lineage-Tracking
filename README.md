[README.md](https://github.com/user-attachments/files/24404807/README.md)
# Data Provenance and Lineage Tracking System

A FastAPI-based system for tracking data transformations and querying data lineage. This system maintains a JSON-based ledger of all data transformation steps, including source datasets, operations, timestamps, and metadata.

## Features

- **Transformation Recording**: Log each data transformation with source, operation, timestamp, and metadata
- **Lineage Querying**: Retrieve complete lineage history for any dataset ID
- **Lineage Graphs**: Get hierarchical lineage graphs showing all source datasets and transformations
- **REST API**: Simple RESTful API for all operations
- **SQLite Storage**: Lightweight, file-based database for lineage records

## Project Structure

```
.
├── main.py              # FastAPI application and API endpoints
├── database.py          # Database schema and operations
├── example_etl.py       # Example ETL script demonstrating lineage tracking
├── requirements.txt     # Python dependencies
├── README.md           # This file
└── lineage.db          # SQLite database (created on first run)
```

## Installation

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **The database will be automatically created on first run** (creates `lineage.db` in the project directory)

## Running the Service

Start the FastAPI server:

```bash
uvicorn main:app --reload
```

The API will be available at `http://localhost:8000`

- **Interactive API documentation**: http://localhost:8000/docs
- **Alternative API documentation**: http://localhost:8000/redoc

## API Endpoints

### 1. Record Transformation

Record a data transformation in the lineage ledger.

**Endpoint**: `POST /lineage/record`

**Request Body**:
```json
{
  "dataset_id": "dataset_123",
  "operation": "filter_by_age",
  "source_dataset_id": "dataset_122",
  "metadata": {
    "filter_condition": "age >= 30",
    "input_records": 100,
    "output_records": 75
  }
}
```

**Response**:
```json
{
  "status": "success",
  "record_id": 1,
  "dataset_id": "dataset_123",
  "message": "Transformation recorded successfully"
}
```

**Parameters**:
- `dataset_id` (required): Unique identifier for the transformed dataset
- `operation` (required): Description of the transformation operation
- `source_dataset_id` (optional): ID of the source dataset
- `metadata` (optional): Additional metadata as key-value pairs (stored as JSON)

### 2. Get Lineage

Retrieve the complete lineage for a given dataset ID.

**Endpoint**: `GET /lineage/{dataset_id}`

**Example**: `GET /lineage/final_dataset_001`

**Response**:
```json
{
  "dataset_id": "final_dataset_001",
  "records": [
    {
      "id": 5,
      "dataset_id": "final_dataset_001",
      "operation": "enrich_with_calculations",
      "source_dataset_id": "aggregated_dataset_001",
      "metadata": {
        "new_columns": ["purchase_per_customer"],
        "input_records": 4
      },
      "timestamp": "2024-01-15T10:30:00",
      "created_at": "2024-01-15T10:30:00"
    }
  ]
}
```

### 3. Get Lineage Graph

Get the full lineage graph by recursively following source_dataset_id links.

**Endpoint**: `GET /lineage/{dataset_id}/graph`

**Example**: `GET /lineage/final_dataset_001/graph`

**Response**:
```json
{
  "dataset_id": "final_dataset_001",
  "graph": {
    "dataset_id": "final_dataset_001",
    "operations": [
      {
        "operation": "enrich_with_calculations",
        "timestamp": "2024-01-15T10:30:00",
        "metadata": {...}
      }
    ],
    "sources": [
      {
        "dataset_id": "aggregated_dataset_001",
        "operations": [...],
        "sources": [...]
      }
    ]
  }
}
```

### 4. List All Datasets

Get a list of all unique dataset IDs in the system.

**Endpoint**: `GET /datasets`

**Response**:
```json
[
  "raw_dataset_001",
  "clean_dataset_001",
  "filtered_dataset_001",
  "aggregated_dataset_001",
  "final_dataset_001"
]
```

### 5. Health Check

Check the service health status.

**Endpoint**: `GET /health`

**Response**:
```json
{
  "status": "healthy",
  "service": "Data Lineage Tracking API"
}
```

## Example Usage

### Running the Example ETL Script

The included `example_etl.py` demonstrates a complete ETL pipeline with lineage tracking:

1. **Start the FastAPI server** (in one terminal):
   ```bash
   uvicorn main:app --reload
   ```

2. **Run the ETL script** (in another terminal):
   ```bash
   python example_etl.py
   ```

This will:
- Create sample data
- Perform multiple transformations (load, clean, filter, aggregate, enrich)
- Record each transformation step in the lineage system

3. **Query lineage**:
   ```bash
   # Query lineage for a specific dataset
   python example_etl.py query final_dataset_001
   
   # Query full lineage graph
   python example_etl.py query final_dataset_001 graph
   ```

### Using the API with Python Requests

```python
import requests

# Record a transformation
response = requests.post("http://localhost:8000/lineage/record", json={
    "dataset_id": "my_dataset",
    "operation": "join_tables",
    "source_dataset_id": "dataset_a",
    "metadata": {
        "joined_with": "dataset_b",
        "join_key": "customer_id"
    }
})

# Query lineage
response = requests.get("http://localhost:8000/lineage/my_dataset")
lineage = response.json()
print(lineage)
```

### Using the API with cURL

```bash
# Record a transformation
curl -X POST "http://localhost:8000/lineage/record" \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_id": "my_dataset",
    "operation": "filter_data",
    "source_dataset_id": "source_dataset",
    "metadata": {"filter": "age > 25"}
  }'

# Query lineage
curl "http://localhost:8000/lineage/my_dataset"

# Get lineage graph
curl "http://localhost:8000/lineage/my_dataset/graph"

# List all datasets
curl "http://localhost:8000/datasets"
```

## Database Schema

The SQLite database uses a single table `lineage_records`:

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key (auto-increment) |
| dataset_id | TEXT | Unique identifier for the dataset |
| operation | TEXT | Description of the transformation operation |
| source_dataset_id | TEXT | ID of the source dataset (nullable) |
| metadata | TEXT | JSON string containing additional metadata |
| timestamp | TEXT | ISO format timestamp of the transformation |
| created_at | TEXT | Database insertion timestamp |

Indexes are created on `dataset_id` and `timestamp` for efficient querying.

## Architecture

- **FastAPI**: Modern, fast web framework for building APIs
- **SQLite**: Lightweight, file-based database (no separate server required)
- **Pydantic**: Data validation and serialization
- **JSON-based ledger**: All metadata stored as JSON for flexibility

## Notes

- The database file (`lineage.db`) is created automatically on first run
- Timestamps are stored in ISO format (UTC)
- Metadata can contain any JSON-serializable data structure
- The lineage graph endpoint recursively follows `source_dataset_id` links to build a complete graph

## Development

To run in development mode with auto-reload:

```bash
uvicorn main:app --reload
```

For production deployment, consider using a production ASGI server like Gunicorn with Uvicorn workers.

