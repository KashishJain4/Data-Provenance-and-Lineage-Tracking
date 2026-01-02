"""
Example ETL script demonstrating data lineage tracking.
This script performs a series of transformations and records each step in the lineage system.
"""
import pandas as pd
import requests
import json
from datetime import datetime
from typing import Dict, Any


# API endpoint (update if running on different host/port)
API_BASE_URL = "http://localhost:8000"


def record_transformation(
    dataset_id: str,
    operation: str,
    source_dataset_id: str = None,
    metadata: Dict[str, Any] = None
) -> bool:
    """
    Record a transformation in the lineage system.
    
    Args:
        dataset_id: ID of the transformed dataset
        operation: Description of the operation
        source_dataset_id: ID of the source dataset
        metadata: Additional metadata
        
    Returns:
        True if successful, False otherwise
    """
    try:
        payload = {
            "dataset_id": dataset_id,
            "operation": operation,
            "source_dataset_id": source_dataset_id,
            "metadata": metadata
        }
        response = requests.post(f"{API_BASE_URL}/lineage/record", json=payload)
        response.raise_for_status()
        result = response.json()
        print(f"✓ Recorded: {dataset_id} - {operation}")
        return True
    except Exception as e:
        print(f"✗ Error recording transformation: {e}")
        return False


def run_etl_pipeline():
    """
    Example ETL pipeline demonstrating lineage tracking.
    
    Simulates a data pipeline:
    1. Load raw data (raw_dataset_001)
    2. Clean data (clean_dataset_001)
    3. Filter data (filtered_dataset_001)
    4. Aggregate data (aggregated_dataset_001)
    5. Enrich data (final_dataset_001)
    """
    print("=" * 60)
    print("Example ETL Pipeline with Lineage Tracking")
    print("=" * 60)
    print()
    
    # Step 1: Load raw data
    print("Step 1: Loading raw data...")
    raw_data = pd.DataFrame({
        "customer_id": range(1, 101),
        "age": [20 + i % 60 for i in range(100)],
        "purchase_amount": [10.0 + i * 2.5 for i in range(100)],
        "region": ["North", "South", "East", "West"] * 25
    })
    
    record_transformation(
        dataset_id="raw_dataset_001",
        operation="load_from_source",
        metadata={
            "source": "external_api",
            "record_count": len(raw_data),
            "columns": list(raw_data.columns),
            "description": "Initial data load from external source"
        }
    )
    
    # Step 2: Clean data (remove duplicates, handle missing values)
    print("\nStep 2: Cleaning data...")
    clean_data = raw_data.drop_duplicates()
    clean_data = clean_data.fillna(0)
    
    record_transformation(
        dataset_id="clean_dataset_001",
        operation="clean_data",
        source_dataset_id="raw_dataset_001",
        metadata={
            "operations": ["remove_duplicates", "fill_missing_values"],
            "input_records": len(raw_data),
            "output_records": len(clean_data),
            "records_removed": len(raw_data) - len(clean_data)
        }
    )
    
    # Step 3: Filter data (age >= 30)
    print("\nStep 3: Filtering data...")
    filtered_data = clean_data[clean_data["age"] >= 30]
    
    record_transformation(
        dataset_id="filtered_dataset_001",
        operation="filter_by_age",
        source_dataset_id="clean_dataset_001",
        metadata={
            "filter_condition": "age >= 30",
            "input_records": len(clean_data),
            "output_records": len(filtered_data),
            "records_filtered": len(clean_data) - len(filtered_data)
        }
    )
    
    # Step 4: Aggregate data (group by region)
    print("\nStep 4: Aggregating data...")
    aggregated_data = filtered_data.groupby("region").agg({
        "purchase_amount": ["sum", "mean", "count"],
        "age": "mean"
    }).reset_index()
    aggregated_data.columns = ["region", "total_purchase", "avg_purchase", "customer_count", "avg_age"]
    
    record_transformation(
        dataset_id="aggregated_dataset_001",
        operation="aggregate_by_region",
        source_dataset_id="filtered_dataset_001",
        metadata={
            "group_by": "region",
            "aggregations": {
                "purchase_amount": ["sum", "mean", "count"],
                "age": "mean"
            },
            "input_records": len(filtered_data),
            "output_records": len(aggregated_data)
        }
    )
    
    # Step 5: Enrich with additional calculations
    print("\nStep 5: Enriching data...")
    final_data = aggregated_data.copy()
    final_data["purchase_per_customer"] = final_data["total_purchase"] / final_data["customer_count"]
    
    record_transformation(
        dataset_id="final_dataset_001",
        operation="enrich_with_calculations",
        source_dataset_id="aggregated_dataset_001",
        metadata={
            "new_columns": ["purchase_per_customer"],
            "calculations": ["total_purchase / customer_count"],
            "input_records": len(aggregated_data),
            "output_records": len(final_data)
        }
    )
    
    print("\n" + "=" * 60)
    print("ETL Pipeline Completed!")
    print("=" * 60)
    print(f"\nFinal dataset shape: {final_data.shape}")
    print(f"\nFinal dataset preview:")
    print(final_data.head())
    print("\n" + "=" * 60)
    print("Lineage has been recorded for all transformation steps.")
    print("Query the API to view the complete lineage graph.")
    print("=" * 60)


def query_lineage(dataset_id: str):
    """
    Query and display the lineage for a dataset.
    
    Args:
        dataset_id: The dataset ID to query
    """
    try:
        response = requests.get(f"{API_BASE_URL}/lineage/{dataset_id}")
        response.raise_for_status()
        lineage = response.json()
        
        print(f"\nLineage for dataset: {dataset_id}")
        print("-" * 60)
        for record in lineage["records"]:
            print(f"\nOperation: {record['operation']}")
            print(f"Timestamp: {record['timestamp']}")
            if record['source_dataset_id']:
                print(f"Source: {record['source_dataset_id']}")
            if record['metadata']:
                print(f"Metadata: {json.dumps(record['metadata'], indent=2)}")
        print("-" * 60)
    except Exception as e:
        print(f"Error querying lineage: {e}")


def query_lineage_graph(dataset_id: str):
    """
    Query and display the full lineage graph for a dataset.
    
    Args:
        dataset_id: The dataset ID to query
    """
    try:
        response = requests.get(f"{API_BASE_URL}/lineage/{dataset_id}/graph")
        response.raise_for_status()
        graph_data = response.json()
        
        print(f"\nFull Lineage Graph for dataset: {dataset_id}")
        print("-" * 60)
        print(json.dumps(graph_data["graph"], indent=2))
        print("-" * 60)
    except Exception as e:
        print(f"Error querying lineage graph: {e}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "query":
        # Query mode
        dataset_id = sys.argv[2] if len(sys.argv) > 2 else "final_dataset_001"
        if len(sys.argv) > 3 and sys.argv[3] == "graph":
            query_lineage_graph(dataset_id)
        else:
            query_lineage(dataset_id)
    else:
        # Run ETL pipeline
        print("\nNote: Make sure the FastAPI server is running on http://localhost:8000")
        print("Start it with: uvicorn main:app --reload\n")
        
        run_etl_pipeline()
        
        # Optionally query the final dataset lineage
        print("\n\nQuerying lineage for final_dataset_001...")
        query_lineage("final_dataset_001")

