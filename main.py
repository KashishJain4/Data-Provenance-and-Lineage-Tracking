"""
FastAPI service for data provenance and lineage tracking.
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from datetime import datetime
from database import LineageDB

app = FastAPI(
    title="Data Lineage Tracking API",
    description="REST API for tracking and querying data provenance and lineage",
    version="1.0.0"
)

# Initialize the database
db = LineageDB()


class TransformationRecord(BaseModel):
    """Model for recording a transformation."""
    dataset_id: str
    operation: str
    source_dataset_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class LineageResponse(BaseModel):
    """Model for lineage query response."""
    dataset_id: str
    records: List[Dict[str, Any]]


class LineageGraphResponse(BaseModel):
    """Model for lineage graph response."""
    dataset_id: str
    graph: Dict[str, Any]


@app.post("/lineage/record", response_model=Dict[str, Any])
async def record_transformation(record: TransformationRecord):
    """
    Record a data transformation in the lineage ledger.
    
    - **dataset_id**: Unique identifier for the transformed dataset
    - **operation**: Description of the transformation operation (e.g., "filter", "join", "aggregate")
    - **source_dataset_id**: ID of the source dataset (optional)
    - **metadata**: Additional metadata as key-value pairs (optional)
    
    Returns the record ID and timestamp of the recorded transformation.
    """
    try:
        record_id = db.record_transformation(
            dataset_id=record.dataset_id,
            operation=record.operation,
            source_dataset_id=record.source_dataset_id,
            metadata=record.metadata
        )
        return {
            "status": "success",
            "record_id": record_id,
            "dataset_id": record.dataset_id,
            "message": "Transformation recorded successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error recording transformation: {str(e)}")


@app.get("/lineage/{dataset_id}", response_model=LineageResponse)
async def get_lineage(dataset_id: str):
    """
    Retrieve the complete lineage for a given dataset ID.
    
    Returns all transformation records associated with the dataset,
    ordered by timestamp (oldest first).
    """
    try:
        records = db.get_lineage(dataset_id)
        if not records:
            raise HTTPException(
                status_code=404,
                detail=f"No lineage records found for dataset_id: {dataset_id}"
            )
        return LineageResponse(dataset_id=dataset_id, records=records)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving lineage: {str(e)}")


@app.get("/lineage/{dataset_id}/graph", response_model=LineageGraphResponse)
async def get_lineage_graph(dataset_id: str):
    """
    Get the full lineage graph by recursively following source_dataset_id links.
    
    Returns a hierarchical structure showing the complete data lineage graph,
    including all source datasets and their transformations.
    """
    try:
        records = db.get_lineage(dataset_id)
        if not records:
            raise HTTPException(
                status_code=404,
                detail=f"No lineage records found for dataset_id: {dataset_id}"
            )
        graph = db.get_full_lineage_graph(dataset_id)
        return LineageGraphResponse(dataset_id=dataset_id, graph=graph)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving lineage graph: {str(e)}")


@app.get("/datasets", response_model=List[str])
async def list_datasets():
    """
    Get a list of all unique dataset IDs in the system.
    
    Returns a list of all dataset IDs that have lineage records.
    """
    try:
        datasets = db.list_all_datasets()
        return datasets
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing datasets: {str(e)}")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "Data Lineage Tracking API"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

