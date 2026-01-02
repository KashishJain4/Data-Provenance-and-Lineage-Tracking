"""
Database schema and operations for data lineage tracking.
"""
import sqlite3
import json
from datetime import datetime
from typing import Optional, List, Dict, Any
from contextlib import contextmanager


class LineageDB:
    """Database manager for lineage tracking."""
    
    def __init__(self, db_path: str = "lineage.db"):
        """
        Initialize the lineage database.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self._init_schema()
    
    def _init_schema(self):
        """Initialize the database schema."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS lineage_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_id TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    source_dataset_id TEXT,
                    metadata TEXT,
                    timestamp TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Create indexes separately (SQLite doesn't support inline INDEX in CREATE TABLE)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_dataset_id ON lineage_records(dataset_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_timestamp ON lineage_records(timestamp)
            """)
            conn.commit()
    
    @contextmanager
    def _get_connection(self):
        """Get a database connection with proper cleanup."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def record_transformation(
        self,
        dataset_id: str,
        operation: str,
        source_dataset_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Record a data transformation in the lineage ledger.
        
        Args:
            dataset_id: Unique identifier for the transformed dataset
            operation: Description of the transformation operation
            source_dataset_id: ID of the source dataset (if applicable)
            metadata: Additional metadata as a dictionary (will be stored as JSON)
            
        Returns:
            The ID of the inserted record
        """
        timestamp = datetime.utcnow().isoformat()
        metadata_json = json.dumps(metadata) if metadata else None
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO lineage_records 
                (dataset_id, operation, source_dataset_id, metadata, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """, (dataset_id, operation, source_dataset_id, metadata_json, timestamp))
            conn.commit()
            return cursor.lastrowid
    
    def get_lineage(self, dataset_id: str) -> List[Dict[str, Any]]:
        """
        Retrieve the complete lineage for a given dataset ID.
        
        Args:
            dataset_id: The dataset ID to query lineage for
            
        Returns:
            List of lineage records (ordered by timestamp)
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    id,
                    dataset_id,
                    operation,
                    source_dataset_id,
                    metadata,
                    timestamp,
                    created_at
                FROM lineage_records
                WHERE dataset_id = ?
                ORDER BY timestamp ASC
            """, (dataset_id,))
            
            records = []
            for row in cursor.fetchall():
                record = {
                    "id": row["id"],
                    "dataset_id": row["dataset_id"],
                    "operation": row["operation"],
                    "source_dataset_id": row["source_dataset_id"],
                    "metadata": json.loads(row["metadata"]) if row["metadata"] else None,
                    "timestamp": row["timestamp"],
                    "created_at": row["created_at"]
                }
                records.append(record)
            
            return records
    
    def get_full_lineage_graph(self, dataset_id: str) -> Dict[str, Any]:
        """
        Get the full lineage graph by recursively following source_dataset_id links.
        
        Args:
            dataset_id: The dataset ID to query lineage for
            
        Returns:
            Dictionary with the lineage graph structure
        """
        visited = set()
        graph = {"dataset_id": dataset_id, "operations": [], "sources": []}
        
        def build_graph(current_id: str, parent_graph: Dict):
            if current_id in visited:
                return
            visited.add(current_id)
            
            records = self.get_lineage(current_id)
            for record in records:
                parent_graph["operations"].append({
                    "operation": record["operation"],
                    "timestamp": record["timestamp"],
                    "metadata": record["metadata"]
                })
                
                if record["source_dataset_id"]:
                    source_graph = {
                        "dataset_id": record["source_dataset_id"],
                        "operations": [],
                        "sources": []
                    }
                    parent_graph["sources"].append(source_graph)
                    build_graph(record["source_dataset_id"], source_graph)
        
        build_graph(dataset_id, graph)
        return graph
    
    def list_all_datasets(self) -> List[str]:
        """
        Get a list of all unique dataset IDs in the system.
        
        Returns:
            List of unique dataset IDs
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT dataset_id
                FROM lineage_records
                ORDER BY dataset_id
            """)
            return [row[0] for row in cursor.fetchall()]

