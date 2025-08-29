"""
Session management utilities for the Flyover data descriptor.

This module contains the Cache class and related session handling functionality.
"""

import os
from typing import Any, Optional


class Cache:
    """
    Session cache class for storing application state and user data.
    
    This class manages the application's session state including database connections,
    file paths, processing status, and user-specific data throughout the workflow.
    
    Attributes:
        repo (str): Repository identifier from environment or default
        file_path (Optional[str]): Path to uploaded file
        table (Optional[str]): Database table name
        url (Optional[str]): Database connection URL
        username (Optional[str]): Database username
        password (Optional[str]): Database password
        db_name (Optional[str]): Database name
        conn (Optional[Any]): Database connection object
        col_cursor (Optional[Any]): Database cursor object
        csvData (Optional[Any]): CSV data content
        csvPath (Optional[str]): Path to CSV file
        uploaded_file (Optional[Any]): Uploaded file object
        global_semantic_map (Optional[dict]): Global semantic mapping data
        existing_graph (bool): Flag indicating if graph already exists
        databases (Optional[list]): List of database names
        descriptive_info (Optional[dict]): Variable descriptive information
        DescriptiveInfoDetails (Optional[dict]): Detailed descriptive information
        StatusToDisplay (Optional[str]): Status message for UI display
        pk_fk_data (Optional[dict]): Primary key/foreign key relationship data
        pk_fk_status (Optional[str]): PK/FK processing status ("processing", "success", "failed")
        cross_graph_link_data (Optional[dict]): Cross-graph relationship data
        cross_graph_link_status (Optional[str]): Cross-graph processing status
        annotation_status (Optional[dict]): Annotation processing results
        annotation_json_path (Optional[str]): Path to uploaded annotation JSON file
    """
    
    def __init__(self) -> None:
        """
        Initialize the Cache with default values.
        
        Sets up all session variables with appropriate default values.
        The repo value is inherited from the global environment configuration.
        """
        # Get repo from environment or use default
        # This will be set by the main application during initialization
        self.repo: str = os.getenv('FLYOVER_REPOSITORY_NAME', 'userRepo')
        
        # File and database connection attributes
        self.file_path: Optional[str] = None
        self.table: Optional[str] = None
        self.url: Optional[str] = None
        self.username: Optional[str] = None
        self.password: Optional[str] = None
        self.db_name: Optional[str] = None
        self.conn: Optional[Any] = None
        self.col_cursor: Optional[Any] = None
        
        # Data processing attributes
        self.csvData: Optional[Any] = None
        self.csvPath: Optional[str] = None
        self.uploaded_file: Optional[Any] = None
        self.global_semantic_map: Optional[dict] = None
        
        # State management attributes
        self.existing_graph: bool = False
        self.databases: Optional[list] = None
        self.descriptive_info: Optional[dict] = None
        self.DescriptiveInfoDetails: Optional[dict] = None
        self.StatusToDisplay: Optional[str] = None
        
        # Relationship processing attributes
        self.pk_fk_data: Optional[dict] = None
        self.pk_fk_status: Optional[str] = None  # "processing", "success", "failed"
        self.cross_graph_link_data: Optional[dict] = None
        self.cross_graph_link_status: Optional[str] = None
        
        # Annotation processing attributes
        self.annotation_status: Optional[dict] = None  # Store annotation results
        self.annotation_json_path: Optional[str] = None  # Store path to uploaded JSON file