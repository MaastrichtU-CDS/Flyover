"""Progress tracking utility for triplifier operations."""
from __future__ import annotations

from dataclasses import dataclass, field
from threading import Lock
from typing import Dict, Optional
import time


@dataclass
class ProgressData:
    """Data structure for tracking progress of a single task."""
    
    table: str
    current_row: int
    total_rows: int
    percentage: float
    status: str
    timestamp: float = field(default_factory=time.time)
    
    def to_dict(self) -> Dict[str, any]:
        """Convert progress data to dictionary for JSON serialisation."""
        return {
            'table': self.table,
            'current_row': self.current_row,
            'total_rows': self.total_rows,
            'percentage': round(self.percentage, 2),
            'status': self.status,
            'timestamp': self.timestamp
        }


class ProgressTracker:
    """
    Thread-safe progress tracker for monitoring triplification progress.
    
    This class maintains a registry of active triplification tasks and their
    progress status, allowing multiple concurrent operations to be tracked
    independently.
    
    Attributes:
        progress_data: Dictionary mapping task IDs to their progress information
        lock: Threading lock for thread-safe operations
    """
    
    def __init__(self) -> None:
        """Initialize the progress tracker with empty progress data and a lock."""
        self.progress_data: Dict[str, ProgressData] = {}
        self.lock = Lock()
    
    def start_task(self, task_id: str, table_name: str, total_rows: int) -> None:
        """
        Register a new task and initialize its progress tracking.
        
        Args:
            task_id: Unique identifier for the task
            table_name: Name of the table being processed
            total_rows: Total number of rows to process
        """
        with self.lock:
            self.progress_data[task_id] = ProgressData(
                table=table_name,
                current_row=0,
                total_rows=total_rows,
                percentage=0.0,
                status='starting'
            )
    
    def update_progress(self, task_id: str, current_row: int, status: str = 'processing') -> None:
        """
        Update the progress of an existing task.
        
        Args:
            task_id: Unique identifier for the task
            current_row: Current row being processed
            status: Current status of the task (processing, completed, failed)
        """
        with self.lock:
            if task_id in self.progress_data:
                progress = self.progress_data[task_id]
                progress.current_row = current_row
                progress.status = status
                if progress.total_rows > 0:
                    progress.percentage = (current_row / progress.total_rows) * 100
                progress.timestamp = time.time()
    
    def complete_task(self, task_id: str, success: bool = True) -> None:
        """
        Mark a task as completed.
        
        Args:
            task_id: Unique identifier for the task
            success: Whether the task completed successfully
        """
        with self.lock:
            if task_id in self.progress_data:
                progress = self.progress_data[task_id]
                progress.status = 'completed' if success else 'failed'
                if success:
                    progress.current_row = progress.total_rows
                    progress.percentage = 100.0
                progress.timestamp = time.time()
    
    def get_progress(self, task_id: str) -> Optional[Dict[str, any]]:
        """
        Retrieve the current progress of a task.
        
        Args:
            task_id: Unique identifier for the task
            
        Returns:
            Dictionary containing progress information, or None if task not found
        """
        with self.lock:
            if task_id in self.progress_data:
                return self.progress_data[task_id].to_dict()
            return None
    
    def remove_task(self, task_id: str) -> None:
        """
        Remove a task from the tracker (cleanup after completion).
        
        Args:
            task_id: Unique identifier for the task
        """
        with self.lock:
            if task_id in self.progress_data:
                del self.progress_data[task_id]
    
    def get_all_tasks(self) -> Dict[str, Dict[str, any]]:
        """
        Retrieve progress information for all active tasks.
        
        Returns:
            Dictionary mapping task IDs to their progress information
        """
        with self.lock:
            return {task_id: progress.to_dict() 
                    for task_id, progress in self.progress_data.items()}


# Global progress tracker instance
progress_tracker = ProgressTracker()
