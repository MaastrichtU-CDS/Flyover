"""
Application configuration and setup utilities for the Flyover data descriptor.

This module contains application-level configuration functions including logging setup.
"""

import logging
from typing import None


def setup_logging() -> None:
    """
    Setup centralised logging with timestamp format.
    
    Configures the logging system with INFO level, structured formatting,
    and console output handler for the entire application.
    
    Returns:
        None
    """
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S %z',
        handlers=[
            logging.StreamHandler()
        ]
    )