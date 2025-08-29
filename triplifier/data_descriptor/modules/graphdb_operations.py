"""
GraphDB operations utilities for the Flyover data descriptor.

This module contains functions for interacting with GraphDB repositories,
including graph existence checking and SPARQL query execution.
"""

import os
import requests
import logging
from typing import Optional, Any
from flask import flash, render_template, jsonify

# Use the centrally configured logger
logger = logging.getLogger(__name__)


def get_graphdb_config() -> tuple[str, str]:
    """
    Get GraphDB configuration from environment variables or defaults.
    
    Returns:
        tuple: (graphdb_url, repo) configuration values
    """
    if os.getenv('FLYOVER_GRAPHDB_URL') and os.getenv('FLYOVER_REPOSITORY_NAME'):
        # Running in Docker
        graphdb_url = os.getenv('FLYOVER_GRAPHDB_URL')
        repo = os.getenv('FLYOVER_REPOSITORY_NAME')
    else:
        # Not running in Docker
        graphdb_url = 'http://localhost:7200'
        repo = 'userRepo'
    
    return graphdb_url, repo


def api_check_graph_exists(session_cache: Any) -> Any:
    """
    API endpoint to check if graph data exists in the repository.
    
    This function provides a JSON API endpoint for checking the existence
    of graph data in the configured GraphDB repository.
    
    Args:
        session_cache: The session cache object containing repository configuration
        
    Returns:
        flask.jsonify: JSON response with exists boolean and optional error message
    """
    try:
        if not session_cache.repo:
            return jsonify({'exists': False, 'error': 'No repository configured'})

        # Check if the main data graph exists
        graph_uri = "http://data.local/"
        exists = check_graph_exists(session_cache.repo, graph_uri)

        return jsonify({'exists': exists})
    except Exception as e:
        return jsonify({'exists': False, 'error': str(e)})


def check_graph_exists(repo: str, graph_uri: str, graphdb_url: Optional[str] = None) -> bool:
    """
    Check if a graph exists in a GraphDB repository.
    
    This function verifies the existence of a specific graph in the GraphDB
    repository by executing an ASK SPARQL query.
    
    Args:
        repo (str): The name of the repository in GraphDB
        graph_uri (str): The URI of the graph to check
        graphdb_url (Optional[str]): GraphDB base URL, if None uses config
        
    Returns:
        bool: True if the graph exists, False otherwise
        
    Raises:
        Exception: If the request to the GraphDB instance fails
    """
    if graphdb_url is None:
        graphdb_url, _ = get_graphdb_config()
    
    # Construct the SPARQL query
    query = f"ASK WHERE {{ GRAPH <{graph_uri}> {{ ?s ?p ?o }} }}"

    # Send a GET request to the GraphDB instance
    response = requests.get(
        f"{graphdb_url}/repositories/{repo}",
        params={"query": query},
        headers={"Accept": "application/sparql-results+json"}
    )

    # If the request is successful, return the result of the ASK query
    if response.status_code == 200:
        return response.json()['boolean']
    # If the request fails, raise an exception with the status code
    else:
        raise Exception(f"Query failed with status code {response.status_code}")


def execute_query(repo: str, query: str, query_type: Optional[str] = None, 
                 endpoint_appendices: Optional[str] = None, 
                 graphdb_url: Optional[str] = None) -> Any:
    """
    Execute a SPARQL query on a specified GraphDB repository.
    
    This function executes SPARQL queries (SELECT, ASK, CONSTRUCT, etc.) on the
    specified GraphDB repository and returns the results.
    
    Args:
        repo (str): The name of the GraphDB repository
        query (str): The SPARQL query to be executed
        query_type (Optional[str]): The type of the SPARQL query. Defaults to "query"
        endpoint_appendices (Optional[str]): Additional endpoint parameters
        graphdb_url (Optional[str]): GraphDB base URL, if None uses config
        
    Returns:
        str: The result of the query execution as a string if successful
        flask.render_template: Error template if execution fails
        
    Raises:
        Exception: If an error occurs during query execution
        
    Note:
        The function performs the following steps:
        1. Sets default values for optional parameters
        2. Constructs the endpoint URL using the repository name and appendices
        3. Executes the SPARQL query on the constructed endpoint
        4. Returns the result as a string if successful
        5. Flashes error message and renders template if execution fails
    """
    if graphdb_url is None:
        graphdb_url, _ = get_graphdb_config()
        
    if query_type is None:
        query_type = "query"

    if endpoint_appendices is None:
        endpoint_appendices = ""
        
    try:
        # Construct the endpoint URL
        endpoint = f"{graphdb_url}/repositories/" + repo + endpoint_appendices
        
        # Execute the query
        response = requests.post(endpoint,
                                data={query_type: query},
                                headers={"Content-Type": "application/x-www-form-urlencoded"})
        
        # Return the result of the query execution
        return response.text
    except Exception as e:
        # If an error occurs, flash the error message to the user and render the 'ingest.html' template
        flash(f'Unexpected error when connecting to GraphDB, error: {e}.')
        return render_template('ingest.html')


# Data Ingest Functions (integrated from utils/data_ingest.py)

import time
from typing import Tuple, List
import gevent
from gevent import subprocess as gevent_subprocess


def upload_file_to_graphdb(file_path: str, url: str, content_type: str,
                           wait_for_completion: bool = True,
                           timeout_seconds: int = 300) -> Tuple[bool, str, str]:
    """
    Upload a single file to GraphDB with an intelligent fallback strategy.
    Uses gevent subprocess for better integration with gevent worker.

    Args:
        file_path (str): Path to the file to upload
        url (str): GraphDB endpoint URL
        content_type (str): MIME type (e.g. 'application/rdf+xml')
        wait_for_completion (bool): Whether to wait for upload completion
        timeout_seconds (int): Timeout for each upload attempt

    Returns:
        tuple: (success: bool, message: str, method_used: str)
    """

    file_name = os.path.basename(file_path)
    
    if not os.path.exists(file_path):
        return False, f"File not found: {file_path}", "error"

    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    logger.info(f"📤 Uploading {file_name} ({file_size_mb:.1f} MB) to GraphDB")

    if not wait_for_completion:
        # Start background upload
        gevent.spawn(_background_upload_with_fallback, file_path, url, content_type, timeout_seconds, file_name)
        return True, f"Background upload started for {file_name}", "background"

    # Synchronous upload with fallback
    return _synchronous_upload_with_fallback(file_path, url, content_type, timeout_seconds, file_name)


def upload_ontology_then_data(root_dir: str, graphdb_url: str, repo: str,
                              upload_ontology: bool = True,
                              upload_data: bool = True,
                              data_background: bool = True,
                              ontology_timeout: int = 300,
                              data_timeout: int = 43200) -> Tuple[bool, List[str]]:
    """
    Upload ontology first, then data (configurable background/foreground).
    Uses gevent for async operations.

    Args:
        root_dir (str): Directory containing the files
        graphdb_url (str): GraphDB base URL
        repo (str): Repository name
        upload_ontology (bool): Whether to upload ontology file
        upload_data (bool): Whether to upload data file
        data_background (bool): Whether to upload data in background
        ontology_timeout (int): Timeout for ontology upload
        data_timeout (int): Timeout for data upload

    Returns:
        tuple: (success: bool, messages: list)
    """

    messages = []
    overall_success = True
    process_start_time = time.time()

    logger.info("📤 Starting sequential upload process (ontology → data)")

    # Step 1: Upload ontology and WAIT for completion
    if upload_ontology:
        ontology_path = f"{root_dir}ontology.owl"
        ontology_url = f"{graphdb_url}/repositories/{repo}/rdf-graphs/service?graph=http://ontology.local/"

        logger.info("📋 Step 1/2: Uploading ontology (synchronous)...")
        success, message, method = upload_file_to_graphdb(
            ontology_path, ontology_url, "application/rdf+xml",
            wait_for_completion=True,  # ALWAYS wait for ontology
            timeout_seconds=ontology_timeout
        )

        messages.append(f"Ontology upload ({method}): {message}")
        if not success:
            overall_success = False
            logger.error("❌ Ontology upload failed - aborting data upload")
            return overall_success, messages
        else:
            logger.info("✅ Ontology upload completed - proceeding to data upload")

    # Step 2: Upload data (background/foreground configurable)
    if upload_data:
        data_path = f"{root_dir}output.ttl"
        data_url = f"{graphdb_url}/repositories/{repo}/rdf-graphs/service?graph=http://data.local/"

        upload_mode = "background" if data_background else "synchronous"
        logger.info(f"📊 Step 2/2: Starting data upload ({upload_mode})...")

        success, message, method = upload_file_to_graphdb(
            data_path, data_url, "application/x-turtle",
            wait_for_completion=not data_background,  # Configurable wait
            timeout_seconds=data_timeout
        )

        messages.append(f"Data upload ({method}): {message}")
        if not success:
            overall_success = False

    process_elapsed = time.time() - process_start_time

    if overall_success:
        logger.info(f"🎉 Sequential upload process completed in {process_elapsed:.1f}s")
        logger.info("   📋 Ontology: Ready for queries")
        if data_background:
            logger.info("   📊 Data: Uploading in background")
        else:
            logger.info("   📊 Data: Upload completed")
    else:
        logger.error(f"❌ Sequential upload process failed after {process_elapsed:.1f}s")

    return overall_success, messages


def _background_upload_with_fallback(file_path: str, url: str, content_type: str,
                                     timeout_seconds: int, file_name: str) -> None:
    """Execute upload in background using gevent greenlet"""
    try:
        start_time = time.time()

        logger.info(f"🔄 Background upload: trying data-binary first for {file_name}")

        # Always try data-binary first (regardless of file size)
        success, message = _try_data_binary_upload(file_path, url, content_type, timeout_seconds)

        if success:
            elapsed = time.time() - start_time
            logger.info(f"✅ Background upload completed via data-binary in {elapsed:.1f}s: {file_name}")
            return

        logger.warning(f"⚠️ Background data-binary failed for {file_name}: {message}")
        logger.info(f"🔄 Background upload: trying streaming fallback for {file_name}")

        # Fallback to streaming
        success, message = _try_streaming_upload(file_path, url, content_type, timeout_seconds)

        elapsed = time.time() - start_time
        if success:
            logger.info(f"✅ Background upload completed via streaming in {elapsed:.1f}s: {file_name}")
        else:
            logger.error(f"❌ Background upload failed completely for {file_name} after {elapsed:.1f}s: {message}")

    except Exception as e:
        logger.error(f"❌ Background upload exception for {file_name}: {e}")


def _synchronous_upload_with_fallback(file_path: str, url: str, content_type: str,
                                      timeout_seconds: int, file_name: str) -> Tuple[bool, str, str]:
    """Execute synchronous upload with fallback strategy"""
    start_time = time.time()

    logger.info(f"🔄 Synchronous upload: trying data-binary first for {file_name}")

    # Always try data-binary first (regardless of file size)
    success, message = _try_data_binary_upload(file_path, url, content_type, timeout_seconds)

    if success:
        elapsed = time.time() - start_time
        logger.info(f"✅ Synchronous upload completed via data-binary in {elapsed:.1f}s: {file_name}")
        return True, message, "data-binary"

    logger.warning(f"⚠️ Synchronous data-binary failed for {file_name}: {message}")
    logger.info(f"🔄 Synchronous upload: trying streaming fallback for {file_name}")

    # Fallback to streaming
    success, message = _try_streaming_upload(file_path, url, content_type, timeout_seconds)

    elapsed = time.time() - start_time
    if success:
        logger.info(f"✅ Synchronous upload completed via streaming in {elapsed:.1f}s: {file_name}")
        return True, message, "streaming"
    else:
        logger.error(f"❌ Synchronous upload failed completely for {file_name} after {elapsed:.1f}s: {message}")
        return False, message, "failed"


def _try_data_binary_upload(file_path: str, url: str, content_type: str, timeout_seconds: int) -> Tuple[bool, str]:
    """Try uploading using curl --data-binary (best for GraphDB)"""
    try:
        result = gevent_subprocess.run([
            "curl", "-X", "POST",
            "-H", f"Content-Type: {content_type}",
            "--data-binary", f"@{file_path}",
            "--fail", "--silent", "--show-error",
            url
        ], capture_output=True, text=True, timeout=timeout_seconds)

        if result.returncode == 0:
            return True, "Upload successful via data-binary method"
        else:
            error_msg = result.stderr.strip() or "Unknown curl error"
            return False, f"curl data-binary failed: {error_msg}"

    except gevent_subprocess.TimeoutExpired:
        return False, "Upload timeout (data-binary method)"
    except Exception as e:
        return False, f"Unexpected error (data-binary method): {str(e)}"


def _try_streaming_upload(file_path: str, url: str, content_type: str, timeout_seconds: int) -> Tuple[bool, str]:
    """Try uploading using curl -T (streaming, fallback method)"""
    try:
        result = gevent_subprocess.run([
            "curl", "-X", "POST",
            "-H", f"Content-Type: {content_type}",
            "-T", file_path,
            "--fail", "--silent", "--show-error",
            url
        ], capture_output=True, text=True, timeout=timeout_seconds)

        if result.returncode == 0:
            return True, "Upload successful via streaming method"
        else:
            error_msg = result.stderr.strip() or "Unknown curl error"
            return False, f"curl streaming failed: {error_msg}"

    except gevent_subprocess.TimeoutExpired:
        return False, "Upload timeout (streaming method)"
    except Exception as e:
        return False, f"Unexpected error (streaming method): {str(e)}"