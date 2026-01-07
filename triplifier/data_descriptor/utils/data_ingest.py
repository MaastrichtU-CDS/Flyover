import os
import time
import logging
from typing import Tuple, List

# Use gevent for consistency with the main app
import gevent
from gevent import subprocess as gevent_subprocess

# Use the centrally configured logger
logger = logging.getLogger(__name__)


def upload_file_to_graphdb(
    file_path: str,
    url: str,
    content_type: str,
    wait_for_completion: bool = True,
    timeout_seconds: int = 300,
) -> Tuple[bool, str, str]:
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

    if not os.path.exists(file_path):
        error_msg = f"File not found: {file_path}"
        logger.error(error_msg)
        return False, error_msg, "none"

    # Get file info
    file_size = os.path.getsize(file_path)
    file_size_mb = file_size / (1024 * 1024)
    file_name = os.path.basename(file_path)

    logger.info(f"Starting upload of {file_name} ({file_size_mb:.1f} MB)")

    if not wait_for_completion:
        # Start upload in background using gevent spawn
        logger.info(f"🚀 Starting background upload for {file_name}")
        gevent.spawn(
            _background_upload_with_fallback,
            file_path,
            url,
            content_type,
            timeout_seconds,
            file_name,
        )
        return True, f"Background upload started for {file_name}", "background"

    # Synchronous upload with fallback
    return _synchronous_upload_with_fallback(
        file_path, url, content_type, timeout_seconds, file_name
    )


def upload_multiple_graphs(
    root_dir: str,
    graphdb_url: str,
    repo: str,
    output_files: List[dict],
    ontology_timeout: int = 300,
    data_timeout: int = 43200,
    data_background: bool = True,
) -> Tuple[bool, List[str]]:
    """
    Upload multiple ontology and data files as separate named graphs.
    Used for CSV files where each table gets its own named graph.

    Args:
        root_dir (str): Directory containing the files
        graphdb_url (str): GraphDB base URL
        repo (str): Repository name
        output_files (List[dict]): List of dicts with keys: data_file, ontology_file, table_name
        ontology_timeout (int): Timeout for ontology upload
        data_timeout (int): Timeout for data upload
        data_background (bool): Whether to upload data in background

    Returns:
        tuple: (success: bool, messages: list)
    """
    messages = []
    overall_success = True
    process_start_time = time.time()

    logger.info(f"📤 Starting upload of {len(output_files)} table(s) to separate named graphs")

    for idx, file_info in enumerate(output_files, 1):
        table_name = file_info["table_name"]
        ontology_path = file_info["ontology_file"]
        data_path = file_info["data_file"]

        logger.info(f"📋 Processing table {idx}/{len(output_files)}: {table_name}")

        # Upload ontology for this table (synchronous)
        ontology_url = f"{graphdb_url}/repositories/{repo}/rdf-graphs/service?graph=http://ontology.local/{table_name}/"
        
        logger.info(f"  📋 Uploading ontology for {table_name} (synchronous)...")
        success, message, method = upload_file_to_graphdb(
            ontology_path,
            ontology_url,
            "application/rdf+xml",
            wait_for_completion=True,  # ALWAYS wait for ontology
            timeout_seconds=ontology_timeout,
        )

        messages.append(f"Ontology upload for {table_name} ({method}): {message}")
        if not success:
            overall_success = False
            logger.error(f"❌ Ontology upload failed for {table_name} - skipping data upload")
            continue
        else:
            logger.info(f"✅ Ontology upload completed for {table_name}")

        # Upload data for this table (background/foreground configurable)
        data_url = f"{graphdb_url}/repositories/{repo}/rdf-graphs/service?graph=http://data.local/{table_name}/"
        
        upload_mode = "background" if data_background else "synchronous"
        logger.info(f"  📊 Starting data upload for {table_name} ({upload_mode})...")

        success, message, method = upload_file_to_graphdb(
            data_path,
            data_url,
            "application/x-turtle",
            wait_for_completion=not data_background,  # Configurable wait
            timeout_seconds=data_timeout,
        )

        messages.append(f"Data upload for {table_name} ({method}): {message}")
        if not success:
            overall_success = False

    process_elapsed = time.time() - process_start_time

    if overall_success:
        logger.info(f"🎉 Multi-graph upload process completed in {process_elapsed:.1f}s")
        logger.info(f"   📋 {len(output_files)} ontologies: Ready for queries")
        if data_background:
            logger.info(f"   📊 {len(output_files)} data files: Uploading in background")
        else:
            logger.info(f"   📊 {len(output_files)} data files: Upload completed")
    else:
        logger.error(
            f"❌ Multi-graph upload process failed after {process_elapsed:.1f}s"
        )

    return overall_success, messages


def upload_ontology_then_data(
    root_dir: str,
    graphdb_url: str,
    repo: str,
    upload_ontology: bool = True,
    upload_data: bool = True,
    data_background: bool = True,
    ontology_timeout: int = 300,
    data_timeout: int = 43200,
) -> Tuple[bool, List[str]]:
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
            ontology_path,
            ontology_url,
            "application/rdf+xml",
            wait_for_completion=True,  # ALWAYS wait for ontology
            timeout_seconds=ontology_timeout,
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
            data_path,
            data_url,
            "application/x-turtle",
            wait_for_completion=not data_background,  # Configurable wait
            timeout_seconds=data_timeout,
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
        logger.error(
            f"❌ Sequential upload process failed after {process_elapsed:.1f}s"
        )

    return overall_success, messages


def _background_upload_with_fallback(
    file_path: str, url: str, content_type: str, timeout_seconds: int, file_name: str
) -> None:
    """Execute upload in background using gevent greenlet"""
    try:
        start_time = time.time()

        logger.info(f"🔄 Background upload: trying data-binary first for {file_name}")

        # Always try data-binary first (regardless of file size)
        success, message = _try_data_binary_upload(
            file_path, url, content_type, timeout_seconds
        )

        if success:
            elapsed = time.time() - start_time
            logger.info(
                f"✅ Background upload (data-binary) successful for {file_name} in {elapsed:.1f}s"
            )
            return
        else:
            logger.warning(
                f"⚠️ Background data-binary failed for {file_name}: {message}"
            )
            logger.info(
                f"🔄 Background upload: falling back to streaming for {file_name}"
            )

        # Fallback to streaming upload
        success, message = _try_streaming_upload(
            file_path, url, content_type, timeout_seconds
        )
        elapsed = time.time() - start_time

        if success:
            logger.info(
                f"✅ Background upload (streaming) successful for {file_name} in {elapsed:.1f}s"
            )
        else:
            logger.error(
                f"❌ Background upload (both methods) failed for {file_name} after {elapsed:.1f}s: {message}"
            )

    except Exception as e:
        logger.error(f"❌ Background upload crashed for {file_name}: {str(e)}")


def _synchronous_upload_with_fallback(
    file_path: str, url: str, content_type: str, timeout_seconds: int, file_name: str
) -> Tuple[bool, str, str]:
    """Execute synchronous upload with intelligent fallback using gevent"""
    start_time = time.time()
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)

    # Always try data-binary first, regardless of file size
    # Let the system decide if it can handle it in RAM
    logger.info(
        f"Attempting --data-binary upload for {file_name} ({file_size_mb:.1f} MB)"
    )
    success, message = _try_data_binary_upload(
        file_path, url, content_type, timeout_seconds
    )

    if success:
        elapsed = time.time() - start_time
        logger.info(
            f"✅ --data-binary upload successful for {file_name} in {elapsed:.1f}s"
        )
        return True, message, "data-binary"
    else:
        logger.warning(f"⚠️ --data-binary failed for {file_name}: {message}")
        logger.info(f"🔄 Falling back to streaming upload for {file_name}")

    # Fallback to streaming upload
    success, message = _try_streaming_upload(
        file_path, url, content_type, timeout_seconds
    )
    elapsed = time.time() - start_time

    if success:
        logger.info(f"✅ Streaming upload successful for {file_name} in {elapsed:.1f}s")
        return True, message, "streaming"
    else:
        logger.error(
            f"❌ Both upload methods failed for {file_name} after {elapsed:.1f}s"
        )
        return False, f"All upload methods failed. Last error: {message}", "failed"


def _try_data_binary_upload(
    file_path: str, url: str, content_type: str, timeout_seconds: int
) -> Tuple[bool, str]:
    """Attempt upload using the --data-binary method with gevent subprocess"""
    try:
        # Use gevent subprocess for consistency
        result = gevent_subprocess.run(
            [
                "curl",
                "-X",
                "POST",
                "-H",
                f"Content-Type: {content_type}",
                "--data-binary",
                f"@{file_path}",
                "--fail",
                "--silent",
                "--show-error",
                url,
            ],
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )

        if result.returncode == 0:
            return True, "Upload successful via data-binary method"
        else:
            error_msg = result.stderr.strip() or "Unknown curl error"
            return False, f"curl data-binary failed: {error_msg}"

    except gevent_subprocess.TimeoutExpired:
        return False, "Upload timeout (data-binary method)"
    except Exception as e:
        return False, f"Unexpected error (data-binary method): {str(e)}"


def _try_streaming_upload(
    file_path: str, url: str, content_type: str, timeout_seconds: int
) -> Tuple[bool, str]:
    """Attempt upload using -T streaming method with gevent subprocess"""
    try:
        # Use gevent subprocess for consistency
        result = gevent_subprocess.run(
            [
                "curl",
                "-X",
                "POST",
                "-H",
                f"Content-Type: {content_type}",
                "-T",
                file_path,
                "--fail",
                "--silent",
                "--show-error",
                url,
            ],
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )

        if result.returncode == 0:
            return True, "Upload successful via streaming method"
        else:
            error_msg = result.stderr.strip() or "Unknown curl error"
            return False, f"curl streaming failed: {error_msg}"

    except gevent_subprocess.TimeoutExpired:
        return False, "Upload timeout (streaming method)"
    except Exception as e:
        return False, f"Unexpected error (streaming method): {str(e)}"
