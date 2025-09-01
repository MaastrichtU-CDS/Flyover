"""
Triplification utilities for the Flyover data descriptor.

This module contains the core triplification logic for converting data
into RDF triples.
"""

from typing import Optional, Tuple, List, Any
import logging
import os
import gevent
from markupsafe import Markup

# Use the centrally configured logger
logger = logging.getLogger(__name__)


def run_triplifier(properties_file: Optional[str] = None, 
                  session_cache: Any = None,
                  upload_folder: str = '',
                  root_dir: str = '',
                  child_dir: str = '.') -> Tuple[bool, str]:
    """
    Run the triplifier process to convert data to RDF format.
    
    This function coordinates the triplification process, including:
    1. Preparing the data and ontology files
    2. Running the triplification Java tool
    3. Generating RDF output files
    4. Handling error conditions and cleanup
    
    Args:
        properties_file (Optional[str]): Path to properties file for configuration
        session_cache: Session cache object containing data and configuration
        upload_folder (str): Path to upload folder for temporary files
        root_dir (str): Root directory path
        child_dir (str): Child directory path
        
    Returns:
        Tuple[bool, str]: (success_flag, status_message) indicating result and details
        
    Note:
        The triplification process involves:
        - Validating input data and configuration
        - Executing the Java-based triplification tool
        - Generating RDF output files
        - Providing user feedback on success/failure
    """
    try:
        if properties_file == 'triplifierCSV.properties':
            if not os.access(upload_folder, os.W_OK):
                return False, "Unable to temporarily save the CSV file: no write access to the application folder."

            # Allow easier debugging outside Docker
            if len(root_dir) == 0 and child_dir == '.':
                # Read the properties file and replace the jdbc.url line
                with open('triplifierCSV.properties', "r") as f:
                    lines = f.readlines()

                modified_lines = [
                    line.replace(
                        "jdbc.url = jdbc:relique:csv:/app/data_descriptor/static/files?fileExtension=.csv",
                        "jdbc.url = jdbc:relique:csv:./static/files?fileExtension=.csv"
                    ) for line in lines
                ]

                # Write the modified content back to the properties file
                with open('triplifierCSV.properties', "w") as f:
                    f.writelines(modified_lines)

            # Save CSV data to files
            for i, csv_data in enumerate(session_cache.csvData):
                csv_path = session_cache.csvPath[i]
                csv_data.to_csv(csv_path, index=False, sep=',', decimal='.', encoding='utf-8')

        # Get JAVA_OPTS from environment or use default
        java_opts = os.getenv('JAVA_OPTS', '-Xms2g -Xmx8g')

        # Use gevent subprocess for better integration with gevent worker
        command = f"java {java_opts} -jar {root_dir}{child_dir}/javaTool/triplifier.jar -p {root_dir}{child_dir}/{properties_file}"

        # Use gevent.subprocess.check_output instead of Popen to avoid threading issues
        try:
            # Create process with gevent subprocess
            output = gevent.subprocess.check_output(
                command,
                shell=True,
                stderr=gevent.subprocess.STDOUT,
                text=True
            )
            logger.info(output)
            return_code = 0
        except gevent.subprocess.CalledProcessError as e:
            output = e.output
            return_code = e.returncode
            logger.error(f"Process failed with return code {return_code}: {output}")

        if properties_file == 'triplifierCSV.properties':
            # Allow easier debugging outside Docker
            if len(root_dir) == 0 and child_dir == '.':
                # Read the properties file and replace the jdbc.url line
                with open('triplifierCSV.properties', "r") as f:
                    lines = f.readlines()

                modified_lines = [
                    line.replace(
                        "jdbc.url = jdbc:relique:csv:./static/files?fileExtension=.csv",
                        "jdbc.url = jdbc:relique:csv:/app/data_descriptor/static/files?fileExtension=.csv"
                    ) for line in lines
                ]

                # Write the modified content back to the properties file
                with open('triplifierCSV.properties', "w") as f:
                    f.writelines(modified_lines)

        if return_code == 0:
            # Import relationship processing functions
            from .relationship_processing import background_pk_fk_processing, background_cross_graph_processing
            
            # START BACKGROUND PK/FK PROCESSING FOR CSV FILES - Use gevent spawn
            if properties_file == 'triplifierCSV.properties' and hasattr(session_cache, 'pk_fk_data') and session_cache.pk_fk_data:
                logger.info("Triplifier successful. Starting background PK/FK processing...")
                gevent.spawn(background_pk_fk_processing, session_cache)

            # START BACKGROUND CROSS-GRAPH PROCESSING FOR CSV FILES - Use gevent spawn
            if properties_file == 'triplifierCSV.properties' and hasattr(session_cache, 'cross_graph_link_data') and session_cache.cross_graph_link_data:
                logger.info("Triplifier successful. Starting background cross-graph processing...")
                gevent.spawn(background_cross_graph_processing, session_cache)

            return True, Markup("The data you have submitted was triplified successfully and "
                                "is now available in GraphDB."
                                "<br>"
                                "You can now proceed to describe your data, "
                                "but please note that this requires in-depth knowledge of the data."
                                "<br><br>"
                                "<i>In case you do not yet wish to describe your data, "
                                "or you would like to add more data, "
                                "please return to the ingest page.</i>"
                                "<br>"
                                "<i>You can always return to Flyover to "
                                "describe the data that is present in GraphDB.</i>")
        else:
            return False, output

    except OSError as e:
        return False, f'Unexpected error attempting to create the upload folder, error: {e}'
    except Exception as e:
        return False, f'Unexpected error attempting to run the Triplifier, error: {e}'


# Additional utility functions that might be part of triplification
def validate_triplification_input(session_cache: any) -> Tuple[bool, str]:
    """
    Validate input data before running triplification.
    
    Args:
        session_cache: Session cache containing data to validate
        
    Returns:
        Tuple[bool, str]: (is_valid, error_message)
    """
    # This would contain validation logic
    raise NotImplementedError("Function needs to be implemented")


def prepare_triplification_files(session_cache: any, root_dir: str, child_dir: str) -> bool:
    """
    Prepare necessary files for the triplification process.
    
    Args:
        session_cache: Session cache containing data
        root_dir (str): Root directory path
        child_dir (str): Child directory path
        
    Returns:
        bool: True if preparation successful
    """
    # This would contain file preparation logic
    raise NotImplementedError("Function needs to be implemented")


def cleanup_triplification_files(root_dir: str, child_dir: str, keep_output: bool = True) -> None:
    """
    Clean up temporary files after triplification.
    
    Args:
        root_dir (str): Root directory path
        child_dir (str): Child directory path
        keep_output (bool): Whether to keep output files
    """
    # This would contain cleanup logic
    raise NotImplementedError("Function needs to be implemented")