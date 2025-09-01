"""
Triplifier operations module.

This module contains functions for running the triplifier tool,
managing properties files, and coordinating background processing.
"""

import os
import logging
from typing import Tuple, List, Optional
import gevent
import gevent.subprocess
from markupsafe import Markup

logger = logging.getLogger(__name__)


def run_triplifier(properties_file: Optional[str], root_dir: str, child_dir: str,
                  upload_folder: str, csv_data: List, csv_path: List,
                  pk_fk_data: Optional[List], cross_graph_link_data: Optional[dict],
                  background_pk_fk_func, background_cross_graph_func) -> Tuple[bool, str]:
    """
    Run the triplifier tool and check if it ran successfully.

    Args:
        properties_file: Name of the properties file to use
        root_dir: Root directory path
        child_dir: Child directory path
        upload_folder: Upload folder path for file access check
        csv_data: List of CSV data frames
        csv_path: List of CSV file paths
        pk_fk_data: Primary key/foreign key relationship data
        cross_graph_link_data: Cross-graph linking data
        background_pk_fk_func: Function for background PK/FK processing
        background_cross_graph_func: Function for background cross-graph processing

    Returns:
        Tuple of (success: bool, message: str)

    This function uses gevent subprocess for better integration with gevent worker.
    It handles CSV properties file modifications for Docker/non-Docker environments,
    runs the triplifier, and initiates background processing if needed.
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
            for i, csv_data_frame in enumerate(csv_data):
                csv_file_path = csv_path[i]
                csv_data_frame.to_csv(csv_file_path, index=False, sep=',', decimal='.', encoding='utf-8')

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

        # Restore properties file if modified for local debugging
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
            # START BACKGROUND PK/FK PROCESSING FOR CSV FILES - Use gevent spawn
            if properties_file == 'triplifierCSV.properties' and pk_fk_data:
                logger.info("Triplifier successful. Starting background PK/FK processing...")
                gevent.spawn(background_pk_fk_func)

            # START BACKGROUND CROSS-GRAPH PROCESSING FOR CSV FILES - Use gevent spawn
            if properties_file == 'triplifierCSV.properties' and cross_graph_link_data:
                logger.info("Triplifier successful. Starting background cross-graph processing...")
                gevent.spawn(background_cross_graph_func)

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