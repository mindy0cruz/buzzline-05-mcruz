"""
consumer_mcruz.py

"""

#####################################
# Import Modules
#####################################

import json
import pathlib
import sys
import time

import utils.utils_config as config
from utils.utils_logger import logger
from .sqlite_consumer_case import init_db, insert_message

#####################################
# Function to process a single message
#####################################

def process_message(message: dict) -> dict | None:
    """
    Process and transform a single JSON message.

    Args:
        message (dict): The JSON message as a dictionary.

    Returns:
        dict | None: Processed message or None if error.
    """
    try:
        # Extract the keyword and only keep if > 5 characters
        keyword = message.get("keyword_mentioned", "")
        if len(keyword) <= 5:
            keyword = None

        processed_message = {
            "message": message.get("message"),
            "author": message.get("author"),
            "timestamp": message.get("timestamp"),
            "category": message.get("category"),
            "sentiment": float(message.get("sentiment", 0.0)),
            "keyword_mentioned": keyword,
            "message_length": len(message.get("message", "")),
        }
        logger.info(f"Processed message: {processed_message}")
        return processed_message
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None

#####################################
# Consume Messages from Live Data File
#####################################

def consume_messages_from_file(live_data_path: pathlib.Path, sql_path: pathlib.Path, interval_secs: int):
    """
    Continuously consume new messages from a live data file.

    Args:
        live_data_path (pathlib.Path): Path to the live data file.
        sql_path (pathlib.Path): Path to the SQLite database file.
        interval_secs (int): Interval in seconds to check for new messages.
    """
    logger.info(f"Starting consumer on {live_data_path}")
    init_db(sql_path)
    last_position = 0

    while True:
        try:
            with open(live_data_path, "r", encoding="utf-8") as file:
                file.seek(last_position)
                for line in file:
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        message = json.loads(line)
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON line: {line} | {e}")
                        continue

                    processed_message = process_message(message)
                    if processed_message:
                        insert_message(processed_message, sql_path)

                last_position = file.tell()

        except FileNotFoundError:
            logger.error(f"Live data file not found: {live_data_path}")
        except Exception as e:
            logger.error(f"Error reading file: {e}")

        time.sleep(interval_secs)

#####################################
# Main Function
#####################################

def main():
    logger.info("Starting dynamic file consumer...")

    try:
        interval_secs = config.get_message_interval_seconds_as_int()
        live_data_path = config.get_live_data_path()
        sqlite_path = config.get_sqlite_path()
    except Exception as e:
        logger.error(f"Failed to read environment variables: {e}")
        sys.exit(1)

    if sqlite_path.exists():
        sqlite_path.unlink()

    try:
        init_db(sqlite_path)
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        sys.exit(2)

    try:
        consume_messages_from_file(live_data_path, sqlite_path, interval_secs)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    finally:
        logger.info("Consumer shutting down.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
