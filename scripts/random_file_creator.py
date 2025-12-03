#!/usr/bin/env python3
"""
Random File Generator Daemon
----------------------------
Creates random text files in the target directory at random intervals (30–60 seconds).
Runs indefinitely until stopped (Ctrl+C).
"""

import os
import time
import random
import string
import logging
from pathlib import Path
from datetime import datetime

# =============================
# Configuration
# =============================

# Directory where files will be created
TARGET_DIR = Path.cwd()/"watched"
MIN_DELAY = 30
MAX_DELAY = 60

# =============================
# Logging setup
# =============================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("RandomFileGenerator")

# =============================
# Helper functions
# =============================

def random_text(length=100):
    """Generate random alphanumeric string."""
    chars = string.ascii_letters + string.digits + " "
    return ''.join(random.choice(chars) for _ in range(length))

def create_random_file(target_dir: Path):
    """Create a random text file in the specified directory."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"random_{timestamp}.txt"
    filepath = target_dir / filename

    try:
        with open(filepath, "w", encoding="utf-8") as file:
            file.write(f"Created: {datetime.now().isoformat()}\n")
            file.write(f"File: {filename}\n\n")
            file.write(random_text(random.randint(100, 300)))
        logger.info(f"Created file: {filepath}")
    except Exception as e:
        logger.error(f"Failed to create file: {e}")

# =============================
# Main loop
# =============================

def main():
    logger.info(f"Starting random file generator in: {TARGET_DIR}")
    logger.info("Press Ctrl+C to stop.\n")

    # Ensure directory exists
    try:
        TARGET_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.error(f"Cannot create or access directory: {e}")
        return

    try:
        while True:
            delay = random.randint(MIN_DELAY, MAX_DELAY)
            logger.info(f"Waiting {delay} seconds before next file...")
            time.sleep(delay)
            create_random_file(TARGET_DIR)
    except KeyboardInterrupt:
        logger.info("Stopped by user. Exiting gracefully.")
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")

# =============================
# Entry point
# =============================

if __name__ == "__main__":
    main()
