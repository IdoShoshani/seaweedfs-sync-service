import time
import requests
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import logging
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SeaweedFSUploader(FileSystemEventHandler):
    """Uploads new files to SeaweedFS"""
    
    def __init__(self, master_url, watched_dir):
        self.master_url = master_url
        self.watched_dir = Path(watched_dir)
        self.uploaded_files = set()  # Track uploaded files to prevent duplicates
        self.processing_files = set()  # Track files currently being processed
        logger.info(f"Initialized uploader. Master: {master_url}, Watching: {watched_dir}")
    
    def on_created(self, event):
        """Triggered when a new file is created"""
        
        if event.is_directory:
            return
        
        file_path = Path(event.src_path)
        
        # Skip temporary and hidden files
        if self._should_skip_file(file_path):
            logger.debug(f"Skipping file: {file_path.name}")
            return
        
        # Prevent duplicate processing
        if file_path in self.processing_files:
            logger.debug(f"File already being processed: {file_path.name}")
            return
        
        self.processing_files.add(file_path)
        
        logger.info(f"New file detected: {file_path.name}")
        
        try:
            # Wait for file to be completely written
            if not self._wait_for_file_completion(file_path):
                logger.warning(f"File not stable after timeout: {file_path.name}")
                return
            
            # Check if already uploaded
            file_hash = self._get_file_hash(file_path)
            if file_hash in self.uploaded_files:
                logger.info(f"File already uploaded (duplicate): {file_path.name}")
                return
            
            # Upload the file
            self.upload_file(file_path)
            
            # Mark as uploaded
            self.uploaded_files.add(file_hash)
            
            # Query and report storage status
            self.report_storage_status()
            
        except Exception as e:
            logger.error(f"Error processing {file_path.name}: {e}")
        finally:
            # Remove from processing set
            self.processing_files.discard(file_path)
    
    def _should_skip_file(self, file_path):
        """Check if file should be skipped"""
        name = file_path.name
        
        # Skip hidden files (starting with .)
        if name.startswith('.'):
            return True
        
        # Skip temporary files
        if name.endswith('~') or name.endswith('.tmp') or name.endswith('.swp'):
            return True
        
        # Skip backup files
        if name.endswith('.bak'):
            return True
        
        return False
    
    def _wait_for_file_completion(self, file_path, timeout=10):
        """Wait until file size stabilizes (file finished writing)"""
        last_size = -1
        stable_count = 0
        start_time = time.time()
        required_stable_checks = 3
        
        while time.time() - start_time < timeout:
            try:
                current_size = file_path.stat().st_size
                
                if current_size == last_size:
                    stable_count += 1
                    if stable_count >= required_stable_checks:
                        logger.debug(f"File stable: {file_path.name} ({current_size} bytes)")
                        return True
                else:
                    stable_count = 0
                    logger.debug(f"File still growing: {file_path.name} ({current_size} bytes)")
                
                last_size = current_size
                time.sleep(0.5)
                
            except OSError as e:
                logger.debug(f"Waiting for file access: {file_path.name} - {e}")
                time.sleep(0.5)
        
        return False
    
    def _get_file_hash(self, file_path):
        """Calculate file hash for duplicate detection"""
        try:
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"Error calculating hash for {file_path.name}: {e}")
            # Return path as fallback
            return str(file_path)
    
    def upload_file(self, file_path):
        """Uploads a file to SeaweedFS"""
        
        # Step 1: Get fid (file ID) from Master
        assign_url = f"{self.master_url}/dir/assign"
        
        try:
            response = requests.post(assign_url, timeout=10)
            response.raise_for_status()
            assign_data = response.json()
            
            fid = assign_data['fid']
            public_url = assign_data['publicUrl']
            
            logger.info(f"Assigned fid: {fid} on {public_url}")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get fid from master: {e}")
            raise
        
        # Step 2: Upload the file to Volume Server
        upload_url = f"http://{public_url}/{fid}"
        
        try:
            with open(file_path, 'rb') as f:
                files = {'file': (file_path.name, f)}
                response = requests.post(upload_url, files=files, timeout=30)
                response.raise_for_status()
            
            logger.info(f"✓ Successfully uploaded: {file_path.name} (fid: {fid})")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to upload file: {e}")
            raise
        except IOError as e:
            logger.error(f"Failed to read file {file_path}: {e}")
            raise
    
    def report_storage_status(self):
        """Queries and prints storage status from Master"""
        
        # Get volume statistics
        volumes_url = f"{self.master_url}/vol/status"
        
        try:
            vol_response = requests.get(volumes_url, timeout=10)
            vol_response.raise_for_status()
            vol_data = vol_response.json()
            
            total_size = 0
            total_files = 0
            volume_count = 0
            
            # Navigate through the nested structure:
            # Volumes -> DataCenters -> {dc_name} -> {rack_name} -> {node_url} -> [volume_list]
            volumes_data = vol_data.get('Volumes', {})
            data_centers = volumes_data.get('DataCenters', {})
            
            for dc_name, dc_data in data_centers.items():
                for rack_name, rack_data in dc_data.items():
                    for node_url, volume_list in rack_data.items():
                        if isinstance(volume_list, list):
                            for volume in volume_list:
                                if isinstance(volume, dict):
                                    size = volume.get('Size', 0)
                                    files = volume.get('FileCount', 0)
                                    total_size += size
                                    total_files += files
                                    volume_count += 1
            
            # Convert to MB for readability
            total_mb = total_size / (1024 * 1024)
            
            logger.info(f"📊 Storage Status: {total_size:,} bytes ({total_mb:.2f} MB) | Files: {total_files} | Volumes: {volume_count}")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get storage status: {e}")
        except Exception as e:
            logger.error(f"Error parsing storage status: {e}")

def main():
    """Service entry point"""
    
    # Configuration
    MASTER_URL = "http://master:9333"
    WATCHED_DIR = "/app/watched"
    
    logger.info("=" * 60)
    logger.info("SeaweedFS File Upload Service Starting")
    logger.info("=" * 60)
    
    # Ensure watched directory exists
    watched_path = Path(WATCHED_DIR)
    watched_path.mkdir(parents=True, exist_ok=True)
    
    # Wait for SeaweedFS to be ready
    logger.info("Waiting for SeaweedFS master to be ready...")
    max_retries = 15
    retry_delay = 2
    
    for i in range(max_retries):
        try:
            response = requests.get(f"{MASTER_URL}/dir/status", timeout=5)
            if response.status_code == 200:
                logger.info("✓ SeaweedFS master is ready!")
                break
        except requests.exceptions.RequestException as e:
            logger.debug(f"Retry {i+1}/{max_retries}: {e}")
        
        if i == max_retries - 1:
            logger.error(f"Failed to connect to SeaweedFS master after {max_retries} attempts")
            return
        
        time.sleep(retry_delay)
    
    # Start monitoring
    event_handler = SeaweedFSUploader(MASTER_URL, WATCHED_DIR)
    observer = Observer()
    observer.schedule(event_handler, WATCHED_DIR, recursive=False)
    observer.start()
    
    logger.info(f"👁 Monitoring directory: {WATCHED_DIR}")
    logger.info("Press Ctrl+C to stop")
    logger.info("=" * 60)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("\nShutting down...")
        observer.stop()
    
    observer.join()
    logger.info("Service stopped")

if __name__ == "__main__":
    main()