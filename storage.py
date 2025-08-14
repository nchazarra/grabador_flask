import os
import shutil
import datetime
import zipfile
import logging
import threading
import time

from config import Config

# Use basic logging instead of importing get_logger
logger = logging.getLogger(__name__)

class StorageManager:
    def __init__(self):
        self.settings = Config.load_settings()
        self.cleanup_thread = None
        self.cleanup_lock = threading.Lock()
        self.stop_cleanup = False
        
        # Start background cleanup if enabled
        if self.settings.get("auto_cleanup", True):
            self.start_background_cleanup()
    
    def get_storage_usage(self):
        """Calculate total storage usage for recordings"""
        total_size = 0
        for root, dirs, files in os.walk(Config.OUTPUT_DIR):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    total_size += os.path.getsize(file_path)
                except (FileNotFoundError, PermissionError) as e:
                    logger.error(f"Error getting size for {file_path}: {e}")
        
        return {
            'size_bytes': total_size,
            'size_formatted': self.format_size(total_size),
            'max_size': self.settings.get("max_storage_gb", Config.MAX_STORAGE_GB) * 1024 * 1024 * 1024,
            'percentage': round((total_size / (self.settings.get("max_storage_gb", Config.MAX_STORAGE_GB) * 1024 * 1024 * 1024)) * 100, 2) if total_size > 0 else 0
        }
    
    def format_size(self, size):
        """Format file size in human-readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} PB"
    
    def clear_old_recordings(self, days=None):
        """Remove recordings older than specified days"""
        if days is None:
            days = self.settings.get("retention_days", Config.DEFAULT_RETENTION_DAYS)
        
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days)
        removed_count = 0
        total_size_freed = 0
        
        logger.info(f"Clearing recordings older than {days} days")
        
        for camera_id in os.listdir(Config.OUTPUT_DIR):
            camera_dir = os.path.join(Config.OUTPUT_DIR, camera_id)
            if not os.path.isdir(camera_dir):
                continue
            
            for file in os.listdir(camera_dir):
                if file == "ffmpeg_log.txt":
                    continue
                
                file_path = os.path.join(camera_dir, file)
                if not os.path.isfile(file_path):
                    continue
                
                # Extract date from filename (format: camera_id_YYYY-MM-DD_HH-MM-SS_XXX.mp4)
                try:
                    # Parse date from filename
                    date_str = '_'.join(file.split('_')[1:3])  # Extract YYYY-MM-DD_HH-MM-SS part
                    file_date = datetime.datetime.strptime(date_str, "%Y-%m-%d_%H-%M-%S")
                    
                    if file_date < cutoff_date:
                        file_size = os.path.getsize(file_path)
                        os.remove(file_path)
                        removed_count += 1
                        total_size_freed += file_size
                except (ValueError, IndexError, OSError) as e:
                    logger.error(f"Error processing file {file_path}: {e}")
        
        logger.info(f"Removed {removed_count} files, freed {self.format_size(total_size_freed)}")
        return {
            'removed_count': removed_count,
            'size_freed': total_size_freed,
            'size_freed_formatted': self.format_size(total_size_freed)
        }
    
    def clear_all_recordings(self):
        """Remove all recordings while preserving directory structure"""
        removed_count = 0
        total_size_freed = 0
        
        for root, dirs, files in os.walk(Config.OUTPUT_DIR):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    file_size = os.path.getsize(file_path)
                    os.remove(file_path)
                    removed_count += 1
                    total_size_freed += file_size
                except (FileNotFoundError, PermissionError) as e:
                    logger.error(f"Error removing file {file_path}: {e}")
        
        return {
            'removed_count': removed_count,
            'size_freed': total_size_freed,
            'size_freed_formatted': self.format_size(total_size_freed)
        }
    
    def get_recordings_list(self):
        """Get list of all recordings grouped by camera"""
        recordings = {}
        total_count = 0
        
        for camera_id in os.listdir(Config.OUTPUT_DIR):
            camera_dir = os.path.join(Config.OUTPUT_DIR, camera_id)
            if not os.path.isdir(camera_dir):
                continue
            
            camera_files = []
            for file in os.listdir(camera_dir):
                if file == "ffmpeg_log.txt":
                    continue
                
                file_path = os.path.join(camera_dir, file)
                if not os.path.isfile(file_path):
                    continue
                
                # Get file information
                file_size = os.path.getsize(file_path)
                file_mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
                
                # Extract recording date from filename if possible
                try:
                    date_str = '_'.join(file.split('_')[1:3])
                    recording_date = datetime.datetime.strptime(date_str, "%Y-%m-%d_%H-%M-%S")
                except (ValueError, IndexError):
                    recording_date = file_mod_time
                
                camera_files.append({
                    'filename': file,
                    'path': f"{camera_id}/{file}",
                    'size': file_size,
                    'size_formatted': self.format_size(file_size),
                    'date': recording_date.strftime("%Y-%m-%d %H:%M:%S"),
                    'age_days': (datetime.datetime.now() - recording_date).days
                })
                
                total_count += 1
            
            # Sort files by date (newest first)
            camera_files.sort(key=lambda x: x['date'], reverse=True)
            recordings[camera_id] = camera_files
        
        return {
            'by_camera': recordings,
            'total_count': total_count
        }
    
    def create_zip_archive(self, files=None, remove_after=False):
        """Create a ZIP archive of recordings"""
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_filename = os.path.join(Config.TEMP_DIR, f"recordings_{timestamp}.zip")
        
        try:
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # If specific files are provided, only include those
                if files:
                    for file_path in files:
                        full_path = os.path.join(Config.OUTPUT_DIR, file_path)
                        if os.path.isfile(full_path):
                            arcname = file_path  # Preserve relative path in archive
                            zipf.write(full_path, arcname)
                else:
                    # Otherwise, include all recordings
                    for root, dirs, files in os.walk(Config.OUTPUT_DIR):
                        for file in files:
                            if file == "ffmpeg_log.txt":
                                continue
                            
                            file_path = os.path.join(root, file)
                            if os.path.isfile(file_path):
                                arcname = os.path.relpath(file_path, Config.OUTPUT_DIR)
                                zipf.write(file_path, arcname)
            
            # Optionally remove files after zipping
            if remove_after and files:
                for file_path in files:
                    full_path = os.path.join(Config.OUTPUT_DIR, file_path)
                    if os.path.isfile(full_path):
                        os.remove(full_path)
            
            return {
                'success': True,
                'filename': os.path.basename(zip_filename),
                'filepath': zip_filename,
                'size': os.path.getsize(zip_filename),
                'size_formatted': self.format_size(os.path.getsize(zip_filename))
            }
            
        except Exception as e:
            logger.error(f"Error creating ZIP archive: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def start_background_cleanup(self):
        """Start background thread for automatic cleanup"""
        if self.cleanup_thread is not None and self.cleanup_thread.is_alive():
            logger.warning("Cleanup thread is already running")
            return False
        
        self.stop_cleanup = False
        self.cleanup_thread = threading.Thread(target=self._background_cleanup, daemon=True)
        self.cleanup_thread.start()
        logger.info("Started background cleanup thread")
        return True
    
    def stop_background_cleanup(self):
        """Stop background cleanup thread"""
        if self.cleanup_thread is None or not self.cleanup_thread.is_alive():
            logger.warning("Cleanup thread is not running")
            return False
        
        self.stop_cleanup = True
        self.cleanup_thread.join(timeout=5)
        logger.info("Stopped background cleanup thread")
        return True
    
    def _background_cleanup(self):
        """Background task to manage storage and clean up old recordings"""
        check_interval = 3600  # Check every hour
        
        while not self.stop_cleanup:
            try:
                with self.cleanup_lock:
                    # Check if we need to clean up by age
                    retention_days = self.settings.get("retention_days", Config.DEFAULT_RETENTION_DAYS)
                    if retention_days > 0:
                        self.clear_old_recordings(retention_days)
                    
                    # Check if we need to clean up by storage limit
                    storage_info = self.get_storage_usage()
                    max_storage_bytes = self.settings.get("max_storage_gb", Config.MAX_STORAGE_GB) * 1024 * 1024 * 1024
                    
                    if storage_info['size_bytes'] > max_storage_bytes * 0.9:  # Over 90% of limit
                        logger.warning(f"Storage usage ({storage_info['size_formatted']}) exceeds 90% of limit. Cleaning up...")
                        
                        # Get recordings sorted by age (oldest first)
                        recordings = self.get_recordings_list()
                        if not recordings['by_camera']:
                            continue
                        
                        # Flatten and sort all recordings by date
                        all_files = []
                        for camera_id, files in recordings['by_camera'].items():
                            for file in files:
                                all_files.append(file)
                        
                        all_files.sort(key=lambda x: x['date'])
                        
                        # Delete oldest files until we're under 80% of limit
                        target_size = max_storage_bytes * 0.8
                        current_size = storage_info['size_bytes']
                        
                        for file in all_files:
                            if current_size <= target_size:
                                break
                                
                            file_path = os.path.join(Config.OUTPUT_DIR, file['path'])
                            try:
                                os.remove(file_path)
                                current_size -= file['size']
                                logger.info(f"Removed old file: {file_path}")
                            except (FileNotFoundError, PermissionError) as e:
                                logger.error(f"Error removing file {file_path}: {e}")
            
            except Exception as e:
                logger.error(f"Error in background cleanup: {e}")
            
            # Sleep for the check interval
            for _ in range(check_interval):
                if self.stop_cleanup:
                    break
                time.sleep(1)