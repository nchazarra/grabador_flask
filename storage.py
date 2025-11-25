import os
import shutil
import datetime
import zipfile
import logging
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Any

from config import Config

logger = logging.getLogger(__name__)


class StorageManager:
    def __init__(self):
        self.settings = Config.load_settings()
        self.cleanup_thread: Optional[threading.Thread] = None
        self.cleanup_lock = threading.Lock()
        self.stop_cleanup = threading.Event()
        
        # Disk space warning thresholds
        self.warning_threshold = 0.9  # 90% full
        self.critical_threshold = 0.95  # 95% full
        
        # Start background cleanup if enabled
        if self.settings.get("auto_cleanup", True):
            self.start_background_cleanup()
    
    def get_disk_usage(self) -> Dict[str, int]:
        """Get disk usage for the recordings directory"""
        try:
            stat = shutil.disk_usage(Config.OUTPUT_DIR)
            recordings_size = self._calculate_directory_size(str(Config.OUTPUT_DIR))
            
            return {
                'total': stat.total,
                'used': stat.used,
                'free': stat.free,
                'recordings_size': recordings_size,
                'free_percentage': (stat.free / stat.total * 100) if stat.total > 0 else 0
            }
        except Exception as e:
            logger.error(f"Error getting disk usage: {e}")
            return {'total': 0, 'used': 0, 'free': 0, 'recordings_size': 0, 'free_percentage': 0}
    
    def _calculate_directory_size(self, directory: str) -> int:
        """Calculate total size of a directory"""
        total_size = 0
        try:
            for dirpath, dirnames, filenames in os.walk(directory):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    try:
                        total_size += os.path.getsize(filepath)
                    except (OSError, FileNotFoundError):
                        pass
        except Exception as e:
            logger.error(f"Error calculating directory size: {e}")
        return total_size
    
    def get_storage_usage(self) -> Dict[str, Any]:
        """Calculate total storage usage for recordings"""
        disk = self.get_disk_usage()
        max_storage = self.settings.get("max_storage_gb", Config.MAX_STORAGE_GB) * 1024 * 1024 * 1024
        
        # Check disk space warnings
        warnings = []
        if disk['free_percentage'] < 5:
            warnings.append("CRITICAL: Less than 5% disk space remaining!")
        elif disk['free_percentage'] < 10:
            warnings.append("WARNING: Less than 10% disk space remaining")
        
        recordings_percentage = (disk['recordings_size'] / max_storage * 100) if max_storage > 0 else 0
        if recordings_percentage > 95:
            warnings.append("CRITICAL: Recordings exceeding storage limit!")
        elif recordings_percentage > 90:
            warnings.append("WARNING: Recordings approaching storage limit")
        
        return {
            'size_bytes': disk['recordings_size'],
            'size_formatted': self.format_size(disk['recordings_size']),
            'max_size': max_storage,
            'max_size_formatted': self.format_size(max_storage),
            'percentage': round(recordings_percentage, 2),
            'disk_total': disk['total'],
            'disk_total_formatted': self.format_size(disk['total']),
            'disk_free': disk['free'],
            'disk_free_formatted': self.format_size(disk['free']),
            'disk_free_percentage': round(disk['free_percentage'], 2),
            'warnings': warnings
        }
    
    def format_size(self, size: int) -> str:
        """Format file size in human-readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} PB"
    
    def parse_filename_date(self, filename: str) -> Optional[datetime.datetime]:
        """Parse date from recording filename"""
        # Expected format: camera_id_YYYY-MM-DD_HH-MM-SS_XXX.mp4
        try:
            parts = filename.split('_')
            if len(parts) >= 3:
                date_str = f"{parts[1]}_{parts[2]}"
                return datetime.datetime.strptime(date_str, "%Y-%m-%d_%H-%M-%S")
        except (ValueError, IndexError):
            pass
        return None
    
    def clear_old_recordings(self, days: Optional[int] = None) -> Dict[str, Any]:
        """Remove recordings older than specified days"""
        if days is None:
            days = self.settings.get("retention_days", Config.DEFAULT_RETENTION_DAYS)
        
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days)
        removed_count = 0
        total_size_freed = 0
        errors = []
        
        logger.info(f"Clearing recordings older than {days} days (before {cutoff_date})")
        
        try:
            for camera_dir in Path(Config.OUTPUT_DIR).iterdir():
                if not camera_dir.is_dir():
                    continue
                
                for file_path in camera_dir.iterdir():
                    if file_path.name == "ffmpeg_log.txt":
                        continue
                    
                    if not file_path.is_file():
                        continue
                    
                    # Try to get date from filename first
                    file_date = self.parse_filename_date(file_path.name)
                    
                    # Fall back to modification time if parsing fails
                    if file_date is None:
                        file_date = datetime.datetime.fromtimestamp(file_path.stat().st_mtime)
                    
                    if file_date < cutoff_date:
                        try:
                            file_size = file_path.stat().st_size
                            file_path.unlink()
                            removed_count += 1
                            total_size_freed += file_size
                            logger.debug(f"Removed old file: {file_path}")
                        except Exception as e:
                            errors.append(f"Failed to remove {file_path}: {e}")
                            logger.error(f"Error removing file {file_path}: {e}")
        
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            errors.append(str(e))
        
        logger.info(f"Removed {removed_count} files, freed {self.format_size(total_size_freed)}")
        
        return {
            'removed_count': removed_count,
            'size_freed': total_size_freed,
            'size_freed_formatted': self.format_size(total_size_freed),
            'errors': errors
        }
    
    def clear_by_storage_limit(self) -> Dict[str, Any]:
        """Remove oldest recordings to stay under storage limit"""
        max_storage = self.settings.get("max_storage_gb", Config.MAX_STORAGE_GB) * 1024 * 1024 * 1024
        target_size = int(max_storage * 0.8)  # Target 80% of limit
        
        current_size = self._calculate_directory_size(str(Config.OUTPUT_DIR))
        
        if current_size <= max_storage * self.warning_threshold:
            return {'removed_count': 0, 'size_freed': 0, 'size_freed_formatted': '0 B'}
        
        logger.info(f"Storage limit cleanup: current {self.format_size(current_size)}, target {self.format_size(target_size)}")
        
        # Get all recordings sorted by date (oldest first)
        all_files = []
        for camera_dir in Path(Config.OUTPUT_DIR).iterdir():
            if not camera_dir.is_dir():
                continue
            
            for file_path in camera_dir.iterdir():
                if file_path.name == "ffmpeg_log.txt" or not file_path.is_file():
                    continue
                
                file_date = self.parse_filename_date(file_path.name)
                if file_date is None:
                    file_date = datetime.datetime.fromtimestamp(file_path.stat().st_mtime)
                
                all_files.append({
                    'path': file_path,
                    'date': file_date,
                    'size': file_path.stat().st_size
                })
        
        # Sort by date (oldest first)
        all_files.sort(key=lambda x: x['date'])
        
        removed_count = 0
        total_size_freed = 0
        
        for file_info in all_files:
            if current_size <= target_size:
                break
            
            try:
                file_info['path'].unlink()
                current_size -= file_info['size']
                total_size_freed += file_info['size']
                removed_count += 1
                logger.debug(f"Removed for space: {file_info['path']}")
            except Exception as e:
                logger.error(f"Error removing {file_info['path']}: {e}")
        
        logger.info(f"Storage cleanup: removed {removed_count} files, freed {self.format_size(total_size_freed)}")
        
        return {
            'removed_count': removed_count,
            'size_freed': total_size_freed,
            'size_freed_formatted': self.format_size(total_size_freed)
        }
    
    def clear_all_recordings(self) -> Dict[str, Any]:
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
        
        logger.info(f"Cleared all recordings: {removed_count} files, {self.format_size(total_size_freed)}")
        
        return {
            'removed_count': removed_count,
            'size_freed': total_size_freed,
            'size_freed_formatted': self.format_size(total_size_freed)
        }
    
    def get_recordings_list(self, camera_id: Optional[str] = None, 
                           limit: Optional[int] = None) -> Dict[str, Any]:
        """Get list of all recordings grouped by camera"""
        recordings: Dict[str, List] = {}
        total_count = 0
        total_size = 0
        
        try:
            for camera_dir in Path(Config.OUTPUT_DIR).iterdir():
                if not camera_dir.is_dir():
                    continue
                
                cam_id = camera_dir.name
                
                # Filter by camera if specified
                if camera_id and cam_id != camera_id:
                    continue
                
                camera_files = []
                
                for file_path in camera_dir.iterdir():
                    if file_path.name == "ffmpeg_log.txt" or not file_path.is_file():
                        continue
                    
                    try:
                        stat = file_path.stat()
                        file_size = stat.st_size
                        
                        # Parse recording date
                        recording_date = self.parse_filename_date(file_path.name)
                        if recording_date is None:
                            recording_date = datetime.datetime.fromtimestamp(stat.st_mtime)
                        
                        camera_files.append({
                            'filename': file_path.name,
                            'path': f"{cam_id}/{file_path.name}",
                            'size': file_size,
                            'size_formatted': self.format_size(file_size),
                            'date': recording_date.strftime("%Y-%m-%d %H:%M:%S"),
                            'date_iso': recording_date.isoformat(),
                            'age_days': (datetime.datetime.now() - recording_date).days
                        })
                        
                        total_size += file_size
                        total_count += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing file {file_path}: {e}")
                
                # Sort by date (newest first)
                camera_files.sort(key=lambda x: x['date'], reverse=True)
                
                # Apply limit if specified
                if limit:
                    camera_files = camera_files[:limit]
                
                if camera_files:
                    recordings[cam_id] = camera_files
        
        except Exception as e:
            logger.error(f"Error listing recordings: {e}")
        
        return {
            'by_camera': recordings,
            'total_count': total_count,
            'total_size': total_size,
            'total_size_formatted': self.format_size(total_size)
        }
    
    def create_zip_archive(self, files: Optional[List[str]] = None, 
                          remove_after: bool = False) -> Dict[str, Any]:
        """Create a ZIP archive of recordings"""
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_filename = Path(Config.TEMP_DIR) / f"recordings_{timestamp}.zip"
        
        try:
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                if files:
                    for file_path in files:
                        full_path = Path(Config.OUTPUT_DIR) / file_path
                        if full_path.is_file():
                            zipf.write(full_path, file_path)
                else:
                    for root, dirs, filenames in os.walk(Config.OUTPUT_DIR):
                        for filename in filenames:
                            if filename == "ffmpeg_log.txt":
                                continue
                            
                            file_path = Path(root) / filename
                            if file_path.is_file():
                                arcname = file_path.relative_to(Config.OUTPUT_DIR)
                                zipf.write(file_path, arcname)
            
            if remove_after and files:
                for file_path in files:
                    full_path = Path(Config.OUTPUT_DIR) / file_path
                    if full_path.is_file():
                        full_path.unlink()
            
            zip_size = zip_filename.stat().st_size
            
            return {
                'success': True,
                'filename': zip_filename.name,
                'filepath': str(zip_filename),
                'size': zip_size,
                'size_formatted': self.format_size(zip_size)
            }
        
        except Exception as e:
            logger.error(f"Error creating ZIP archive: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def cleanup_temp_files(self, max_age_hours: int = 24):
        """Clean up old temporary files"""
        cutoff = datetime.datetime.now() - datetime.timedelta(hours=max_age_hours)
        removed = 0
        
        try:
            for file_path in Path(Config.TEMP_DIR).iterdir():
                if file_path.is_file():
                    mtime = datetime.datetime.fromtimestamp(file_path.stat().st_mtime)
                    if mtime < cutoff:
                        file_path.unlink()
                        removed += 1
        except Exception as e:
            logger.error(f"Error cleaning temp files: {e}")
        
        if removed > 0:
            logger.info(f"Cleaned up {removed} temporary files")
        
        return removed
    
    def start_background_cleanup(self) -> bool:
        """Start background thread for automatic cleanup"""
        if self.cleanup_thread is not None and self.cleanup_thread.is_alive():
            logger.warning("Cleanup thread is already running")
            return False
        
        self.stop_cleanup.clear()
        self.cleanup_thread = threading.Thread(
            target=self._background_cleanup,
            daemon=True,
            name="StorageCleanup"
        )
        self.cleanup_thread.start()
        logger.info("Started background cleanup thread")
        return True
    
    def stop_background_cleanup(self) -> bool:
        """Stop background cleanup thread"""
        if self.cleanup_thread is None or not self.cleanup_thread.is_alive():
            logger.warning("Cleanup thread is not running")
            return False
        
        self.stop_cleanup.set()
        self.cleanup_thread.join(timeout=5)
        logger.info("Stopped background cleanup thread")
        return True
    
    def _background_cleanup(self):
        """Background task to manage storage and clean up old recordings"""
        check_interval = 3600  # Check every hour
        
        while not self.stop_cleanup.is_set():
            try:
                with self.cleanup_lock:
                    # Reload settings in case they changed
                    self.settings = Config.load_settings()
                    
                    # Clean up by age
                    retention_days = self.settings.get("retention_days", Config.DEFAULT_RETENTION_DAYS)
                    if retention_days > 0:
                        self.clear_old_recordings(retention_days)
                    
                    # Clean up by storage limit
                    self.clear_by_storage_limit()
                    
                    # Clean up temp files
                    self.cleanup_temp_files()
                    
                    # Check disk space and log warnings
                    usage = self.get_storage_usage()
                    for warning in usage.get('warnings', []):
                        logger.warning(warning)
            
            except Exception as e:
                logger.error(f"Error in background cleanup: {e}")
            
            # Wait for next check interval
            self.stop_cleanup.wait(check_interval)
