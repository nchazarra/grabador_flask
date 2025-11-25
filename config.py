import os
import json
import logging
import secrets
from pathlib import Path

# Basic logging setup - will be enhanced later
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

class Config:
    # Directories
    BASE_DIR = Path(__file__).parent.absolute()
    OUTPUT_DIR = BASE_DIR / "recordings"
    TEMP_DIR = BASE_DIR / "temp"
    LOGS_DIR = BASE_DIR / "logs"
    
    # File paths
    CAMERAS_FILE = BASE_DIR / "cameras.json"
    SETTINGS_FILE = BASE_DIR / "settings.json"
    LOG_FILE = BASE_DIR / "app.log"
    
    # Default settings
    DEFAULT_SEGMENT_TIME = 60  # seconds
    DEFAULT_RETENTION_DAYS = 7  # days
    MAX_STORAGE_GB = 50  # GB
    
    # Flask settings
    SECRET_KEY = secrets.token_hex(32)
    HOST = '0.0.0.0'
    PORT = 5000
    DEBUG = False
    
    # Timezone
    TIMEZONE = 'Europe/Madrid'
    
    # FFmpeg settings
    FFMPEG_PATH = 'ffmpeg'
    FFMPEG_TIMEOUT = 60
    FFMPEG_RECONNECT_DELAY = 5
    FFMPEG_MAX_FAILURES = 5
    
    @classmethod
    def load_cameras(cls):
        """Load cameras from JSON file with validation"""
        try:
            if not cls.CAMERAS_FILE.exists():
                logging.warning(f"Cameras file not found: {cls.CAMERAS_FILE}")
                return {}
            
            with open(cls.CAMERAS_FILE, "r", encoding='utf-8') as f:
                cameras = json.load(f)
            
            # Validate camera configuration
            validated_cameras = {}
            for cam_id, cam_config in cameras.items():
                if not isinstance(cam_config, dict):
                    logging.warning(f"Invalid config for camera {cam_id}, skipping")
                    continue
                
                # Ensure required fields exist
                if 'name' not in cam_config:
                    cam_config['name'] = cam_id
                
                if 'rtsp_url' not in cam_config:
                    logging.warning(f"Camera {cam_id} has no rtsp_url configured")
                    cam_config['rtsp_url'] = ''
                
                # Validate coordinates if auto_recording is enabled
                if cam_config.get('auto_recording', False):
                    lat = cam_config.get('latitude')
                    lon = cam_config.get('longitude')
                    
                    if lat is None or lon is None:
                        logging.warning(
                            f"Camera {cam_id} has auto_recording=true but missing coordinates. "
                            "Auto recording will not work."
                        )
                    elif not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
                        logging.warning(
                            f"Camera {cam_id} has invalid coordinates: lat={lat}, lon={lon}"
                        )
                
                validated_cameras[cam_id] = cam_config
            
            logging.info(f"Loaded {len(validated_cameras)} cameras from configuration")
            return validated_cameras
            
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in cameras file: {e}")
            return {}
        except Exception as e:
            logging.error(f"Error loading cameras: {e}")
            return {}
    
    @classmethod
    def load_settings(cls):
        """Load or create settings with validation"""
        default_settings = {
            "segment_time": cls.DEFAULT_SEGMENT_TIME,
            "retention_days": cls.DEFAULT_RETENTION_DAYS,
            "max_storage_gb": cls.MAX_STORAGE_GB,
            "auto_cleanup": True,
            "default_encoding": "copy",
            "default_quality": "HIGH",
            "default_audio": False
        }
        
        if not cls.SETTINGS_FILE.exists():
            cls.save_settings(default_settings)
            return default_settings
        
        try:
            with open(cls.SETTINGS_FILE, "r", encoding='utf-8') as f:
                settings = json.load(f)
            
            # Merge with defaults to ensure all keys exist
            for key, value in default_settings.items():
                if key not in settings:
                    settings[key] = value
            
            # Validate settings ranges
            settings['segment_time'] = max(10, min(3600, int(settings.get('segment_time', 60))))
            settings['retention_days'] = max(1, min(365, int(settings.get('retention_days', 7))))
            settings['max_storage_gb'] = max(1, min(10000, int(settings.get('max_storage_gb', 50))))
            
            return settings
            
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in settings file: {e}")
            return default_settings
        except Exception as e:
            logging.error(f"Error loading settings: {e}")
            return default_settings
    
    @classmethod
    def save_settings(cls, settings):
        """Save settings with atomic write"""
        try:
            # Write to temp file first, then rename (atomic operation)
            temp_file = cls.SETTINGS_FILE.with_suffix('.tmp')
            with open(temp_file, "w", encoding='utf-8') as f:
                json.dump(settings, f, indent=4)
            
            temp_file.replace(cls.SETTINGS_FILE)
            return True
        except Exception as e:
            logging.error(f"Error saving settings: {e}")
            return False
    
    @classmethod
    def save_cameras(cls, cameras):
        """Save cameras configuration"""
        try:
            temp_file = cls.CAMERAS_FILE.with_suffix('.tmp')
            with open(temp_file, "w", encoding='utf-8') as f:
                json.dump(cameras, f, indent=4)
            
            temp_file.replace(cls.CAMERAS_FILE)
            return True
        except Exception as e:
            logging.error(f"Error saving cameras: {e}")
            return False

# Create necessary directories
Config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
Config.TEMP_DIR.mkdir(parents=True, exist_ok=True)
Config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
