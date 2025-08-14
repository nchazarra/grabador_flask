import os
import json
import logging

# Basic logging setup - will be enhanced later
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# Application configuration
class Config:
    # Directories
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    OUTPUT_DIR = os.path.join(BASE_DIR, "recordings")
    TEMP_DIR = os.path.join(BASE_DIR, "temp")
    LOGS_DIR = os.path.join(BASE_DIR, "logs")
    
    # File paths
    CAMERAS_FILE = os.path.join(BASE_DIR, "cameras.json")
    SETTINGS_FILE = os.path.join(BASE_DIR, "settings.json")
    LOG_FILE = os.path.join(BASE_DIR, "app.log")
    
    # Default settings
    DEFAULT_SEGMENT_TIME = 60  # seconds
    DEFAULT_RETENTION_DAYS = 7  # days
    MAX_STORAGE_GB = 50  # GB
    
    # Flask settings
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev_key_change_in_production')
    HOST = '0.0.0.0'
    PORT = 5000
    DEBUG = False
    
    # Load cameras from JSON file
    @staticmethod
    def load_cameras():
        try:
            with open(Config.CAMERAS_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading cameras: {e}")
            return {}
    
    # Load or create settings
    @staticmethod
    def load_settings():
        if not os.path.exists(Config.SETTINGS_FILE):
            default_settings = {
                "segment_time": Config.DEFAULT_SEGMENT_TIME,
                "retention_days": Config.DEFAULT_RETENTION_DAYS,
                "max_storage_gb": Config.MAX_STORAGE_GB,
                "auto_cleanup": True
            }
            with open(Config.SETTINGS_FILE, "w") as f:
                json.dump(default_settings, f, indent=4)
            return default_settings
        
        try:
            with open(Config.SETTINGS_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading settings: {e}")
            return {
                "segment_time": Config.DEFAULT_SEGMENT_TIME,
                "retention_days": Config.DEFAULT_RETENTION_DAYS,
                "max_storage_gb": Config.MAX_STORAGE_GB,
                "auto_cleanup": True
            }
    
    # Save settings
    @staticmethod
    def save_settings(settings):
        try:
            with open(Config.SETTINGS_FILE, "w") as f:
                json.dump(settings, f, indent=4)
            return True
        except Exception as e:
            logging.error(f"Error saving settings: {e}")
            return False

# Create necessary directories
os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
os.makedirs(Config.TEMP_DIR, exist_ok=True)
os.makedirs(Config.LOGS_DIR, exist_ok=True)