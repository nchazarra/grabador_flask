import os
import logging
import logging.handlers
import time
import re
from datetime import datetime
import threading

# Default log file path
DEFAULT_LOG_FILE = "app.log"

class LogManager:
    """Manages application logging and log file access"""
    
    _instance = None
    
    @staticmethod
    def get_instance(log_file=None):
        """Get or create the singleton instance"""
        if LogManager._instance is None:
            if log_file is None:
                log_file = DEFAULT_LOG_FILE  # Use default if not specified
            LogManager._instance = LogManager(log_file)
        return LogManager._instance
    
    def __init__(self, log_file=DEFAULT_LOG_FILE, max_size=10*1024*1024, backup_count=5):
        """
        Initialize the log manager
        
        Args:
            log_file (str): Path to the log file
            max_size (int): Maximum size of log file in bytes before rotation
            backup_count (int): Number of backup log files to keep
        """
        self.log_file = log_file if log_file else DEFAULT_LOG_FILE
        self.max_size = max_size
        self.backup_count = backup_count
        self.lock = threading.Lock()
        
        # Create directory for log file if doesn't exist
        log_dir = os.path.dirname(self.log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        # Set up logging
        self._setup_logging()
    
    def _setup_logging(self):
        """Configure the logging system"""
        # Create root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        
        # Clear existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Create a formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Create a file handler for log rotation
        file_handler = logging.handlers.RotatingFileHandler(
            self.log_file,
            maxBytes=self.max_size,
            backupCount=self.backup_count
        )
        file_handler.setFormatter(formatter)
        
        # Create a stream handler for console output
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        # Add handlers to root logger
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)
        
        # Create a logger for this class
        self.logger = logging.getLogger(__name__)
        self.logger.info("Logging system initialized with log file: %s", self.log_file)
    
    def get_logger(self, name):
        """
        Get a logger with the specified name
        
        Args:
            name (str): Name for the logger, typically __name__ of the module
            
        Returns:
            logging.Logger: Configured logger instance
        """
        return logging.getLogger(name)
    
    def get_logs(self, n=100, log_level=None, module=None, start_date=None, end_date=None):
        """
        Get the most recent logs from the log file
        
        Args:
            n (int): Maximum number of log entries to retrieve
            log_level (str, optional): Filter by log level (INFO, WARNING, ERROR)
            module (str, optional): Filter by module name
            start_date (datetime, optional): Filter logs after this date
            end_date (datetime, optional): Filter logs before this date
            
        Returns:
            list: List of log entries as strings
        """
        with self.lock:
            try:
                if not os.path.exists(self.log_file):
                    return []
                
                with open(self.log_file, 'r') as f:
                    logs = f.readlines()
                
                # Filter logs if necessary
                if log_level or module or start_date or end_date:
                    filtered_logs = []
                    date_pattern = r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
                    
                    for log in logs:
                        # Check if this log entry should be included based on filters
                        include = True
                        
                        # Filter by log level
                        if log_level and log_level.upper() not in log:
                            include = False
                        
                        # Filter by module
                        if module and module not in log:
                            include = False
                        
                        # Filter by date range
                        if start_date or end_date:
                            date_match = re.match(date_pattern, log)
                            if date_match:
                                log_date_str = date_match.group(0)
                                try:
                                    log_date = datetime.strptime(log_date_str, '%Y-%m-%d %H:%M:%S')
                                    
                                    if start_date and log_date < start_date:
                                        include = False
                                    
                                    if end_date and log_date > end_date:
                                        include = False
                                except ValueError:
                                    # If date parsing fails, include the log by default
                                    pass
                        
                        if include:
                            filtered_logs.append(log)
                    
                    logs = filtered_logs
                
                # Return the last n logs, newest first
                return reversed(logs[-n:]) if logs else []
            
            except Exception as e:
                self.logger.error(f"Error reading logs: {e}")
                return []
    
    def clear_logs(self):
        """
        Clear all logs (reset the log file)
        
        Returns:
            bool: True if successful, False otherwise
        """
        with self.lock:
            try:
                with open(self.log_file, 'w') as f:
                    f.write(f"Logs cleared at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                self.logger.info("Log file cleared")
                return True
            except Exception as e:
                self.logger.error(f"Error clearing logs: {e}")
                return False
    
    def get_log_file_size(self):
        """
        Get the current size of the log file
        
        Returns:
            int: Size of the log file in bytes
        """
        try:
            return os.path.getsize(self.log_file)
        except OSError:
            return 0
    
    def get_log_stats(self):
        """
        Get statistics about the logs
        
        Returns:
            dict: Statistics including counts by log level, file size, etc.
        """
        stats = {
            'total_entries': 0,
            'levels': {
                'INFO': 0,
                'WARNING': 0,
                'ERROR': 0,
                'DEBUG': 0,
                'CRITICAL': 0
            },
            'file_size': self.get_log_file_size(),
            'file_size_formatted': self._format_size(self.get_log_file_size())
        }
        
        try:
            with open(self.log_file, 'r') as f:
                for line in f:
                    stats['total_entries'] += 1
                    
                    # Count by log level
                    for level in stats['levels'].keys():
                        if f" - {level} - " in line:
                            stats['levels'][level] += 1
                            break
            
            return stats
        
        except Exception as e:
            self.logger.error(f"Error getting log stats: {e}")
            return stats
    
    def _format_size(self, size):
        """Format file size in human-readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} PB"

# Helper function to get a logger
def get_logger(name):
    """Get a logger with the specified name"""
    return LogManager.get_instance().get_logger(name)

# Initialize with a default log file
log_manager = LogManager.get_instance(DEFAULT_LOG_FILE)