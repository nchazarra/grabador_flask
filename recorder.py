import subprocess
import os
import threading
import datetime
import time
import signal
import logging
from threading import Lock

from config import Config

# Don't import get_logger here, we'll use basic logging
logger = logging.getLogger(__name__)

class Recorder:
    def __init__(self):
        self.recording_processes = {}
        self.process_lock = Lock()
        self.stop_flags = {}
        self.cameras = Config.load_cameras()
        self.settings = Config.load_settings()
    
    def record_rtsp_stream(self, rtsp_url, segment_time, output_dir, camera_id, retry_delay=5):
        """Record an RTSP stream with automatic reconnection and improved error handling"""
        camera_output_dir = os.path.join(output_dir, camera_id)
        os.makedirs(camera_output_dir, exist_ok=True)
        
        logger.info(f"Starting recording for camera {camera_id}")
        
        consecutive_failures = 0
        max_consecutive_failures = 5
        
        while True:
            # Check the stop flag for this camera
            if self.stop_flags.get(camera_id, False):
                logger.info(f"Stopping recording for camera {camera_id}.")
                break
            
            # If too many consecutive failures, increase the retry delay
            if consecutive_failures >= max_consecutive_failures:
                current_retry_delay = retry_delay * 2
                logger.warning(f"Multiple failures for camera {camera_id}. Increasing retry delay to {current_retry_delay}s")
            else:
                current_retry_delay = retry_delay
            
            try:
                # Create timestamp for file naming
                start_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                
                # Create log file
                log_file_path = os.path.join(camera_output_dir, "ffmpeg_log.txt")
                
                # Build ffmpeg command with improved parameters
                command = [
                    "ffmpeg",
                    "-rtsp_transport", "tcp",           # Use TCP for RTSP transport
                    "-i", rtsp_url,                     # Input RTSP URL
                    "-c:v", "copy",                     # Copy the video stream without re-encoding
                    "-an",                              # Disable audio recording
                    "-f", "segment",                    # Use segment muxer for splitting the output
                    "-segment_time", str(segment_time), # Segment duration
                    "-segment_format", "mp4",           # Force MP4 format for segments
                    "-reset_timestamps", "1",           # Reset timestamps for each segment
                    "-strftime", "1",                   # Enable strftime support for segment names
                    "-reconnect", "1",                  # Reconnect on errors
                    "-reconnect_at_eof", "1",           # Reconnect at EOF
                    "-reconnect_streamed", "1",         # Reconnect if stream is interrupted
                    "-reconnect_delay_max", "10",       # Max delay between reconnections
                    "-timeout", "60",                   # Connection timeout
                    "-stimeout", "60000000",            # Socket timeout in microseconds (60s)
                    os.path.join(camera_output_dir, f"{camera_id}_{start_time}_%03d.mp4")
                ]
                
                # Start the FFmpeg process
                with open(log_file_path, "ab") as log_file:
                    process = subprocess.Popen(command, stdout=log_file, stderr=subprocess.STDOUT)
                
                # Store the process reference
                with self.process_lock:
                    self.recording_processes[camera_id] = process
                
                # Wait for the process to complete
                process.wait()
                
                # Check if the process was manually stopped
                if self.stop_flags.get(camera_id, False):
                    logger.info(f"Recording stopped manually for camera {camera_id}.")
                    break
                else:
                    # Process ended for other reasons, restart after a delay
                    logger.warning(f"FFmpeg process for camera {camera_id} ended with code {process.returncode}. Restarting...")
                    if process.returncode != 0:
                        consecutive_failures += 1
                    else:
                        consecutive_failures = 0  # Reset failure counter on success
                    time.sleep(current_retry_delay)
            
            except Exception as e:
                consecutive_failures += 1
                logger.error(f"Error occurred for camera {camera_id}: {e}. Restarting...")
                time.sleep(current_retry_delay)
    
    def start_recording(self, camera_id, segment_time=None):
        """Start recording for a specific camera"""
        if segment_time is None:
            segment_time = self.settings.get("segment_time", Config.DEFAULT_SEGMENT_TIME)
        
        with self.process_lock:
            if camera_id not in self.recording_processes:
                if camera_id not in self.cameras:
                    logger.error(f"Camera {camera_id} not found in configuration")
                    return False
                
                rtsp_url = self.cameras[camera_id]["rtsp_url"]
                
                # Reset the stop flag if it was set
                self.stop_flags.pop(camera_id, None)
                
                # Start recording thread
                thread = threading.Thread(
                    target=self.record_rtsp_stream, 
                    args=(rtsp_url, segment_time, Config.OUTPUT_DIR, camera_id),
                    daemon=True
                )
                thread.start()
                return True
            else:
                logger.warning(f"Recording is already in progress for camera {camera_id}")
                return False
    
    def stop_recording(self, camera_id):
        """Stop recording for a specific camera"""
        with self.process_lock:
            if camera_id in self.recording_processes:
                # Set the stop flag for this camera
                self.stop_flags[camera_id] = True
                
                process = self.recording_processes[camera_id]
                
                # Send SIGINT for a graceful shutdown
                try:
                    process.send_signal(signal.SIGINT)
                    
                    # Wait for the process to terminate
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        logger.warning(f"Process for camera {camera_id} did not terminate gracefully, forcing kill")
                        process.kill()
                    
                    del self.recording_processes[camera_id]
                    return True
                
                except Exception as e:
                    logger.error(f"Error stopping recording for camera {camera_id}: {e}")
                    return False
            else:
                logger.warning(f"No recording in progress for camera {camera_id}")
                return False
    
    def start_all_recordings(self, segment_time=None):
        """Start recording for all cameras"""
        if segment_time is None:
            segment_time = self.settings.get("segment_time", Config.DEFAULT_SEGMENT_TIME)
        
        success_count = 0
        for camera_id in self.cameras:
            if self.start_recording(camera_id, segment_time):
                success_count += 1
        
        return success_count
    
    def stop_all_recordings(self):
        """Stop recording for all cameras"""
        with self.process_lock:
            camera_ids = list(self.recording_processes.keys())
            
            for camera_id in camera_ids:
                self.stop_recording(camera_id)
            
            # Clear the stop flags after stopping all recordings
            self.stop_flags.clear()
            
            return len(camera_ids)
    
    def get_recording_status(self):
        """Get status of all recording processes"""
        with self.process_lock:
            return list(self.recording_processes.keys())