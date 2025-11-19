import subprocess
import os
import threading
import datetime
import time
import signal
import logging
import platform
from threading import Lock
from enum import Enum

from config import Config

logger = logging.getLogger(__name__)

class EncodingPreset(Enum):
    """Video encoding presets"""
    COPY = "copy"
    H264_CPU = "h264_cpu"
    H264_GPU_NVIDIA = "h264_nvenc"
    H264_GPU_AMD = "h264_amf"
    H264_GPU_INTEL = "h264_qsv"
    H265_CPU = "h265_cpu"
    H265_GPU_NVIDIA = "h265_nvenc"
    H265_GPU_AMD = "h265_amf"

class VideoQuality(Enum):
    """Video quality presets"""
    LOW = {"bitrate": "500k", "resolution": "640x480", "fps": 15}
    MEDIUM = {"bitrate": "1500k", "resolution": "1280x720", "fps": 25}
    HIGH = {"bitrate": "3000k", "resolution": "1920x1080", "fps": 30}
    ULTRA = {"bitrate": "6000k", "resolution": "1920x1080", "fps": 30}
    CUSTOM = {}

class Recorder:
    def __init__(self):
        self.recording_processes = {}
        self.process_lock = Lock()
        self.stop_flags = {}
        self.cameras = Config.load_cameras()
        self.settings = Config.load_settings()
        self.gpu_available = self._detect_gpu()
        self.encoding_capabilities = self._detect_encoding_capabilities()

    def _detect_gpu(self):
        """Detect available GPU hardware for encoding"""
        gpu_info = {'nvidia': False, 'amd': False, 'intel': False, 'type': None}
        try:
            nvidia_check = subprocess.run(['nvidia-smi'], capture_output=True, text=True)
            if nvidia_check.returncode == 0:
                gpu_info['nvidia'] = True
                gpu_info['type'] = 'nvidia'
                logger.info("NVIDIA GPU detected")
        except (FileNotFoundError, subprocess.SubprocessError):
            pass
        # Add other GPU detection logic here if needed (AMD, Intel)
        return gpu_info

    def _detect_encoding_capabilities(self):
        """Detect which encoding methods are available"""
        capabilities = [EncodingPreset.COPY, EncodingPreset.H264_CPU, EncodingPreset.H265_CPU]
        if self.gpu_available['nvidia']:
            capabilities.extend([EncodingPreset.H264_GPU_NVIDIA, EncodingPreset.H265_GPU_NVIDIA])
        # Add other GPU capabilities here
        logger.info(f"Available encoding capabilities: {[c.value for c in capabilities]}")
        return capabilities

    def _build_encoding_params(self, preset, quality, custom_params=None):
        """Build FFmpeg encoding parameters"""
        params = []
        if preset == EncodingPreset.COPY:
            params.extend(['-c:v', 'copy'])
        elif preset == EncodingPreset.H264_CPU:
            params.extend(['-c:v', 'libx264', '-preset', 'medium', '-crf', '23'])
        elif preset == EncodingPreset.H264_GPU_NVIDIA:
            params.extend(['-c:v', 'h264_nvenc', '-preset', 'p4', '-rc', 'vbr', '-cq', '23', '-b:v', quality.value.get('bitrate', '2000k')])
        elif preset == EncodingPreset.H265_CPU:
            params.extend(['-c:v', 'libx265', '-preset', 'medium', '-crf', '28'])
        elif preset == EncodingPreset.H265_GPU_NVIDIA:
            params.extend(['-c:v', 'hevc_nvenc', '-preset', 'p4', '-rc', 'vbr', '-cq', '28', '-b:v', quality.value.get('bitrate', '1500k')])
        
        if preset != EncodingPreset.COPY and quality != VideoQuality.CUSTOM:
            if 'resolution' in quality.value:
                params.extend(['-s', quality.value['resolution']])
            if 'fps' in quality.value:
                params.extend(['-r', str(quality.value['fps'])])
        
        if custom_params:
            params.extend(custom_params)
        return params

    def record_rtsp_stream(self, rtsp_url, segment_time, output_dir, camera_id,
                          encoding_preset=EncodingPreset.COPY,
                          quality=VideoQuality.HIGH,
                          audio_enabled=False,
                          custom_params=None,
                          retry_delay=5):
        camera_output_dir = os.path.join(output_dir, camera_id)
        os.makedirs(camera_output_dir, exist_ok=True)
        logger.info(f"Starting recording for camera {camera_id} with encoding: {encoding_preset.value}")
        consecutive_failures = 0
        max_consecutive_failures = 5
        
        while not self.stop_flags.get(camera_id, False):
            # Check stop flag at the beginning of each loop iteration
            if self.stop_flags.get(camera_id, False):
                logger.info(f"Stop flag detected for camera {camera_id}, exiting recording loop")
                break
                
            if consecutive_failures >= max_consecutive_failures:
                current_retry_delay = retry_delay * 2
                logger.warning(f"Multiple failures for camera {camera_id}. Increasing retry delay to {current_retry_delay}s")
            else:
                current_retry_delay = retry_delay
            try:
                start_time = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
                log_file_path = os.path.join(camera_output_dir, "ffmpeg_log.txt")
                command = ["ffmpeg", "-rtsp_transport", "tcp", "-i", rtsp_url]
                encoding_params = self._build_encoding_params(encoding_preset, quality, custom_params)
                command.extend(encoding_params)
                if audio_enabled:
                    command.extend(['-c:a', 'aac', '-b:a', '128k'])
                else:
                    command.extend(['-an'])
                command.extend([
                    "-f", "segment", "-segment_time", str(segment_time),
                    "-segment_format", "mp4", "-reset_timestamps", "1",
                    "-reconnect", "1", "-reconnect_at_eof", "1",
                    "-reconnect_streamed", "1", "-reconnect_delay_max", "10",
                    "-timeout", "60"
                ])
                ext = "mp4"
                if encoding_preset in [EncodingPreset.H265_CPU, EncodingPreset.H265_GPU_NVIDIA, EncodingPreset.H265_GPU_AMD]:
                    ext = "hevc"
                output_pattern = os.path.join(camera_output_dir, f"{camera_id}_{start_time}_%03d.{ext}")
                command.append(output_pattern)
                logger.debug(f"FFmpeg command: {' '.join(command)}")
                
                # Don't use process groups on Windows - it causes issues with FFmpeg termination
                creation_flags = 0
                
                with open(log_file_path, "ab") as log_file:
                    # Enable stdin pipe for Windows to send 'q' command
                    process = subprocess.Popen(
                        command,
                        stdin=subprocess.PIPE if platform.system() == "Windows" else None,
                        stdout=log_file,
                        stderr=subprocess.STDOUT,
                        creationflags=creation_flags
                    )
                    
                with self.process_lock:
                    self.recording_processes[camera_id] = process
                
                # Monitor process with periodic stop flag checks
                while process.poll() is None:
                    # Check stop flag every second
                    if self.stop_flags.get(camera_id, False):
                        logger.info(f"Stop flag detected during recording for camera {camera_id}")
                        break
                    time.sleep(1)
                
                # If we broke out due to stop flag, don't retry
                if self.stop_flags.get(camera_id, False):
                    logger.info(f"Recording stopped by user for camera {camera_id}")
                    break
                
                # Process ended naturally
                logger.warning(f"FFmpeg process for camera {camera_id} ended with code {process.returncode}. Restarting...")
                consecutive_failures = consecutive_failures + 1 if process.returncode != 0 else 0
                time.sleep(current_retry_delay)
                
            except Exception as e:
                consecutive_failures += 1
                logger.error(f"Error occurred for camera {camera_id}: {e}. Restarting...")
                time.sleep(current_retry_delay)
        
        logger.info(f"Recording thread for camera {camera_id} has exited")

    def start_recording(self, camera_id, segment_time=None, encoding_preset=None, 
                       quality=None, audio_enabled=False, custom_params=None):
        """Start recording for a specific camera with encoding options"""
        if segment_time is None:
            segment_time = self.settings.get("segment_time", Config.DEFAULT_SEGMENT_TIME)
        
        if encoding_preset is None:
            encoding_preset = EncodingPreset.COPY
        elif isinstance(encoding_preset, str):
            encoding_preset = EncodingPreset(encoding_preset)
            
        if quality is None:
            quality = VideoQuality.HIGH
        elif isinstance(quality, str):
            quality = VideoQuality[quality.upper()]
        
        with self.process_lock:
            if camera_id not in self.recording_processes:
                if camera_id not in self.cameras:
                    logger.error(f"Camera {camera_id} not found in configuration")
                    return False
                
                rtsp_url = self.cameras[camera_id]["rtsp_url"]
                
                if encoding_preset not in self.encoding_capabilities:
                    logger.warning(f"Encoding preset {encoding_preset.value} not available, falling back to copy")
                    encoding_preset = EncodingPreset.COPY
                
                self.stop_flags[camera_id] = False
                
                thread = threading.Thread(
                    target=self.record_rtsp_stream, 
                    args=(rtsp_url, segment_time, Config.OUTPUT_DIR, camera_id),
                    kwargs={
                        'encoding_preset': encoding_preset,
                        'quality': quality,
                        'audio_enabled': audio_enabled,
                        'custom_params': custom_params
                    },
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
                # Set stop flag first to prevent restart loop
                self.stop_flags[camera_id] = True
                process = self.recording_processes[camera_id]
                
                try:
                    logger.info(f"Stopping recording for camera {camera_id}")
                    
                    if platform.system() == "Windows":
                        # On Windows, FFmpeg responds well to 'q' on stdin for graceful shutdown
                        try:
                            if process.stdin:
                                process.stdin.write(b'q')
                                process.stdin.flush()
                                process.stdin.close()
                                logger.info(f"Sent 'q' command to FFmpeg for camera {camera_id}")
                        except Exception as stdin_error:
                            logger.warning(f"Could not write to stdin for camera {camera_id}: {stdin_error}")
                        
                        # Wait a bit for graceful shutdown
                        try:
                            process.wait(timeout=5)
                            logger.info(f"FFmpeg gracefully stopped for camera {camera_id}")
                        except subprocess.TimeoutExpired:
                            logger.warning(f"FFmpeg did not stop gracefully for camera {camera_id}, forcing termination")
                            process.terminate()
                            try:
                                process.wait(timeout=5)
                            except subprocess.TimeoutExpired:
                                logger.error(f"FFmpeg did not terminate for camera {camera_id}, killing process")
                                process.kill()
                                process.wait()
                    else:
                        # On Unix-like systems, use SIGTERM first, then SIGKILL
                        logger.info(f"Sending SIGTERM to process for camera {camera_id}")
                        process.terminate()
                        try:
                            process.wait(timeout=10)
                            logger.info(f"FFmpeg gracefully stopped for camera {camera_id}")
                        except subprocess.TimeoutExpired:
                            logger.warning(f"FFmpeg did not stop gracefully for camera {camera_id}, forcing kill")
                            process.kill()
                            process.wait()
                    
                    # Clean up
                    if camera_id in self.recording_processes:
                        del self.recording_processes[camera_id]
                    logger.info(f"Recording stopped successfully for camera {camera_id}")
                    return True
                    
                except Exception as e:
                    logger.error(f"Error stopping recording for camera {camera_id}: {e}")
                    try:
                        process.kill()
                        process.wait()
                        if camera_id in self.recording_processes:
                            del self.recording_processes[camera_id]
                    except Exception as kill_e:
                        logger.error(f"Failed to kill process for camera {camera_id} after error: {kill_e}")
                    return False
            else:
                logger.warning(f"No recording in progress for camera {camera_id}")
                return False
    
    def capture_frame(self, camera_id):
        """Capture a single frame from a camera's RTSP stream."""
        if camera_id not in self.cameras:
            logger.error(f"Camera {camera_id} not found.")
            return None

        rtsp_url = self.cameras[camera_id]["rtsp_url"]
        if not rtsp_url:
            logger.error(f"RTSP URL for camera {camera_id} is not configured.")
            return None
            
        temp_frame_path = os.path.join(Config.TEMP_DIR, f"{camera_id}_preview.jpg")

        command = [
            "ffmpeg", "-y", "-i", rtsp_url,
            "-vframes", "1", "-q:v", "2", "-f", "image2",
            temp_frame_path
        ]

        try:
            subprocess.run(command, timeout=10, check=True, capture_output=True)
            if os.path.exists(temp_frame_path):
                return temp_frame_path
        except subprocess.TimeoutExpired:
            logger.error(f"Timeout while capturing frame for camera {camera_id}.")
        except subprocess.CalledProcessError as e:
            logger.error(f"FFmpeg error capturing frame for {camera_id}: {e.stderr.decode()}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while capturing frame for {camera_id}: {e}")
            
        return None

    def start_all_recordings(self, segment_time=None, encoding_preset=None, quality=None):
        """Start recording for all cameras"""
        success_count = 0
        for camera_id in self.cameras:
            if self.start_recording(camera_id, segment_time, encoding_preset, quality):
                success_count += 1
        return success_count

    def stop_all_recordings(self):
        """Stop recording for all cameras"""
        with self.process_lock:
            camera_ids = list(self.recording_processes.keys())
        
        # Stop recordings without holding the lock for too long
        stopped_count = 0
        for camera_id in camera_ids:
            if self.stop_recording(camera_id):
                stopped_count += 1
        
        return stopped_count

    def get_recording_status(self):
        """Get status of all recording processes"""
        with self.process_lock:
            return list(self.recording_processes.keys())

    def get_encoding_info(self):
        """Get information about available encoding options"""
        return {
            'gpu_available': self.gpu_available,
            'encoding_capabilities': [c.value for c in self.encoding_capabilities],
            'quality_presets': {
                'low': VideoQuality.LOW.value,
                'medium': VideoQuality.MEDIUM.value,
                'high': VideoQuality.HIGH.value,
                'ultra': VideoQuality.ULTRA.value
            }
        }