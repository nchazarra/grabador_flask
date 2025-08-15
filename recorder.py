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
    COPY = "copy"  # No re-encoding, fastest
    H264_CPU = "h264_cpu"  # CPU encoding
    H264_GPU_NVIDIA = "h264_nvenc"  # NVIDIA GPU encoding
    H264_GPU_AMD = "h264_amf"  # AMD GPU encoding
    H264_GPU_INTEL = "h264_qsv"  # Intel QuickSync
    H265_CPU = "h265_cpu"  # H.265/HEVC CPU encoding
    H265_GPU_NVIDIA = "h265_nvenc"  # NVIDIA GPU H.265 encoding
    H265_GPU_AMD = "h265_amf"  # AMD GPU H.265 encoding

class VideoQuality(Enum):
    """Video quality presets"""
    LOW = {"bitrate": "500k", "resolution": "640x480", "fps": 15}
    MEDIUM = {"bitrate": "1500k", "resolution": "1280x720", "fps": 25}
    HIGH = {"bitrate": "3000k", "resolution": "1920x1080", "fps": 30}
    ULTRA = {"bitrate": "6000k", "resolution": "1920x1080", "fps": 30}
    CUSTOM = {}  # User-defined settings

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
        gpu_info = {
            'nvidia': False,
            'amd': False,
            'intel': False,
            'type': None
        }
        
        try:
            # Check for NVIDIA GPU
            nvidia_check = subprocess.run(['nvidia-smi'], capture_output=True, text=True)
            if nvidia_check.returncode == 0:
                gpu_info['nvidia'] = True
                gpu_info['type'] = 'nvidia'
                logger.info("NVIDIA GPU detected for hardware encoding")
                
        except (FileNotFoundError, subprocess.SubprocessError):
            pass
            
        try:
            # Check for AMD GPU (Windows)
            if platform.system() == 'Windows':
                import wmi
                c = wmi.WMI()
                for gpu in c.Win32_VideoController():
                    if 'AMD' in gpu.Name or 'Radeon' in gpu.Name:
                        gpu_info['amd'] = True
                        gpu_info['type'] = 'amd'
                        logger.info("AMD GPU detected for hardware encoding")
                        break
        except:
            pass
            
        # Check for Intel QuickSync
        try:
            cpu_info = subprocess.run(['wmic', 'cpu', 'get', 'name'], 
                                    capture_output=True, text=True)
            if 'Intel' in cpu_info.stdout:
                gpu_info['intel'] = True
                if not gpu_info['type']:
                    gpu_info['type'] = 'intel'
                logger.info("Intel QuickSync detected for hardware encoding")
        except:
            pass
            
        return gpu_info
    
    def _detect_encoding_capabilities(self):
        """Detect which encoding methods are available"""
        capabilities = []
        
        # Always available
        capabilities.append(EncodingPreset.COPY)
        capabilities.append(EncodingPreset.H264_CPU)
        capabilities.append(EncodingPreset.H265_CPU)
        
        # GPU-specific capabilities
        if self.gpu_available['nvidia']:
            capabilities.extend([
                EncodingPreset.H264_GPU_NVIDIA,
                EncodingPreset.H265_GPU_NVIDIA
            ])
        
        if self.gpu_available['amd']:
            capabilities.extend([
                EncodingPreset.H264_GPU_AMD,
                EncodingPreset.H265_GPU_AMD
            ])
            
        if self.gpu_available['intel']:
            capabilities.append(EncodingPreset.H264_GPU_INTEL)
            
        logger.info(f"Available encoding capabilities: {[c.value for c in capabilities]}")
        return capabilities
    
    def _build_encoding_params(self, preset, quality, custom_params=None):
        """Build FFmpeg encoding parameters based on preset and quality"""
        params = []
        
        if preset == EncodingPreset.COPY:
            params.extend(['-c:v', 'copy'])
            
        elif preset == EncodingPreset.H264_CPU:
            params.extend([
                '-c:v', 'libx264',
                '-preset', 'medium',
                '-crf', '23'
            ])
            
        elif preset == EncodingPreset.H264_GPU_NVIDIA:
            params.extend([
                '-c:v', 'h264_nvenc',
                '-preset', 'p4',  # Balance between quality and speed
                '-rc', 'vbr',
                '-cq', '23',
                '-b:v', quality.value['bitrate'] if quality != VideoQuality.CUSTOM else '2000k',
                '-maxrate', quality.value['bitrate'] if quality != VideoQuality.CUSTOM else '2000k',
                '-bufsize', '4M'
            ])
            
        elif preset == EncodingPreset.H264_GPU_AMD:
            params.extend([
                '-c:v', 'h264_amf',
                '-quality', 'balanced',
                '-b:v', quality.value['bitrate'] if quality != VideoQuality.CUSTOM else '2000k',
                '-maxrate', quality.value['bitrate'] if quality != VideoQuality.CUSTOM else '2000k',
                '-bufsize', '4M'
            ])
            
        elif preset == EncodingPreset.H264_GPU_INTEL:
            params.extend([
                '-c:v', 'h264_qsv',
                '-preset', 'medium',
                '-b:v', quality.value['bitrate'] if quality != VideoQuality.CUSTOM else '2000k'
            ])
            
        elif preset == EncodingPreset.H265_CPU:
            params.extend([
                '-c:v', 'libx265',
                '-preset', 'medium',
                '-crf', '28'
            ])
            
        elif preset == EncodingPreset.H265_GPU_NVIDIA:
            params.extend([
                '-c:v', 'hevc_nvenc',
                '-preset', 'p4',
                '-rc', 'vbr',
                '-cq', '28',
                '-b:v', quality.value['bitrate'] if quality != VideoQuality.CUSTOM else '1500k',
                '-maxrate', quality.value['bitrate'] if quality != VideoQuality.CUSTOM else '1500k',
                '-bufsize', '4M'
            ])
            
        elif preset == EncodingPreset.H265_GPU_AMD:
            params.extend([
                '-c:v', 'hevc_amf',
                '-quality', 'balanced',
                '-b:v', quality.value['bitrate'] if quality != VideoQuality.CUSTOM else '1500k',
                '-maxrate', quality.value['bitrate'] if quality != VideoQuality.CUSTOM else '1500k',
                '-bufsize', '4M'
            ])
        
        # Add resolution and framerate if re-encoding
        if preset != EncodingPreset.COPY and quality != VideoQuality.CUSTOM:
            if 'resolution' in quality.value:
                params.extend(['-s', quality.value['resolution']])
            if 'fps' in quality.value:
                params.extend(['-r', str(quality.value['fps'])])
        
        # Add custom parameters if provided
        if custom_params:
            params.extend(custom_params)
            
        return params
    
    def record_rtsp_stream(self, rtsp_url, segment_time, output_dir, camera_id, 
                          encoding_preset=EncodingPreset.COPY, 
                          quality=VideoQuality.HIGH,
                          audio_enabled=False,
                          custom_params=None,
                          retry_delay=5):
        """Record an RTSP stream with encoding options and GPU support"""
        camera_output_dir = os.path.join(output_dir, camera_id)
        os.makedirs(camera_output_dir, exist_ok=True)
        
        logger.info(f"Starting recording for camera {camera_id} with encoding: {encoding_preset.value}")
        
        consecutive_failures = 0
        max_consecutive_failures = 5
        
        while True:
            if self.stop_flags.get(camera_id, False):
                logger.info(f"Stopping recording for camera {camera_id}.")
                break
            
            if consecutive_failures >= max_consecutive_failures:
                current_retry_delay = retry_delay * 2
                logger.warning(f"Multiple failures for camera {camera_id}. Increasing retry delay to {current_retry_delay}s")
            else:
                current_retry_delay = retry_delay
            
            try:
                start_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                log_file_path = os.path.join(camera_output_dir, "ffmpeg_log.txt")
                
                # Build base command
                command = [
                    "ffmpeg",
                    "-rtsp_transport", "tcp",
                    "-i", rtsp_url,
                ]
                
                # Add hardware acceleration for decoding if available
                if self.gpu_available['nvidia'] and encoding_preset != EncodingPreset.COPY:
                    command.insert(1, "-hwaccel")
                    command.insert(2, "cuda")
                    command.insert(3, "-hwaccel_output_format")
                    command.insert(4, "cuda")
                elif self.gpu_available['intel'] and encoding_preset != EncodingPreset.COPY:
                    command.insert(1, "-hwaccel")
                    command.insert(2, "qsv")
                
                # Add encoding parameters
                encoding_params = self._build_encoding_params(encoding_preset, quality, custom_params)
                command.extend(encoding_params)
                
                # Audio settings
                if audio_enabled:
                    command.extend(['-c:a', 'aac', '-b:a', '128k'])
                else:
                    command.extend(['-an'])
                
                # Output format settings
                command.extend([
                    "-f", "segment",
                    "-segment_time", str(segment_time),
                    "-segment_format", "mp4",
                    "-reset_timestamps", "1",
                    "-strftime", "1",
                    "-reconnect", "1",
                    "-reconnect_at_eof", "1",
                    "-reconnect_streamed", "1",
                    "-reconnect_delay_max", "10",
                    "-timeout", "60",
                    "-stimeout", "60000000",
                ])
                
                # Determine file extension based on codec
                if encoding_preset in [EncodingPreset.H265_CPU, EncodingPreset.H265_GPU_NVIDIA, 
                                      EncodingPreset.H265_GPU_AMD]:
                    ext = "hevc"
                else:
                    ext = "mp4"
                
                output_pattern = os.path.join(camera_output_dir, f"{camera_id}_{start_time}_%03d.{ext}")
                command.append(output_pattern)
                
                logger.debug(f"FFmpeg command: {' '.join(command)}")
                
                # Start the FFmpeg process
                with open(log_file_path, "ab") as log_file:
                    process = subprocess.Popen(command, stdout=log_file, stderr=subprocess.STDOUT)
                
                with self.process_lock:
                    self.recording_processes[camera_id] = process
                
                process.wait()
                
                if self.stop_flags.get(camera_id, False):
                    logger.info(f"Recording stopped manually for camera {camera_id}.")
                    break
                else:
                    logger.warning(f"FFmpeg process for camera {camera_id} ended with code {process.returncode}. Restarting...")
                    if process.returncode != 0:
                        consecutive_failures += 1
                    else:
                        consecutive_failures = 0
                    time.sleep(current_retry_delay)
            
            except Exception as e:
                consecutive_failures += 1
                logger.error(f"Error occurred for camera {camera_id}: {e}. Restarting...")
                time.sleep(current_retry_delay)
    
    def start_recording(self, camera_id, segment_time=None, encoding_preset=None, 
                       quality=None, audio_enabled=False, custom_params=None):
        """Start recording for a specific camera with encoding options"""
        if segment_time is None:
            segment_time = self.settings.get("segment_time", Config.DEFAULT_SEGMENT_TIME)
        
        if encoding_preset is None:
            encoding_preset = EncodingPreset.COPY
        elif isinstance(encoding_preset, str):
            # Convert string to enum
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
                
                # Validate encoding preset is available
                if encoding_preset not in self.encoding_capabilities:
                    logger.warning(f"Encoding preset {encoding_preset.value} not available, falling back to copy")
                    encoding_preset = EncodingPreset.COPY
                
                self.stop_flags.pop(camera_id, None)
                
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
                self.stop_flags[camera_id] = True
                process = self.recording_processes[camera_id]
                
                try:
                    process.send_signal(signal.SIGINT)
                    
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
    
    def start_all_recordings(self, segment_time=None, encoding_preset=None, quality=None):
        """Start recording for all cameras"""
        if segment_time is None:
            segment_time = self.settings.get("segment_time", Config.DEFAULT_SEGMENT_TIME)
        
        success_count = 0
        for camera_id in self.cameras:
            if self.start_recording(camera_id, segment_time, encoding_preset, quality):
                success_count += 1
        
        return success_count
    
    def stop_all_recordings(self):
        """Stop recording for all cameras"""
        with self.process_lock:
            camera_ids = list(self.recording_processes.keys())
            
            for camera_id in camera_ids:
                self.stop_recording(camera_id)
            
            self.stop_flags.clear()
            
            return len(camera_ids)
    
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