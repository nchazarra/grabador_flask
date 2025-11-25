import subprocess
import os
import threading
import datetime
import time
import logging
import platform
from threading import Lock, Event
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, List, Any

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


@dataclass
class RecordingStats:
    """Statistics for a recording session"""
    camera_id: str
    started_at: datetime.datetime
    encoding_preset: str
    quality: str
    segments_created: int = 0
    total_bytes_written: int = 0
    errors_count: int = 0
    last_error: Optional[str] = None
    last_segment_time: Optional[datetime.datetime] = None
    restarts_count: int = 0


class Recorder:
    def __init__(self):
        self.recording_processes: Dict[str, subprocess.Popen] = {}
        self.recording_threads: Dict[str, threading.Thread] = {}
        self.recording_stats: Dict[str, RecordingStats] = {}
        
        self.process_lock = Lock()
        self.stop_flags: Dict[str, Event] = {}
        
        self.cameras = Config.load_cameras()
        self.settings = Config.load_settings()
        
        self.gpu_available = self._detect_gpu()
        self.encoding_capabilities = self._detect_encoding_capabilities()
        
        # Verify FFmpeg is available
        self._verify_ffmpeg()

    def _verify_ffmpeg(self):
        """Verify FFmpeg is installed and accessible"""
        try:
            result = subprocess.run(
                [Config.FFMPEG_PATH, '-version'],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                version_line = result.stdout.split('\n')[0]
                logger.info(f"FFmpeg found: {version_line}")
            else:
                logger.error("FFmpeg returned non-zero exit code")
        except FileNotFoundError:
            logger.error(
                f"FFmpeg not found at '{Config.FFMPEG_PATH}'. "
                "Please install FFmpeg."
            )
        except subprocess.TimeoutExpired:
            logger.error("FFmpeg version check timed out")
        except Exception as e:
            logger.error(f"Error verifying FFmpeg: {e}")

    def check_rtsp_stream(self, camera_id: str, timeout: int = 15) -> Dict[str, Any]:
        """
        Check if RTSP stream is accessible and get stream info.
        Returns dict with 'success', 'error', and 'stream_info' keys.
        """
        if camera_id not in self.cameras:
            return {'success': False, 'error': 'Camera not found', 'stream_info': None}
        
        rtsp_url = self.cameras[camera_id].get("rtsp_url", "")
        if not rtsp_url:
            return {'success': False, 'error': 'No RTSP URL configured', 'stream_info': None}
        
        # Use ffprobe to check stream
        command = [
            'ffprobe',
            '-v', 'quiet',
            '-rtsp_transport', 'tcp',
            '-timeout', str(timeout * 1000000),
            '-print_format', 'json',
            '-show_streams',
            '-show_format',
            rtsp_url
        ]
        
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=timeout + 5
            )
            
            if result.returncode == 0:
                import json
                stream_info = json.loads(result.stdout)
                return {
                    'success': True,
                    'error': None,
                    'stream_info': stream_info
                }
            else:
                return {
                    'success': False,
                    'error': f'FFprobe failed with code {result.returncode}: {result.stderr[:200]}',
                    'stream_info': None
                }
        
        except subprocess.TimeoutExpired:
            return {
                'success': False,
                'error': f'Connection timeout after {timeout}s',
                'stream_info': None
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'stream_info': None
            }

    def _detect_gpu(self):
        """Detect available GPU hardware for encoding"""
        gpu_info = {'nvidia': False, 'amd': False, 'intel': False, 'type': None}
        
        # Check NVIDIA
        try:
            nvidia_check = subprocess.run(
                ['nvidia-smi', '--query-gpu=name', '--format=csv,noheader'],
                capture_output=True,
                text=True,
                timeout=10
            )
            if nvidia_check.returncode == 0:
                gpu_name = nvidia_check.stdout.strip()
                gpu_info['nvidia'] = True
                gpu_info['type'] = 'nvidia'
                gpu_info['nvidia_name'] = gpu_name
                logger.info(f"NVIDIA GPU detected: {gpu_name}")
        except (FileNotFoundError, subprocess.SubprocessError, subprocess.TimeoutExpired):
            pass

        # Check Intel (vainfo for Linux)
        if platform.system() == "Linux":
            try:
                vainfo_check = subprocess.run(
                    ['vainfo'],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if vainfo_check.returncode == 0 and 'Intel' in vainfo_check.stdout:
                    gpu_info['intel'] = True
                    if not gpu_info['type']:
                        gpu_info['type'] = 'intel'
                    logger.info("Intel QuickSync detected")
            except (FileNotFoundError, subprocess.SubprocessError, subprocess.TimeoutExpired):
                pass

        return gpu_info

    def _detect_encoding_capabilities(self):
        """Detect which encoding methods are available"""
        capabilities = [EncodingPreset.COPY, EncodingPreset.H264_CPU, EncodingPreset.H265_CPU]
        
        if self.gpu_available['nvidia']:
            if self._test_encoder('h264_nvenc'):
                capabilities.append(EncodingPreset.H264_GPU_NVIDIA)
            if self._test_encoder('hevc_nvenc'):
                capabilities.append(EncodingPreset.H265_GPU_NVIDIA)
        
        if self.gpu_available['intel']:
            if self._test_encoder('h264_qsv'):
                capabilities.append(EncodingPreset.H264_GPU_INTEL)
        
        if self.gpu_available['amd']:
            if self._test_encoder('h264_amf'):
                capabilities.append(EncodingPreset.H264_GPU_AMD)
            if self._test_encoder('hevc_amf'):
                capabilities.append(EncodingPreset.H265_GPU_AMD)
        
        logger.info(f"Available encoding capabilities: {[c.value for c in capabilities]}")
        return capabilities

    def _test_encoder(self, encoder_name: str) -> bool:
        """Test if a specific encoder is available"""
        try:
            result = subprocess.run(
                [Config.FFMPEG_PATH, '-hide_banner', '-encoders'],
                capture_output=True,
                text=True,
                timeout=10
            )
            return encoder_name in result.stdout
        except Exception:
            return False

    def _build_encoding_params(self, preset: EncodingPreset, quality: VideoQuality, 
                                custom_params: Optional[List[str]] = None) -> List[str]:
        """Build FFmpeg encoding parameters"""
        params = []
        
        if preset == EncodingPreset.COPY:
            params.extend(['-c:v', 'copy'])
        elif preset == EncodingPreset.H264_CPU:
            params.extend(['-c:v', 'libx264', '-preset', 'medium', '-crf', '23'])
        elif preset == EncodingPreset.H264_GPU_NVIDIA:
            params.extend([
                '-c:v', 'h264_nvenc', 
                '-preset', 'p4', 
                '-rc', 'vbr', 
                '-cq', '23', 
                '-b:v', quality.value.get('bitrate', '2000k')
            ])
        elif preset == EncodingPreset.H265_CPU:
            params.extend(['-c:v', 'libx265', '-preset', 'medium', '-crf', '28'])
        elif preset == EncodingPreset.H265_GPU_NVIDIA:
            params.extend([
                '-c:v', 'hevc_nvenc', 
                '-preset', 'p4', 
                '-rc', 'vbr', 
                '-cq', '28', 
                '-b:v', quality.value.get('bitrate', '1500k')
            ])
        elif preset == EncodingPreset.H264_GPU_INTEL:
            params.extend(['-c:v', 'h264_qsv', '-preset', 'medium'])
        elif preset == EncodingPreset.H264_GPU_AMD:
            params.extend(['-c:v', 'h264_amf', '-quality', 'balanced'])
        elif preset == EncodingPreset.H265_GPU_AMD:
            params.extend(['-c:v', 'hevc_amf', '-quality', 'balanced'])
        
        # Apply quality settings for non-copy presets
        if preset != EncodingPreset.COPY and quality != VideoQuality.CUSTOM:
            if 'resolution' in quality.value:
                params.extend(['-s', quality.value['resolution']])
            if 'fps' in quality.value:
                params.extend(['-r', str(quality.value['fps'])])
        
        if custom_params:
            params.extend(custom_params)
        
        return params

    def record_rtsp_stream(self, rtsp_url: str, segment_time: int, output_dir: str, 
                          camera_id: str, encoding_preset: EncodingPreset = EncodingPreset.COPY,
                          quality: VideoQuality = VideoQuality.HIGH,
                          audio_enabled: bool = False,
                          custom_params: Optional[List[str]] = None):
        """Main recording loop for a camera"""
        
        camera_output_dir = os.path.join(output_dir, camera_id)
        os.makedirs(camera_output_dir, exist_ok=True)
        
        logger.info(f"Starting recording for camera {camera_id} with encoding: {encoding_preset.value}")
        
        # Initialize stats
        stats = RecordingStats(
            camera_id=camera_id,
            started_at=datetime.datetime.now(),
            encoding_preset=encoding_preset.value,
            quality=quality.name
        )
        self.recording_stats[camera_id] = stats
        
        stop_event = self.stop_flags.get(camera_id, Event())
        consecutive_failures = 0
        max_consecutive_failures = Config.FFMPEG_MAX_FAILURES
        base_retry_delay = Config.FFMPEG_RECONNECT_DELAY
        
        while not stop_event.is_set():
            # Calculate retry delay with exponential backoff
            if consecutive_failures >= max_consecutive_failures:
                retry_delay = min(base_retry_delay * (2 ** min(consecutive_failures, 6)), 300)
                logger.warning(
                    f"Multiple failures ({consecutive_failures}) for camera {camera_id}. "
                    f"Retry delay: {retry_delay}s"
                )
            else:
                retry_delay = base_retry_delay
            
            try:
                start_time = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
                log_file_path = os.path.join(camera_output_dir, "ffmpeg_log.txt")
                
                # Build FFmpeg command with improved RTSP handling
                command = [
                    Config.FFMPEG_PATH,
                    # Input options (must come before -i)
                    '-rtsp_transport', 'tcp',
                    '-rtsp_flags', 'prefer_tcp',
                    # Increase buffer and analysis time for problematic streams
                    '-analyzeduration', '10000000',  # 10 seconds
                    '-probesize', '10000000',  # 10MB
                    '-fflags', '+genpts+discardcorrupt',
                    '-err_detect', 'ignore_err',
                    # Timeout settings
                    '-timeout', str(Config.FFMPEG_TIMEOUT * 1000000),
                    '-stimeout', str(Config.FFMPEG_TIMEOUT * 1000000),
                    # Input
                    '-i', rtsp_url
                ]
                
                # Add encoding parameters
                encoding_params = self._build_encoding_params(encoding_preset, quality, custom_params)
                command.extend(encoding_params)
                
                # Audio settings
                if audio_enabled:
                    command.extend(['-c:a', 'aac', '-b:a', '128k'])
                else:
                    command.extend(['-an'])
                
                # Segmentation and reconnection settings
                command.extend([
                    '-f', 'segment',
                    '-segment_time', str(segment_time),
                    '-segment_format', 'mp4',
                    '-segment_atclocktime', '1',
                    '-reset_timestamps', '1',
                    '-strftime', '0',
                    # Reconnection options
                    '-reconnect', '1',
                    '-reconnect_at_eof', '1',
                    '-reconnect_streamed', '1',
                    '-reconnect_delay_max', '30',
                    # Handle stream errors gracefully
                    '-max_muxing_queue_size', '1024',
                    '-avoid_negative_ts', 'make_zero',
                ])
                
                # Output filename pattern
                output_pattern = os.path.join(
                    camera_output_dir, 
                    f"{camera_id}_{start_time}_%03d.mp4"
                )
                command.append(output_pattern)
                
                logger.debug(f"FFmpeg command: {' '.join(command)}")
                
                # Start FFmpeg process
                with open(log_file_path, "ab") as log_file:
                    log_file.write(f"\n--- Recording started at {start_time} ---\n".encode())
                    
                    process = subprocess.Popen(
                        command,
                        stdin=subprocess.PIPE if platform.system() == "Windows" else None,
                        stdout=log_file,
                        stderr=subprocess.STDOUT,
                    )
                    
                    with self.process_lock:
                        self.recording_processes[camera_id] = process
                
                # Monitor process
                while not stop_event.is_set() and process.poll() is None:
                    stop_event.wait(timeout=1)
                    self._update_recording_stats(camera_id, camera_output_dir)
                
                # Check why we exited the loop
                if stop_event.is_set():
                    logger.info(f"Stop requested for camera {camera_id}")
                    self._graceful_stop_ffmpeg(process, camera_id)
                    break
                
                # Process ended unexpectedly
                return_code = process.returncode
                
                # Decode common FFmpeg error codes
                error_messages = {
                    1: "Generic error (check ffmpeg_log.txt for details)",
                    8: "Invalid data / connection issue with RTSP stream",
                    69: "Service unavailable",
                    188: "Invalid input",
                    234: "More data needed",
                }
                error_detail = error_messages.get(return_code, "Unknown error")
                
                logger.warning(
                    f"FFmpeg process for camera {camera_id} ended "
                    f"with code {return_code} ({error_detail}). Restarting..."
                )
                
                stats.restarts_count += 1
                consecutive_failures = consecutive_failures + 1 if return_code != 0 else 0
                
                if not stop_event.is_set():
                    stop_event.wait(timeout=retry_delay)
                
            except Exception as e:
                consecutive_failures += 1
                stats.errors_count += 1
                stats.last_error = str(e)
                logger.error(f"Error for camera {camera_id}: {e}. Restarting...")
                
                if not stop_event.is_set():
                    stop_event.wait(timeout=retry_delay)
        
        # Cleanup
        with self.process_lock:
            if camera_id in self.recording_processes:
                del self.recording_processes[camera_id]
        
        logger.info(f"Recording thread for camera {camera_id} has exited")

    def _update_recording_stats(self, camera_id: str, output_dir: str):
        """Update recording statistics"""
        if camera_id not in self.recording_stats:
            return
        
        stats = self.recording_stats[camera_id]
        
        try:
            segments = [f for f in os.listdir(output_dir) 
                       if f.startswith(camera_id) and f.endswith('.mp4')]
            
            total_size = sum(
                os.path.getsize(os.path.join(output_dir, f)) 
                for f in segments
            )
            
            stats.segments_created = len(segments)
            stats.total_bytes_written = total_size
            stats.last_segment_time = datetime.datetime.now()
            
        except Exception as e:
            logger.debug(f"Error updating stats for {camera_id}: {e}")

    def _graceful_stop_ffmpeg(self, process: subprocess.Popen, camera_id: str):
        """Gracefully stop an FFmpeg process"""
        try:
            if platform.system() == "Windows":
                if process.stdin:
                    try:
                        process.stdin.write(b'q')
                        process.stdin.flush()
                        process.stdin.close()
                    except Exception:
                        pass
                
                try:
                    process.wait(timeout=5)
                    logger.info(f"FFmpeg gracefully stopped for camera {camera_id}")
                    return
                except subprocess.TimeoutExpired:
                    pass
            else:
                process.terminate()
                try:
                    process.wait(timeout=10)
                    logger.info(f"FFmpeg gracefully stopped for camera {camera_id}")
                    return
                except subprocess.TimeoutExpired:
                    pass
            
            logger.warning(f"Force killing FFmpeg for camera {camera_id}")
            process.kill()
            process.wait(timeout=5)
            
        except Exception as e:
            logger.error(f"Error stopping FFmpeg for camera {camera_id}: {e}")

    def start_recording(self, camera_id: str, segment_time: Optional[int] = None,
                       encoding_preset=None, quality=None,
                       audio_enabled: bool = False,
                       custom_params: Optional[List[str]] = None,
                       verify_stream: bool = False) -> bool:
        """Start recording for a specific camera"""
        
        if segment_time is None:
            segment_time = self.settings.get("segment_time", Config.DEFAULT_SEGMENT_TIME)
        
        if encoding_preset is None:
            preset_str = self.settings.get("default_encoding", "copy")
            encoding_preset = EncodingPreset(preset_str)
        elif isinstance(encoding_preset, str):
            encoding_preset = EncodingPreset(encoding_preset)
        
        if quality is None:
            quality_str = self.settings.get("default_quality", "HIGH")
            quality = VideoQuality[quality_str.upper()]
        elif isinstance(quality, str):
            quality = VideoQuality[quality.upper()]
        
        with self.process_lock:
            if camera_id in self.recording_processes:
                logger.warning(f"Recording already in progress for camera {camera_id}")
                return False
            
            if camera_id not in self.cameras:
                logger.error(f"Camera {camera_id} not found in configuration")
                return False
            
            rtsp_url = self.cameras[camera_id].get("rtsp_url", "")
            if not rtsp_url:
                logger.error(f"Camera {camera_id} has no RTSP URL configured")
                return False
        
        # Optional stream verification (outside lock to avoid blocking)
        if verify_stream:
            logger.info(f"Verifying RTSP stream for camera {camera_id}...")
            check_result = self.check_rtsp_stream(camera_id)
            if not check_result['success']:
                logger.error(f"Stream verification failed for {camera_id}: {check_result['error']}")
                return False
            logger.info(f"Stream verified for camera {camera_id}")
        
        with self.process_lock:
            # Re-check in case something changed while verifying
            if camera_id in self.recording_processes:
                logger.warning(f"Recording already started for camera {camera_id}")
                return False
            
            if encoding_preset not in self.encoding_capabilities:
                logger.warning(
                    f"Encoding preset {encoding_preset.value} not available, "
                    "falling back to copy"
                )
                encoding_preset = EncodingPreset.COPY
            
            # Create stop event
            stop_event = Event()
            self.stop_flags[camera_id] = stop_event
            
            # Start recording thread
            thread = threading.Thread(
                target=self.record_rtsp_stream,
                args=(rtsp_url, segment_time, str(Config.OUTPUT_DIR), camera_id),
                kwargs={
                    'encoding_preset': encoding_preset,
                    'quality': quality,
                    'audio_enabled': audio_enabled,
                    'custom_params': custom_params
                },
                daemon=True,
                name=f"Recorder-{camera_id}"
            )
            self.recording_threads[camera_id] = thread
            thread.start()
            
            logger.info(
                f"Started recording for camera {camera_id} "
                f"(encoding={encoding_preset.value}, quality={quality.name})"
            )
            return True

    def stop_recording(self, camera_id: str) -> bool:
        """Stop recording for a specific camera"""
        
        if camera_id in self.stop_flags:
            self.stop_flags[camera_id].set()
        
        with self.process_lock:
            if camera_id not in self.recording_processes:
                logger.warning(f"No recording in progress for camera {camera_id}")
                return False
            
            process = self.recording_processes[camera_id]
        
        self._graceful_stop_ffmpeg(process, camera_id)
        
        if camera_id in self.recording_threads:
            thread = self.recording_threads[camera_id]
            thread.join(timeout=10)
            del self.recording_threads[camera_id]
        
        with self.process_lock:
            if camera_id in self.recording_processes:
                del self.recording_processes[camera_id]
        
        if camera_id in self.stop_flags:
            del self.stop_flags[camera_id]
        
        logger.info(f"Recording stopped for camera {camera_id}")
        return True

    def capture_frame(self, camera_id: str) -> Optional[str]:
        """Capture a single frame from a camera's RTSP stream"""
        if camera_id not in self.cameras:
            logger.error(f"Camera {camera_id} not found")
            return None
        
        rtsp_url = self.cameras[camera_id].get("rtsp_url", "")
        if not rtsp_url:
            logger.error(f"RTSP URL for camera {camera_id} is not configured")
            return None
        
        temp_frame_path = os.path.join(str(Config.TEMP_DIR), f"{camera_id}_preview.jpg")
        
        command = [
            Config.FFMPEG_PATH, '-y',
            '-rtsp_transport', 'tcp',
            '-i', rtsp_url,
            '-vframes', '1',
            '-q:v', '2',
            '-f', 'image2',
            temp_frame_path
        ]
        
        try:
            subprocess.run(command, timeout=15, check=True, capture_output=True)
            if os.path.exists(temp_frame_path):
                return temp_frame_path
        except subprocess.TimeoutExpired:
            logger.error(f"Timeout capturing frame for camera {camera_id}")
        except subprocess.CalledProcessError as e:
            logger.error(f"FFmpeg error capturing frame for {camera_id}: {e.stderr.decode()[:200]}")
        except Exception as e:
            logger.error(f"Unexpected error capturing frame for {camera_id}: {e}")
        
        return None

    def start_all_recordings(self, segment_time: Optional[int] = None,
                            encoding_preset: Optional[str] = None,
                            quality: Optional[str] = None) -> int:
        """Start recording for all cameras"""
        success_count = 0
        for camera_id in self.cameras:
            if self.start_recording(camera_id, segment_time, encoding_preset, quality):
                success_count += 1
        return success_count

    def stop_all_recordings(self) -> int:
        """Stop recording for all cameras"""
        with self.process_lock:
            camera_ids = list(self.recording_processes.keys())
        
        stopped_count = 0
        for camera_id in camera_ids:
            if self.stop_recording(camera_id):
                stopped_count += 1
        
        return stopped_count

    def get_recording_status(self) -> List[str]:
        """Get list of cameras currently recording"""
        with self.process_lock:
            return list(self.recording_processes.keys())

    def get_recording_stats(self, camera_id: Optional[str] = None) -> Dict[str, Any]:
        """Get recording statistics"""
        if camera_id:
            stats = self.recording_stats.get(camera_id)
            if stats:
                return {
                    'camera_id': stats.camera_id,
                    'started_at': stats.started_at.isoformat(),
                    'encoding_preset': stats.encoding_preset,
                    'quality': stats.quality,
                    'segments_created': stats.segments_created,
                    'total_bytes_written': stats.total_bytes_written,
                    'errors_count': stats.errors_count,
                    'last_error': stats.last_error,
                    'restarts_count': stats.restarts_count,
                    'uptime_seconds': (datetime.datetime.now() - stats.started_at).total_seconds()
                }
            return {}
        
        return {
            cam_id: self.get_recording_stats(cam_id) 
            for cam_id in self.recording_stats
        }

    def get_encoding_info(self) -> Dict[str, Any]:
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

    def reload_cameras(self):
        """Reload camera configuration from file"""
        new_cameras = Config.load_cameras()
        
        for camera_id in list(self.cameras.keys()):
            if camera_id not in new_cameras:
                if camera_id in self.recording_processes:
                    logger.info(f"Camera {camera_id} removed, stopping recording")
                    self.stop_recording(camera_id)
        
        self.cameras = new_cameras
        logger.info(f"Reloaded camera configuration: {len(self.cameras)} cameras")
