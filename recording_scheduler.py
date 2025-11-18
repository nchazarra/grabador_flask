import time
import threading
from datetime import datetime, date
from suntime import Sun, SunTimeException
from zoneinfo import ZoneInfo

from logs import get_logger

logger = get_logger(__name__)

class RecordingScheduler:
    def __init__(self, recorder_instance, cameras):
        self.recorder = recorder_instance
        self.cameras = cameras
        self.schedule_thread = None
        self.stop_event = threading.Event()
        # Track manual stops to prevent immediate restart
        self.manual_stops = {}
        # Time window to prevent restart after manual stop (in seconds)
        self.manual_stop_cooldown = 300  # 5 minutes

    def _schedule_checker(self):
        """Periodically check if recording should be started or stopped."""
        
        local_tz = ZoneInfo("Europe/Madrid")

        while not self.stop_event.is_set():
            # Get current time in local timezone
            now_local = datetime.now(local_tz)

            for camera_id, details in self.cameras.items():
                # Skip if auto_recording is not enabled
                if not details.get("auto_recording", False):
                    continue

                # Skip if required fields are missing
                if "latitude" not in details or "longitude" not in details:
                    logger.warning(f"Scheduler: Skipping {camera_id} - missing latitude or longitude")
                    continue
                
                # Check if camera was manually stopped recently
                if camera_id in self.manual_stops:
                    time_since_stop = (now_local.timestamp() - self.manual_stops[camera_id])
                    if time_since_stop < self.manual_stop_cooldown:
                        logger.debug(f"Scheduler: Skipping {camera_id} - in manual stop cooldown ({int(self.manual_stop_cooldown - time_since_stop)}s remaining)")
                        continue
                    else:
                        # Cooldown expired, remove from manual stops
                        del self.manual_stops[camera_id]
                
                try:
                    lat = float(details["latitude"])
                    lon = float(details["longitude"])

                    sun = Sun(lat, lon)
                    
                    # Create a naive datetime for today at midnight (no timezone)
                    today_naive = datetime(now_local.year, now_local.month, now_local.day)
                    
                    # Get sunrise/sunset times in UTC
                    sunrise_utc = sun.get_sunrise_time(today_naive)
                    sunset_utc = sun.get_sunset_time(today_naive)
                    
                    # Convert UTC times to local timezone
                    sunrise_local = sunrise_utc.astimezone(local_tz)
                    sunset_local = sunset_utc.astimezone(local_tz)

                    is_recording = camera_id in self.recorder.get_recording_status()

                    # Record during night time (after sunset OR before sunrise)
                    should_record = now_local >= sunset_local or now_local < sunrise_local

                    if should_record and not is_recording:
                        logger.info(f"Scheduler: Starting recording for {camera_id} (night time). Sunrise: {sunrise_local.strftime('%H:%M')}, Sunset: {sunset_local.strftime('%H:%M')}, Current: {now_local.strftime('%H:%M')}")
                        self.recorder.start_recording(camera_id)
                    elif not should_record and is_recording:
                        # During day time (between sunrise and sunset)
                        logger.info(f"Scheduler: Stopping recording for {camera_id} (day time). Sunrise: {sunrise_local.strftime('%H:%M')}, Sunset: {sunset_local.strftime('%H:%M')}, Current: {now_local.strftime('%H:%M')}")
                        self.recorder.stop_recording(camera_id)

                except SunTimeException as e:
                    logger.error(f"Sun time calculation error for {camera_id}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error in scheduler for {camera_id}: {type(e).__name__}: {e}")
            
            # Check every minute
            self.stop_event.wait(60)

    def mark_manual_stop(self, camera_id):
        """Mark a camera as manually stopped to prevent immediate restart"""
        self.manual_stops[camera_id] = datetime.now(ZoneInfo("Europe/Madrid")).timestamp()
        logger.info(f"Scheduler: Camera {camera_id} marked as manually stopped (cooldown: {self.manual_stop_cooldown}s)")

    def clear_manual_stop(self, camera_id):
        """Clear manual stop flag for a camera"""
        if camera_id in self.manual_stops:
            del self.manual_stops[camera_id]
            logger.info(f"Scheduler: Manual stop cleared for camera {camera_id}")

    def start(self):
        """Start the background scheduling thread."""
        if self.schedule_thread is None or not self.schedule_thread.is_alive():
            self.schedule_thread = threading.Thread(target=self._schedule_checker, daemon=True)
            self.schedule_thread.start()
            logger.info("Sunrise/sunset recording scheduler started.")

    def stop(self):
        """Stop the background scheduler."""
        self.stop_event.set()
        if self.schedule_thread:
            self.schedule_thread.join(timeout=5)
        logger.info("Sunrise/sunset recording scheduler stopped.")