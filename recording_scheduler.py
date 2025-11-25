import time
import threading
from datetime import datetime, timedelta
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

    def _get_sun_times_for_date(self, lat: float, lon: float, date: datetime, local_tz) -> dict:
        """Get sunrise and sunset times for a specific date."""
        sun = Sun(lat, lon)
        
        # Create a naive datetime for the date (suntime expects naive datetime)
        date_naive = datetime(date.year, date.month, date.day)
        
        # Get sunrise/sunset in UTC
        sunrise_utc = sun.get_sunrise_time(date_naive)
        sunset_utc = sun.get_sunset_time(date_naive)
        
        # Convert to local timezone
        sunrise_local = sunrise_utc.astimezone(local_tz)
        sunset_local = sunset_utc.astimezone(local_tz)
        
        return {
            'sunrise': sunrise_local,
            'sunset': sunset_local
        }

    def _is_night_time(self, camera_id: str, lat: float, lon: float, local_tz) -> tuple:
        """
        Determine if it's currently night time (between sunset and sunrise).
        
        Night is defined as:
        - After today's sunset until midnight
        - After midnight until today's sunrise
        
        Returns: (is_night: bool, next_change_time: datetime, change_type: str, sunrise, sunset)
        """
        now = datetime.now(local_tz)
        today = now.date()
        
        try:
            # Get today's sun times
            today_sun = self._get_sun_times_for_date(lat, lon, now, local_tz)
            sunrise_today = today_sun['sunrise']
            sunset_today = today_sun['sunset']
            
            # Extract just the time for comparison (ignore date component issues)
            now_time = now.time()
            sunrise_time = sunrise_today.time()
            sunset_time = sunset_today.time()
            
            # Simple logic: it's DAY if we're between sunrise and sunset
            is_day = sunrise_time <= now_time < sunset_time
            is_night = not is_day
            
            # Determine next transition
            if is_night:
                if now_time < sunrise_time:
                    # It's night (early morning), next change is sunrise today
                    next_change = sunrise_today
                    change_type = "sunrise"
                else:
                    # It's night (evening), next change is sunrise tomorrow
                    tomorrow = today + timedelta(days=1)
                    tomorrow_sun = self._get_sun_times_for_date(lat, lon, 
                        datetime.combine(tomorrow, datetime.min.time()), local_tz)
                    next_change = tomorrow_sun['sunrise']
                    change_type = "sunrise"
            else:
                # It's day, next change is sunset today
                next_change = sunset_today
                change_type = "sunset"
            
            logger.debug(
                f"Camera {camera_id}: now={now_time.strftime('%H:%M')}, "
                f"sunrise={sunrise_time.strftime('%H:%M')}, "
                f"sunset={sunset_time.strftime('%H:%M')}, "
                f"is_night={is_night}"
            )
            
            return is_night, next_change, change_type, sunrise_today, sunset_today
            
        except SunTimeException as e:
            logger.error(f"Sun time calculation error for {camera_id}: {e}")
            return False, None, None, None, None
        except Exception as e:
            logger.error(f"Unexpected error calculating sun times for {camera_id}: {e}")
            return False, None, None, None, None

    def _schedule_checker(self):
        """Periodically check if recording should be started or stopped."""
        
        local_tz = ZoneInfo("Europe/Madrid")
        
        # Log initial status
        logger.info("Scheduler started, performing initial check...")
        
        while not self.stop_event.is_set():
            now_local = datetime.now(local_tz)

            for camera_id, details in self.cameras.items():
                # Skip if auto_recording is not enabled
                if not details.get("auto_recording", False):
                    continue

                # Skip if required fields are missing
                lat = details.get("latitude")
                lon = details.get("longitude")
                
                if lat is None or lon is None:
                    logger.warning(
                        f"Scheduler: Skipping {camera_id} - missing latitude or longitude"
                    )
                    continue
                
                # Check if camera was manually stopped recently
                if camera_id in self.manual_stops:
                    time_since_stop = now_local.timestamp() - self.manual_stops[camera_id]
                    if time_since_stop < self.manual_stop_cooldown:
                        logger.debug(
                            f"Scheduler: Skipping {camera_id} - in manual stop cooldown "
                            f"({int(self.manual_stop_cooldown - time_since_stop)}s remaining)"
                        )
                        continue
                    else:
                        # Cooldown expired, remove from manual stops
                        del self.manual_stops[camera_id]
                        logger.info(f"Scheduler: Manual stop cooldown expired for {camera_id}")
                
                try:
                    is_night, next_change, change_type, sunrise, sunset = self._is_night_time(
                        camera_id, float(lat), float(lon), local_tz
                    )
                    
                    is_recording = camera_id in self.recorder.get_recording_status()
                    
                    # Should record during night time
                    should_record = is_night

                    if should_record and not is_recording:
                        sunrise_str = sunrise.strftime('%H:%M') if sunrise else "N/A"
                        sunset_str = sunset.strftime('%H:%M') if sunset else "N/A"
                        logger.info(
                            f"Scheduler: Starting recording for {camera_id} (night time). "
                            f"Current: {now_local.strftime('%H:%M')}, "
                            f"Sunrise: {sunrise_str}, Sunset: {sunset_str}"
                        )
                        self.recorder.start_recording(camera_id)
                        
                    elif not should_record and is_recording:
                        sunrise_str = sunrise.strftime('%H:%M') if sunrise else "N/A"
                        sunset_str = sunset.strftime('%H:%M') if sunset else "N/A"
                        logger.info(
                            f"Scheduler: Stopping recording for {camera_id} (day time). "
                            f"Current: {now_local.strftime('%H:%M')}, "
                            f"Sunrise: {sunrise_str}, Sunset: {sunset_str}"
                        )
                        self.recorder.stop_recording(camera_id)

                except Exception as e:
                    logger.error(
                        f"Unexpected error in scheduler for {camera_id}: "
                        f"{type(e).__name__}: {e}"
                    )
            
            # Check every minute
            self.stop_event.wait(60)

    def mark_manual_stop(self, camera_id):
        """Mark a camera as manually stopped to prevent immediate restart"""
        self.manual_stops[camera_id] = datetime.now(ZoneInfo("Europe/Madrid")).timestamp()
        logger.info(
            f"Scheduler: Camera {camera_id} marked as manually stopped "
            f"(cooldown: {self.manual_stop_cooldown}s)"
        )

    def clear_manual_stop(self, camera_id):
        """Clear manual stop flag for a camera"""
        if camera_id in self.manual_stops:
            del self.manual_stops[camera_id]
            logger.info(f"Scheduler: Manual stop cleared for camera {camera_id}")

    def get_schedule_info(self, camera_id: str) -> dict:
        """Get current schedule information for a camera (useful for debugging/UI)"""
        local_tz = ZoneInfo("Europe/Madrid")
        
        if camera_id not in self.cameras:
            return {"error": "Camera not found"}
            
        details = self.cameras[camera_id]
        lat = details.get("latitude")
        lon = details.get("longitude")
        
        if lat is None or lon is None:
            return {"error": "Missing coordinates"}
        
        try:
            is_night, next_change, change_type, sunrise, sunset = self._is_night_time(
                camera_id, float(lat), float(lon), local_tz
            )
            
            now_local = datetime.now(local_tz)
            
            return {
                "camera_id": camera_id,
                "auto_recording": details.get("auto_recording", False),
                "latitude": lat,
                "longitude": lon,
                "current_time": now_local.strftime('%Y-%m-%d %H:%M:%S'),
                "sunrise_today": sunrise.strftime('%H:%M') if sunrise else None,
                "sunset_today": sunset.strftime('%H:%M') if sunset else None,
                "is_night": is_night,
                "should_record": is_night,
                "next_change": next_change.strftime('%Y-%m-%d %H:%M:%S') if next_change else None,
                "next_change_type": change_type,
                "is_recording": camera_id in self.recorder.get_recording_status(),
                "manual_stop_active": camera_id in self.manual_stops,
            }
        except Exception as e:
            return {"error": str(e)}

    def start(self):
        """Start the background scheduling thread."""
        if self.schedule_thread is None or not self.schedule_thread.is_alive():
            self.stop_event.clear()
            self.schedule_thread = threading.Thread(
                target=self._schedule_checker, 
                daemon=True,
                name="RecordingScheduler"
            )
            self.schedule_thread.start()
            logger.info("Sunrise/sunset recording scheduler started.")

    def stop(self):
        """Stop the background scheduler."""
        self.stop_event.set()
        if self.schedule_thread:
            self.schedule_thread.join(timeout=5)
        logger.info("Sunrise/sunset recording scheduler stopped.")
