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
        # Cache for sun times to avoid recalculating every minute
        self._sun_times_cache = {}
        self._cache_date = None

    def _get_sun_times(self, camera_id, lat, lon, local_tz):
        """
        Get sunrise and sunset times, handling the day boundary correctly.
        Returns (sunrise, sunset) for the current night period.
        """
        now_local = datetime.now(local_tz)
        today = now_local.date()
        
        # Check cache
        if self._cache_date == today and camera_id in self._sun_times_cache:
            return self._sun_times_cache[camera_id]
        
        # Clear cache if date changed
        if self._cache_date != today:
            self._sun_times_cache = {}
            self._cache_date = today
        
        sun = Sun(lat, lon)
        
        # Get today's sunrise and sunset
        today_naive = datetime(today.year, today.month, today.day)
        sunrise_today_utc = sun.get_sunrise_time(today_naive)
        sunset_today_utc = sun.get_sunset_time(today_naive)
        
        sunrise_today = sunrise_today_utc.astimezone(local_tz)
        sunset_today = sunset_today_utc.astimezone(local_tz)
        
        # Get yesterday's sunset (for the case when we're after midnight but before sunrise)
        yesterday = today - timedelta(days=1)
        yesterday_naive = datetime(yesterday.year, yesterday.month, yesterday.day)
        sunset_yesterday_utc = sun.get_sunset_time(yesterday_naive)
        sunset_yesterday = sunset_yesterday_utc.astimezone(local_tz)
        
        # Get tomorrow's sunrise (for the case when we're after today's sunset)
        tomorrow = today + timedelta(days=1)
        tomorrow_naive = datetime(tomorrow.year, tomorrow.month, tomorrow.day)
        sunrise_tomorrow_utc = sun.get_sunrise_time(tomorrow_naive)
        sunrise_tomorrow = sunrise_tomorrow_utc.astimezone(local_tz)
        
        # Determine which night period we're in
        # Night period 1: Yesterday sunset -> Today sunrise (after midnight, before sunrise)
        # Night period 2: Today sunset -> Tomorrow sunrise (after sunset, before midnight)
        
        result = {
            'sunrise_today': sunrise_today,
            'sunset_today': sunset_today,
            'sunset_yesterday': sunset_yesterday,
            'sunrise_tomorrow': sunrise_tomorrow,
        }
        
        self._sun_times_cache[camera_id] = result
        return result

    def _is_night_time(self, camera_id, lat, lon, local_tz):
        """
        Determine if it's currently night time (between sunset and sunrise).
        Handles the day boundary correctly.
        """
        now_local = datetime.now(local_tz)
        
        try:
            sun_times = self._get_sun_times(camera_id, lat, lon, local_tz)
            
            sunrise_today = sun_times['sunrise_today']
            sunset_today = sun_times['sunset_today']
            sunset_yesterday = sun_times['sunset_yesterday']
            sunrise_tomorrow = sun_times['sunrise_tomorrow']
            
            # Case 1: After midnight but before today's sunrise
            # We're in the night that started yesterday
            if now_local < sunrise_today:
                is_night = now_local >= sunset_yesterday.replace(
                    year=now_local.year, 
                    month=now_local.month, 
                    day=now_local.day
                ) - timedelta(days=1) if sunset_yesterday else True
                logger.debug(
                    f"Camera {camera_id}: After midnight, before sunrise. "
                    f"Now: {now_local.strftime('%H:%M')}, "
                    f"Sunrise today: {sunrise_today.strftime('%H:%M')} -> Night: {is_night}"
                )
                return True, sunrise_today, sunset_yesterday
            
            # Case 2: After today's sunset
            # We're in the night that will end tomorrow
            if now_local >= sunset_today:
                logger.debug(
                    f"Camera {camera_id}: After sunset. "
                    f"Now: {now_local.strftime('%H:%M')}, "
                    f"Sunset today: {sunset_today.strftime('%H:%M')}, "
                    f"Sunrise tomorrow: {sunrise_tomorrow.strftime('%H:%M')} -> Night: True"
                )
                return True, sunrise_tomorrow, sunset_today
            
            # Case 3: Daytime (after sunrise, before sunset)
            logger.debug(
                f"Camera {camera_id}: Daytime. "
                f"Now: {now_local.strftime('%H:%M')}, "
                f"Sunrise: {sunrise_today.strftime('%H:%M')}, "
                f"Sunset: {sunset_today.strftime('%H:%M')} -> Night: False"
            )
            return False, sunrise_today, sunset_today
            
        except SunTimeException as e:
            logger.error(f"Sun time calculation error for {camera_id}: {e}")
            # Default to not recording on error
            return False, None, None

    def _schedule_checker(self):
        """Periodically check if recording should be started or stopped."""
        
        local_tz = ZoneInfo("Europe/Madrid")
        
        # Log initial status
        logger.info("Scheduler started, checking cameras...")
        for camera_id, details in self.cameras.items():
            if details.get("auto_recording", False):
                lat = details.get("latitude")
                lon = details.get("longitude")
                if lat and lon:
                    is_night, next_sunrise, last_sunset = self._is_night_time(
                        camera_id, float(lat), float(lon), local_tz
                    )
                    logger.info(
                        f"Camera {camera_id}: auto_recording=True, "
                        f"lat={lat}, lon={lon}, is_night={is_night}"
                    )

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
                    is_night, next_sunrise, last_sunset = self._is_night_time(
                        camera_id, float(lat), float(lon), local_tz
                    )
                    
                    is_recording = camera_id in self.recorder.get_recording_status()

                    if is_night and not is_recording:
                        sunrise_str = next_sunrise.strftime('%H:%M') if next_sunrise else "N/A"
                        sunset_str = last_sunset.strftime('%H:%M') if last_sunset else "N/A"
                        logger.info(
                            f"Scheduler: Starting recording for {camera_id} (night time). "
                            f"Current: {now_local.strftime('%H:%M')}, "
                            f"Sunset: {sunset_str}, Next sunrise: {sunrise_str}"
                        )
                        self.recorder.start_recording(camera_id)
                        
                    elif not is_night and is_recording:
                        sunrise_str = next_sunrise.strftime('%H:%M') if next_sunrise else "N/A"
                        sunset_str = last_sunset.strftime('%H:%M') if last_sunset else "N/A"
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

    def get_schedule_info(self, camera_id):
        """Get current schedule information for a camera (useful for debugging/UI)"""
        local_tz = ZoneInfo("Europe/Madrid")
        
        if camera_id not in self.cameras:
            return None
            
        details = self.cameras[camera_id]
        lat = details.get("latitude")
        lon = details.get("longitude")
        
        if lat is None or lon is None:
            return {"error": "Missing coordinates"}
        
        try:
            is_night, next_sunrise, last_sunset = self._is_night_time(
                camera_id, float(lat), float(lon), local_tz
            )
            
            now_local = datetime.now(local_tz)
            
            return {
                "camera_id": camera_id,
                "auto_recording": details.get("auto_recording", False),
                "latitude": lat,
                "longitude": lon,
                "current_time": now_local.strftime('%Y-%m-%d %H:%M:%S'),
                "is_night": is_night,
                "should_record": is_night,
                "next_sunrise": next_sunrise.strftime('%Y-%m-%d %H:%M:%S') if next_sunrise else None,
                "last_sunset": last_sunset.strftime('%Y-%m-%d %H:%M:%S') if last_sunset else None,
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
