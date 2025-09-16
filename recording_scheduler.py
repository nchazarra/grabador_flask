import time
import threading
from datetime import datetime, date
from suntime import Sun, SunTimeException
from zoneinfo import ZoneInfo # ¡Importante! Necesitas Python 3.9+

# Import only the logger, not the recorder instance
from logs import get_logger

logger = get_logger(__name__)

class RecordingScheduler:
    def __init__(self, recorder_instance, cameras):
        """
        Initializes the scheduler.
        Args:
            recorder_instance: The instantiated Recorder object from app.py.
            cameras: The dictionary of cameras to manage.
        """
        self.recorder = recorder_instance
        self.cameras = cameras
        self.schedule_thread = None
        self.stop_event = threading.Event()

    def _schedule_checker(self):
        """Periodically check if recording should be started or stopped."""
        # --- INICIO DE LA MODIFICACIÓN ---

        # 1. Define tu zona horaria local explícitamente.
        local_tz = ZoneInfo("Europe/Madrid")

        while not self.stop_event.is_set():
            # 2. Obtén la hora actual CON zona horaria.
            now_local = datetime.now(local_tz)
            today = date.today()

            for camera_id, details in self.cameras.items():
                if not details.get("auto_recording"):
                    continue

                try:
                    lat = float(details["latitude"])
                    lon = float(details["longitude"])
                    
                    sun = Sun(lat, lon)
                    
                    # 3. Obtén las horas de amanecer/atardecer en UTC.
                    sunrise_utc = sun.get_sunrise_time(today)
                    sunset_utc = sun.get_sunset_time(today)

                    # 4. Conviértelas a tu zona horaria local.
                    sunrise_local = sunrise_utc.astimezone(local_tz)
                    sunset_local = sunset_utc.astimezone(local_tz)

                    is_recording = camera_id in self.recorder.get_recording_status()

                    # 5. Compara las horas (ahora todas son 'timezone-aware').
                    # Es de noche si la hora actual es posterior al atardecer O anterior al amanecer.
                    if now_local > sunset_local or now_local < sunrise_local:
                        if not is_recording:
                            logger.info(f"Scheduler: Starting recording for {camera_id} (night time).")
                            self.recorder.start_recording(camera_id)
                    # Es de día
                    else:
                        if is_recording:
                            logger.info(f"Scheduler: Stopping recording for {camera_id} (day time).")
                            self.recorder.stop_recording(camera_id)

                except SunTimeException as e:
                    logger.error(f"Could not get sunrise/sunset times for {camera_id}: {e}")
                except (KeyError, TypeError, ValueError):
                    logger.warning(f"Scheduler: Skipping {camera_id} due to missing or invalid lat/lon.")
                except Exception as e:
                    logger.error(f"Error in scheduler for {camera_id}: {e}")
            
            # Espera 15 minutos para la siguiente comprobación
            self.stop_event.wait(900)
        # --- FIN DE LA MODIFICACIÓN ---

    def start(self):
        """Start the background scheduling thread."""
        if self.schedule_thread is None or not self.schedule_thread.is_alive():
            self.schedule_thread = threading.Thread(target=self._schedule_checker, daemon=True)
            self.schedule_thread.start()
            logger.info("Sunrise/sunset recording scheduler started.")

    def stop(self):
        """Stop the background scheduler."""
        self.stop_event.set()
