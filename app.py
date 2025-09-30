from flask import Flask, render_template, request, redirect, url_for, send_from_directory, jsonify, send_file, flash, abort
import os
import time
from flask_httpauth import HTTPBasicAuth
from datetime import datetime
import logging
from recording_scheduler import RecordingScheduler
# First, import config
from config import Config
import psutil

# Initialize logging system with a specific file path
from logs import LogManager, get_logger, DEFAULT_LOG_FILE

# Make sure log file path exists
log_file_path = os.path.join(Config.BASE_DIR, "app.log")
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

# Set up the log manager with the configured log file
log_manager = LogManager.get_instance(log_file_path)

# Now import the enhanced recorder and storage modules
from recorder import Recorder, EncodingPreset, VideoQuality
from storage import StorageManager

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = Config.SECRET_KEY

# Set up basic authentication (optional, can be enabled in settings)
auth = HTTPBasicAuth()
USERS = {"admin": "password"}  # Change this in production!

@auth.verify_password
def verify_password(username, password):
    if username in USERS and USERS[username] == password:
        return username
    return None

# Get logger for this module
logger = get_logger(__name__)

# Initialize recorder and storage manager
recorder = Recorder()
storage_manager = StorageManager()

@app.route('/')
def index():
    """Main dashboard page with encoding options"""
    cameras = recorder.cameras
    recording_cameras = recorder.get_recording_status()
    storage_info = storage_manager.get_storage_usage()
    settings = Config.load_settings()
    encoding_info = recorder.get_encoding_info()
    
    return render_template(
        'index.html', 
        cameras=cameras, 
        recording_cameras=recording_cameras,
        storage_info=storage_info,
        settings=settings,
        encoding_info=encoding_info
    )

@app.route('/start_recording', methods=['POST'])
def start_recording():
    """Start recording for a specific camera with encoding options"""
    camera_id = request.form['camera_id']
    segment_time = int(request.form.get('segment_time', Config.DEFAULT_SEGMENT_TIME))
    
    # Get encoding settings
    encoding_preset = request.form.get('encoding_preset', 'copy')
    quality = request.form.get('quality', 'HIGH')
    audio_enabled = request.form.get('audio_enabled', 'off') == 'on'
    
    # Parse custom encoding parameters if provided
    custom_params = None
    custom_params_str = request.form.get('custom_params', '').strip()
    if custom_params_str:
        custom_params = custom_params_str.split()
    
    if recorder.start_recording(
        camera_id, 
        segment_time, 
        encoding_preset=encoding_preset,
        quality=quality,
        audio_enabled=audio_enabled,
        custom_params=custom_params
    ):
        flash(f"Started recording for camera {camera_id} with {encoding_preset} encoding", "success")
    else:
        flash(f"Failed to start recording for camera {camera_id}", "error")
    
    return redirect(url_for('index'))

@app.route('/stop_recording', methods=['POST'])
def stop_recording():
    """Stop recording for a specific camera"""
    camera_id = request.form['camera_id']
    
    if recorder.stop_recording(camera_id):
        flash(f"Stopped recording for camera {camera_id}", "success")
    else:
        flash(f"Failed to stop recording for camera {camera_id}", "error")
    
    return redirect(url_for('index'))

@app.route('/recordings')
def list_recordings():
    """List all recordings by camera"""
    recordings = storage_manager.get_recordings_list()
    cameras = recorder.cameras
    return render_template('recordings.html', recordings=recordings, cameras=cameras)

@app.route('/recordings/<path:filename>')
def download_recording(filename):
    """Download a specific recording file"""
    return send_from_directory(Config.OUTPUT_DIR, filename)

@app.route('/start_all_recordings', methods=['POST'])
def start_all_recordings():
    """Start recording for all cameras with encoding options"""
    segment_time = int(request.form.get('segment_time', Config.DEFAULT_SEGMENT_TIME))
    encoding_preset = request.form.get('encoding_preset', 'copy')
    quality = request.form.get('quality', 'HIGH')
    
    count = recorder.start_all_recordings(segment_time, encoding_preset, quality)
    
    if count > 0:
        flash(f"Started recording for {count} cameras with {encoding_preset} encoding", "success")
    else:
        flash("No new recordings started", "info")
    
    return redirect(url_for('index'))

@app.route('/stop_all_recordings', methods=['POST'])
def stop_all_recordings():
    """Stop recording for all cameras"""
    count = recorder.stop_all_recordings()
    
    if count > 0:
        flash(f"Stopped recording for {count} cameras", "success")
    else:
        flash("No recordings were active", "info")
    
    return redirect(url_for('index'))

@app.route('/clear_old_recordings', methods=['POST'])
def clear_old_recordings():
    """Clear old recordings based on retention settings"""
    days = request.form.get('days', None)
    if days:
        days = int(days)
    
    result = storage_manager.clear_old_recordings(days)
    
    if result['removed_count'] > 0:
        flash(f"Removed {result['removed_count']} files, freed {result['size_freed_formatted']}", "success")
    else:
        flash("No files were removed", "info")
    
    return redirect(url_for('index'))

@app.route('/clear_all_recordings', methods=['POST'])
def clear_all_recordings():
    """Clear all recordings"""
    result = storage_manager.clear_all_recordings()
    
    if result['removed_count'] > 0:
        flash(f"Removed all {result['removed_count']} recordings, freed {result['size_freed_formatted']}", "success")
    else:
        flash("No files were removed", "info")
    
    return redirect(url_for('index'))

@app.route('/get_storage_usage')
def get_storage_usage():
    """Get current storage usage"""
    return jsonify(storage_manager.get_storage_usage())

@app.route('/get_encoding_info')
def get_encoding_info():
    """Get available encoding options"""
    return jsonify(recorder.get_encoding_info())

@app.route('/download_all_recordings')
def download_all_recordings():
    """Download all recordings as a ZIP archive"""
    result = storage_manager.create_zip_archive()
    
    if result['success']:
        return send_file(
            result['filepath'], 
            as_attachment=True, 
            download_name=result['filename']
        )
    else:
        flash(f"Failed to create ZIP archive: {result.get('error', 'Unknown error')}", "error")
        return redirect(url_for('recordings'))

@app.route('/download_selected_recordings', methods=['POST'])
def download_selected_recordings():
    """Download selected recordings as a ZIP archive"""
    selected_files = request.form.getlist('selected_files')
    
    if not selected_files:
        flash("No files selected", "error")
        return redirect(url_for('recordings'))
    
    result = storage_manager.create_zip_archive(selected_files)
    
    if result['success']:
        return send_file(
            result['filepath'], 
            as_attachment=True, 
            download_name=result['filename']
        )
    else:
        flash(f"Failed to create ZIP archive: {result.get('error', 'Unknown error')}", "error")
        return redirect(url_for('recordings'))

@app.route('/settings', methods=['GET', 'POST'])
def manage_settings():
    """Manage application settings including encoding defaults"""
    if request.method == 'POST':
        # Update settings
        settings = {
            "segment_time": int(request.form.get('segment_time', Config.DEFAULT_SEGMENT_TIME)),
            "retention_days": int(request.form.get('retention_days', Config.DEFAULT_RETENTION_DAYS)),
            "max_storage_gb": int(request.form.get('max_storage_gb', Config.MAX_STORAGE_GB)),
            "auto_cleanup": request.form.get('auto_cleanup', 'off') == 'on',
            "default_encoding": request.form.get('default_encoding', 'copy'),
            "default_quality": request.form.get('default_quality', 'HIGH'),
            "default_audio": request.form.get('default_audio', 'off') == 'on'
        }
        
        # Save settings
        if Config.save_settings(settings):
            # Restart background cleanup if needed
            if settings.get("auto_cleanup", True):
                storage_manager.start_background_cleanup()
            else:
                storage_manager.stop_background_cleanup()
                
            flash("Settings updated successfully", "success")
        else:
            flash("Failed to save settings", "error")
            
        return redirect(url_for('manage_settings'))
    
    # GET request - show settings form
    settings = Config.load_settings()
    encoding_info = recorder.get_encoding_info()
    return render_template('settings.html', settings=settings, encoding_info=encoding_info)

@app.route('/camera_stats/<camera_id>')
def camera_stats(camera_id):
    """Get statistics for a specific camera"""
    if camera_id not in recorder.cameras:
        abort(404)
    
    # Get recordings for this camera
    recordings = storage_manager.get_recordings_list()
    camera_recordings = recordings['by_camera'].get(camera_id, [])
    
    # Calculate stats
    total_size = sum(file['size'] for file in camera_recordings)
    total_duration = len(camera_recordings) * Config.load_settings().get('segment_time', Config.DEFAULT_SEGMENT_TIME)
    
    return jsonify({
        'camera_id': camera_id,
        'camera_name': recorder.cameras[camera_id]['name'],
        'recording_count': len(camera_recordings),
        'total_size': total_size,
        'total_size_formatted': storage_manager.format_size(total_size),
        'estimated_duration': total_duration,
        'estimated_duration_formatted': f"{total_duration // 3600}h {(total_duration % 3600) // 60}m {total_duration % 60}s",
        'is_recording': camera_id in recorder.get_recording_status(),
        'camera_rtsp_url': recorder.cameras[camera_id].get('rtsp_url', 'N/A')
    })

@app.route('/logs')
def view_logs():
    """View application logs"""
    # Parameters for filtering
    log_level = request.args.get('level', None)
    module = request.args.get('module', None)
    lines_param = request.args.get('lines', '100')
    
    try:
        lines = int(lines_param) if lines_param and isinstance(lines_param, str) and lines_param.isdigit() else 100
    except (ValueError, AttributeError):
        lines = 100
    
    # Get date filters if provided
    start_date_str = request.args.get('start_date', None)
    end_date_str = request.args.get('end_date', None)
    
    start_date = None
    end_date = None
    
    if start_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        except ValueError:
            pass
    
    if end_date_str:
        try:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        except ValueError:
            pass
    
    try:
        # Get logs with filtering
        log_content = log_manager.get_logs(
            n=lines, 
            log_level=log_level, 
            module=module,
            start_date=start_date,
            end_date=end_date
        )
        
        # Get log statistics
        log_stats = log_manager.get_log_stats()
        
        return render_template('logs.html', logs=log_content, stats=log_stats)
    except Exception as e:
        flash(f"Failed to read logs: {e}", "error")
        return redirect(url_for('index'))

@app.route('/logs/clear', methods=['POST'])
def clear_logs():
    """Clear all logs"""
    if log_manager.clear_logs():
        flash("Logs cleared successfully", "success")
    else:
        flash("Failed to clear logs", "error")
    
    return redirect(url_for('view_logs'))

@app.route('/api/start_recording/<camera_id>', methods=['POST'])
def api_start_recording(camera_id):
    """API endpoint to start recording for a camera with encoding options"""
    if camera_id not in recorder.cameras:
        return jsonify({"success": False, "error": "Camera not found"}), 404
    
    data = request.json or {}
    segment_time = data.get('segment_time', Config.DEFAULT_SEGMENT_TIME)
    encoding_preset = data.get('encoding_preset', 'copy')
    quality = data.get('quality', 'HIGH')
    audio_enabled = data.get('audio_enabled', False)
    custom_params = data.get('custom_params', None)
    
    if recorder.start_recording(camera_id, segment_time, encoding_preset, quality, audio_enabled, custom_params):
        return jsonify({"success": True, "message": f"Started recording for camera {camera_id}"})
    else:
        return jsonify({"success": False, "error": f"Failed to start recording for camera {camera_id}"}), 400

@app.route('/api/stop_recording/<camera_id>', methods=['POST'])
def api_stop_recording(camera_id):
    """API endpoint to stop recording for a camera"""
    if camera_id not in recorder.cameras:
        return jsonify({"success": False, "error": "Camera not found"}), 404
    
    if recorder.stop_recording(camera_id):
        return jsonify({"success": True, "message": f"Stopped recording for camera {camera_id}"})
    else:
        return jsonify({"success": False, "error": f"Failed to stop recording or camera {camera_id} is not recording"}), 400

@app.route('/api/storage_info')
def api_storage_info():
    """API endpoint to get storage information"""
    return jsonify(storage_manager.get_storage_usage())

@app.route('/api/encoding_info')
def api_encoding_info():
    """API endpoint to get encoding capabilities"""
    return jsonify(recorder.get_encoding_info())

@app.route('/api/recordings')
def api_recordings():
    """API endpoint to get list of recordings"""
    limit = request.args.get('limit', None)
    camera_id = request.args.get('camera_id', None)
    
    recordings = storage_manager.get_recordings_list()
    
    if camera_id:
        # Filter by camera
        if camera_id in recordings['by_camera']:
            result = {
                'by_camera': {camera_id: recordings['by_camera'][camera_id]},
                'total_count': len(recordings['by_camera'][camera_id])
            }
        else:
            result = {'by_camera': {}, 'total_count': 0}
    else:
        result = recordings
    
    # Apply limit if provided
    if limit and limit.isdigit():
        limit = int(limit)
        for camera_id in result['by_camera']:
            result['by_camera'][camera_id] = result['by_camera'][camera_id][:limit]
    
    return jsonify(result)

@app.route('/camera_preview/<camera_id>')
def camera_preview(camera_id):
    """Generate and serve a preview frame for a camera."""
    frame_path = recorder.capture_frame(camera_id)
    if frame_path:
        return send_file(frame_path, mimetype='image/jpeg')
    else:
        # Return a placeholder image if frame capture fails
        return send_from_directory('static', 'placeholder.png')

@app.route('/api/system_stats')
def api_system_stats():
    """API endpoint to get system stats like CPU load."""
    return jsonify({
        'cpu_percent': psutil.cpu_percent(interval=0.1)
    })

# Error handlers
@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

@app.errorhandler(500)
def server_error(e):
    return render_template('500.html'), 500

# Initialize the application
def init_app():
    """Initialize the application"""
    # Create necessary directories
    os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
    os.makedirs(Config.TEMP_DIR, exist_ok=True)
    
    # Start background cleanup if enabled
    settings = Config.load_settings()
    if settings.get('auto_cleanup', True):
        storage_manager.start_background_cleanup()
    
    # Initialize and start the sunrise/sunset scheduler
    # We pass the 'recorder' instance to the scheduler here
    camera_scheduler = RecordingScheduler(recorder, recorder.cameras)
    camera_scheduler.start()
    
    # Log application start and GPU capabilities
    logger.info("Application started")
    encoding_info = recorder.get_encoding_info()
    if encoding_info['gpu_available']['nvidia']:
        logger.info("NVIDIA GPU acceleration available")
    if encoding_info['gpu_available']['amd']:
        logger.info("AMD GPU acceleration available")
    if encoding_info['gpu_available']['intel']:
        logger.info("Intel QuickSync acceleration available")

if __name__ == "__main__":
    init_app()
    app.run(
        host=Config.HOST,
        port=Config.PORT,
        debug=Config.DEBUG
    )
