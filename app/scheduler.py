# app/scheduler.py
"""
Background scheduler to automatically fetch survey data on a schedule.
This runs on the server side, not triggered by clients.
Works with both Flask dev server and Gunicorn.
"""
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime, timedelta
import logging
import sys
import os
from threading import Lock

logger = logging.getLogger(__name__)

# Global state
fetch_lock = Lock()
last_fetch_time = None
scheduler = None


def scheduled_fetch():
    """
    This function runs on a schedule (server-side).
    Imports are inside the function to avoid circular imports.
    MUST run within Flask application context to access database.
    """
    global last_fetch_time
    
    if not fetch_lock.acquire(blocking=False):
        print("⏭️  Scheduled fetch skipped - another fetch in progress", file=sys.stderr)
        return
    
    try:
        # Import app to create context
        from app import create_app
        from app.data_fetcher import DataFetcher
        
        # Create application context for database access
        app = create_app()
        
        with app.app_context():
            print("\n" + "=" * 60, file=sys.stderr)
            print(f"🔄 Starting scheduled survey fetch at {datetime.now()}", file=sys.stderr)
            print("=" * 60, file=sys.stderr)
            
            count1 = DataFetcher.fetch_and_store_survey("survey1")
            count2 = DataFetcher.fetch_and_store_survey("survey2")
            
            last_fetch_time = datetime.now()
            
            print("=" * 60, file=sys.stderr)
            print(f"✅ Scheduled fetch completed successfully!", file=sys.stderr)
            print(f"   Survey 1: {count1} responses", file=sys.stderr)
            print(f"   Survey 2: {count2} responses", file=sys.stderr)
            print(f"   Completed at: {last_fetch_time}", file=sys.stderr)
            print("=" * 60 + "\n", file=sys.stderr)
        
    except Exception as e:
        print("=" * 60, file=sys.stderr)
        print(f"❌ Scheduled fetch failed: {str(e)}", file=sys.stderr)
        print("=" * 60 + "\n", file=sys.stderr)
        logger.error(f"Scheduled fetch failed: {str(e)}", exc_info=True)
    finally:
        fetch_lock.release()


def init_scheduler(app):
    """
    Initialize the background scheduler.
    Works with both Flask dev server and Gunicorn.
    
    Args:
        app: Flask application instance
    """
    global scheduler
    
    # ========================================
    # MULTI-WORKER PROTECTION
    # ========================================
    # Only ONE process should run the scheduler, even with multiple Gunicorn workers
    
    # 1. Skip if explicitly disabled
    if os.environ.get('DISABLE_SCHEDULER') == 'true':
        print("⏸️  Scheduler disabled via DISABLE_SCHEDULER env var", file=sys.stderr)
        return None
    
    # 2. Flask development server with reloader check
    werkzeug_run_main = os.environ.get('WERKZEUG_RUN_MAIN')
    if werkzeug_run_main is not None and werkzeug_run_main != 'true':
        print("⏸️  Skipping scheduler init in parent process (Flask dev reloader)", file=sys.stderr)
        return None
    
    # 3. For Gunicorn/production: Use file-based lock
    # This ensures only the FIRST worker to start gets the scheduler
    lockfile = '/tmp/survey_scheduler.lock'
    
    try:
        # Check if lock already exists
        if os.path.exists(lockfile):
            # Read PID from lockfile
            try:
                with open(lockfile, 'r') as f:
                    lock_pid = int(f.read().strip())
                
                # Check if that process is still running
                try:
                    os.kill(lock_pid, 0)  # Signal 0 just checks if process exists
                    # Process exists - another worker has the scheduler
                    print(f"⏸️  Scheduler already running in process {lock_pid}", file=sys.stderr)
                    return None
                except OSError:
                    # Process doesn't exist - stale lockfile, remove it
                    print(f"🧹 Removing stale lockfile (PID {lock_pid} not found)", file=sys.stderr)
                    os.remove(lockfile)
            except (ValueError, IOError):
                # Corrupt lockfile - remove it
                print("🧹 Removing corrupt lockfile", file=sys.stderr)
                os.remove(lockfile)
        
        # Create lockfile with our PID
        with open(lockfile, 'w') as f:
            f.write(str(os.getpid()))
        
        print(f"🔒 Acquired scheduler lock (PID: {os.getpid()})", file=sys.stderr)
        
        # Clean up lockfile on exit
        import atexit
        def cleanup_lockfile():
            try:
                if os.path.exists(lockfile):
                    with open(lockfile, 'r') as f:
                        lock_pid = int(f.read().strip())
                    # Only remove if it's our PID
                    if lock_pid == os.getpid():
                        os.remove(lockfile)
                        print("🧹 Cleaned up scheduler lockfile", file=sys.stderr)
            except Exception as e:
                print(f"Warning: Could not clean up lockfile: {e}", file=sys.stderr)
        
        atexit.register(cleanup_lockfile)
        
    except Exception as e:
        print(f"⚠️  Could not acquire scheduler lock: {e}", file=sys.stderr)
        return None
    
    # ========================================
    # SCHEDULER INITIALIZATION
    # ========================================
    
    if scheduler is not None:
        print("⚠️  WARNING: Scheduler already initialized", file=sys.stderr)
        return scheduler
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    print("\n" + "=" * 60, file=sys.stderr)
    print("🚀 INITIALIZING BACKGROUND SCHEDULER", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    
    scheduler = BackgroundScheduler(
        job_defaults={'coalesce': False, 'max_instances': 1}
    )
    
    # Schedule to run every hour
    scheduler.add_job(
        func=scheduled_fetch,
        trigger=IntervalTrigger(hours=1),
        id='fetch_surveys',
        name='Fetch survey data every hour',
        replace_existing=True
    )
    
    scheduler.start()
    
    # Get next run time
    job = scheduler.get_job('fetch_surveys')
    next_run = job.next_run_time if job else 'Unknown'
    
    print("✅ SCHEDULER STARTED SUCCESSFULLY", file=sys.stderr)
    print(f"📅 Fetch interval: Every 1 hour", file=sys.stderr)
    print(f"⏰ Next scheduled run: {next_run}", file=sys.stderr)
    print("=" * 60 + "\n", file=sys.stderr)
    
    # Run first fetch immediately on startup (in 10 seconds)
    print("🔄 Scheduling initial fetch to run in 10 seconds...", file=sys.stderr)
    scheduler.add_job(
        func=scheduled_fetch, 
        trigger='date',
        run_date=datetime.now() + timedelta(seconds=10),
        id='initial_fetch'
    )
    
    # Shutdown scheduler when Flask app stops
    import atexit
    atexit.register(lambda: shutdown_scheduler())
    
    return scheduler


def shutdown_scheduler():
    """Gracefully shutdown the scheduler"""
    global scheduler
    if scheduler:
        print("\n🛑 Shutting down scheduler...", file=sys.stderr)
        scheduler.shutdown()
        print("✅ Scheduler stopped.\n", file=sys.stderr)


def get_last_fetch_time():
    """Get the last time data was fetched"""
    return last_fetch_time


def is_fetch_in_progress():
    """Check if a fetch operation is currently running"""
    return fetch_lock.locked()


def get_next_run_time():
    """Get the next scheduled run time"""
    if scheduler is None:
        return None
    
    job = scheduler.get_job('fetch_surveys')
    if job:
        return job.next_run_time
    return None