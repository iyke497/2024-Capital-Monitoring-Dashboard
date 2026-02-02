# app/scheduler.py
"""
Background scheduler to automatically fetch survey data on a schedule.
This runs on the server side, not triggered by clients.
Optimized to prevent memory leaks.
"""
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime, timedelta, timezone
import logging
import sys
import os
import atexit
from threading import Lock
import fcntl  # For proper file locking
import gc  # For explicit garbage collection

logger = logging.getLogger(__name__)

# Keep the file descriptor open for the lifetime of the process so the flock is held.
_scheduler_lock_fd = None
_scheduler_lockfile = None  # set in init_scheduler(app)


# Global state
fetch_lock = Lock()
last_fetch_time = None
scheduler = None
_app_instance = None  # Cache app instance to avoid repeated creation


def scheduled_fetch():
    """
    This function runs on a schedule (server-side).
    Imports are inside the function to avoid circular imports.
    MUST run within Flask application context to access database.
    
    MEMORY LEAK FIXES:
    - Reuses cached app instance instead of creating new ones
    - Explicitly closes database sessions
    - Forces garbage collection after each run
    """
    global last_fetch_time, _app_instance
    
    if not fetch_lock.acquire(blocking=False):
        print("⏭️  Scheduled fetch skipped - another fetch in progress", file=sys.stderr)
        return
    
    try:
        # Use cached app instance to avoid memory leak from repeated create_app() calls
        from app.data_fetcher import DataFetcher
        from app.database import db
        
        if _app_instance is None:
            print("⚠️  Warning: App instance not cached, this shouldn't happen", file=sys.stderr)
            from app import create_app
            _app_instance = create_app()
        
        # Create application context for database access
        with _app_instance.app_context():
            print("\n" + "=" * 60, file=sys.stderr)
            print(f"🔄 Starting scheduled fetch at {datetime.now()}", file=sys.stderr)
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
            
            # CRITICAL: Explicitly remove all database sessions
            # This prevents connection pooling leaks
            db.session.remove()
        
    except Exception as e:
        print("=" * 60, file=sys.stderr)
        print(f"❌ Scheduled fetch failed: {str(e)}", file=sys.stderr)
        print("=" * 60 + "\n", file=sys.stderr)
        logger.error(f"Scheduled fetch failed: {str(e)}", exc_info=True)
        
        # Clean up database session on error too
        try:
            from app.database import db
            db.session.rollback()
            db.session.remove()
        except Exception:
            pass
            
    finally:
        fetch_lock.release()
        
        # MEMORY LEAK FIX: Force garbage collection after each scheduled run
        # This ensures that any circular references are cleaned up
        gc.collect()


def init_scheduler(app):
    """
    Initialize the background scheduler.
    Simple and reliable approach.
    
    Args:
        app: Flask application instance
        
    MEMORY LEAK FIXES:
    - Caches app instance to avoid creating multiple Flask apps
    - Configures APScheduler with memory-efficient settings
    - Uses proper thread pool limits
    """
    global scheduler, _app_instance
    
    # Cache the app instance to avoid memory leaks from repeated create_app() calls
    _app_instance = app
    
    # Skip if scheduler is already running
    if scheduler is not None:
        print("⚠️  Scheduler already initialized", file=sys.stderr)
        return scheduler
    
    # Skip if explicitly disabled
    if os.environ.get('DISABLE_SCHEDULER') == 'true':
        print("⏸️  Scheduler disabled via DISABLE_SCHEDULER env var", file=sys.stderr)
        return None
    
    # SIMPLE FILE LOCK - This actually works
    global _scheduler_lock_fd, _scheduler_lockfile
    if _scheduler_lockfile is None:
        _scheduler_lockfile = os.path.join(app.instance_path, 'scheduler.lock')
    lockfile = _scheduler_lockfile
    
    try:
        # Try to get an exclusive lock on the lockfile
        lock_fd = os.open(lockfile, os.O_CREAT | os.O_WRONLY)
        try:
            # Try to get an exclusive lock (non-blocking)
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            
            # Write our PID to the file without closing the FD (closing releases the flock)
            os.ftruncate(lock_fd, 0)
            os.write(lock_fd, str(os.getpid()).encode('utf-8'))
            os.fsync(lock_fd)
            
            global _scheduler_lock_fd
            _scheduler_lock_fd = lock_fd
            print(f"🔒 Scheduler lock acquired (PID: {os.getpid()})", file=sys.stderr)
            
            # Clean up on exit
            def cleanup():
                global _scheduler_lock_fd
                try:
                    if _scheduler_lock_fd is not None:
                        try:
                            fcntl.flock(_scheduler_lock_fd, fcntl.LOCK_UN)
                        finally:
                            os.close(_scheduler_lock_fd)
                            _scheduler_lock_fd = None
                    # Best-effort cleanup of lockfile
                    try:
                        os.remove(lockfile)
                    except FileNotFoundError:
                        pass
                    print("🧹 Scheduler lock cleaned up", file=sys.stderr)
                except Exception:
                    pass
            
            atexit.register(cleanup)
            
        except BlockingIOError:
            # Lock is held by another process
            os.close(lock_fd)
            print("⏸️  Scheduler already running in another process", file=sys.stderr)
            return None
            
    except Exception as e:
        print(f"⚠️  Could not acquire scheduler lock: {e}", file=sys.stderr)
        return None
    
    # ========================================
    # INITIALIZE SCHEDULER
    # ========================================
    
    print("\n" + "=" * 60, file=sys.stderr)
    print("🚀 INITIALIZING BACKGROUND SCHEDULER", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    
    # Configure APScheduler logging to reduce memory from excessive logging
    logging.getLogger('apscheduler').setLevel(logging.WARNING)
    logging.getLogger('apscheduler.executors.default').setLevel(logging.WARNING)
    
    interval_hours = int(app.config.get('SCHEDULER_INTERVAL_HOURS', 1)) if hasattr(app, 'config') else 1

    # MEMORY LEAK FIX: Configure thread pool with limits
    # Default ThreadPoolExecutor can grow unbounded, causing memory leaks
    scheduler = BackgroundScheduler(
        job_defaults={
            'coalesce': True,  # Combine multiple missed runs
            'max_instances': 1,  # Only one instance of each job at a time
            'misfire_grace_time': 300  # 5 minutes grace for missed jobs
        },
        executors={
            'default': {
                'type': 'threadpool',
                'max_workers': 3  # Limit thread pool size to prevent unbounded growth
            }
        },
        timezone='UTC'
    )
    
    # Schedule to run every hour
    scheduler.add_job(
        func=scheduled_fetch,
        trigger=IntervalTrigger(hours=interval_hours),
        id='fetch_surveys',
        name='Fetch survey data periodically',
        replace_existing=True
    )
    
    scheduler.start()
    
    # Get next run time
    job = scheduler.get_job('fetch_surveys')
    next_run = job.next_run_time if job else 'Unknown'
    
    print("✅ SCHEDULER STARTED SUCCESSFULLY", file=sys.stderr)
    print(f"📅 Fetch interval: Every {interval_hours} hour(s)", file=sys.stderr)
    print(f"⏰ Next scheduled run: {next_run}", file=sys.stderr)
    print(f"🧵 Thread pool max workers: 3", file=sys.stderr)
    print("=" * 60 + "\n", file=sys.stderr)
    
    # Run first fetch immediately (but delayed to let app start)
    print("🔄 Scheduling initial fetch to run in 5 seconds...", file=sys.stderr)
    scheduler.add_job(
        func=scheduled_fetch, 
        trigger='date',
        run_date=datetime.now(timezone.utc) + timedelta(seconds=5),
        id='initial_fetch'
    )
    
    # Register shutdown
    atexit.register(shutdown_scheduler)
    
    return scheduler


def shutdown_scheduler():
    """Gracefully shutdown the scheduler and clean up resources"""
    global scheduler, _app_instance
    
    if scheduler:
        print("\n🛑 Shutting down scheduler...", file=sys.stderr)
        
        try:
            # Wait for running jobs to finish (with timeout)
            scheduler.shutdown(wait=True)
            print("✅ Scheduler stopped gracefully.\n", file=sys.stderr)
        except Exception as e:
            print(f"⚠️  Error during scheduler shutdown: {e}", file=sys.stderr)
            scheduler.shutdown(wait=False)
        
        scheduler = None
    
    # Clean up cached app instance
    if _app_instance is not None:
        try:
            # Close any remaining database connections
            from app.database import db
            db.session.remove()
            db.engine.dispose()
        except Exception:
            pass
        
        _app_instance = None
    
    # Force final garbage collection
    gc.collect()


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


def get_scheduler_stats():
    """
    Get scheduler statistics for monitoring.
    Useful for detecting memory issues.
    """
    if scheduler is None:
        return {
            'running': False,
            'jobs': 0,
            'fetch_in_progress': False
        }
    
    jobs = scheduler.get_jobs()
    return {
        'running': scheduler.running,
        'jobs': len(jobs),
        'fetch_in_progress': is_fetch_in_progress(),
        'last_fetch': last_fetch_time,
        'next_run': get_next_run_time()
    }