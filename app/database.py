"""Database configuration and initialization with SQLite optimizations."""
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import event, text
from sqlalchemy.engine import Engine

db = SQLAlchemy()


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_conn, connection_record):
    """
    Set SQLite PRAGMAs for better concurrency and performance.
    
    This event listener is called whenever a new database connection is created.
    It configures SQLite settings that improve performance and enable WAL mode.
    """
    cursor = dbapi_conn.cursor()
    
    # Enable WAL (Write-Ahead Logging) mode for better concurrency
    # WAL allows multiple readers while a write is in progress
    cursor.execute("PRAGMA journal_mode=WAL")
    
    # Set synchronous mode to NORMAL for better performance while maintaining safety
    # NORMAL is safe for WAL mode and much faster than FULL
    cursor.execute("PRAGMA synchronous=NORMAL")
    
    # Increase cache size (negative value = KB, positive = pages)
    # -64000 = 64MB cache
    cursor.execute("PRAGMA cache_size=-64000")
    
    # Set busy timeout to 5 seconds (5000 milliseconds)
    # This is in addition to SQLAlchemy's timeout setting
    cursor.execute("PRAGMA busy_timeout=5000")
    
    # Enable foreign key constraints
    cursor.execute("PRAGMA foreign_keys=ON")
    
    # Set temp_store to memory for faster temporary operations
    cursor.execute("PRAGMA temp_store=MEMORY")
    
    # Set mmap_size for memory-mapped I/O (improves read performance)
    # 256MB mmap size
    cursor.execute("PRAGMA mmap_size=268435456")
    
    cursor.close()


def init_db(app):
    """
    Initialize the database with the Flask app.
    
    This function should be called during app creation to set up the database.
    It ensures the instance directory exists and creates all tables.
    """
    # Ensure instance directory exists
    import os
    db_uri = app.config.get('SQLALCHEMY_DATABASE_URI', '')
    
    if db_uri.startswith('sqlite:///'):
        # Extract the database path
        db_path = db_uri.replace('sqlite:///', '')
        
        # Convert to absolute path if relative
        if not os.path.isabs(db_path):
            db_path = os.path.join(os.getcwd(), db_path)
        
        # Get the directory and create it if it doesn't exist
        instance_dir = os.path.dirname(db_path)
        if instance_dir:  # Only create if there's actually a directory path
            os.makedirs(instance_dir, exist_ok=True)
            app.logger.info(f"Database directory ensured: {instance_dir}")
    
    with app.app_context():
        db.create_all()
        
        # Log WAL mode status
        result = db.session.execute(text("PRAGMA journal_mode")).fetchone()
        app.logger.info(f"SQLite journal mode: {result[0] if result else 'unknown'}")
        
        result = db.session.execute(text("PRAGMA synchronous")).fetchone()
        app.logger.info(f"SQLite synchronous mode: {result[0] if result else 'unknown'}")

def get_db_info(app):
    """
    Get information about the current database configuration.
    
    Returns a dictionary with database settings for debugging/monitoring.
    """
    info = {}
    with app.app_context():
        
        pragmas = [
            "journal_mode",
            "synchronous", 
            "cache_size",
            "busy_timeout",
            "foreign_keys",
            "temp_store",
            "mmap_size",
            "page_size",
            "wal_autocheckpoint"
        ]
        
        for pragma in pragmas:
            try:
                result = db.session.execute(text(f"PRAGMA {pragma}")).fetchone()
                info[pragma] = result[0] if result else None
            except Exception as e:
                info[pragma] = f"Error: {str(e)}"
    
    return info