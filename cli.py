# cli.py
import click
import os
import time
import threading
from flask.cli import with_appcontext
from flask import current_app
from app.data_cleaner import DataCleaner
from app.models import BudgetProject2024
from app.database import db, get_db_info
from sqlalchemy import text

@click.group()
def data():
    """Manages foundational data tasks (e.g., budget ingestion)."""
    pass

@click.group()
def db_manage():
    """Database management commands."""
    pass

@data.command('ingest-budget')
@click.argument('file_path')
def ingest_budget_data(file_path):
    """
    Ingests 2024 Approved Budget data from a specified CSV file path.
    Example: flask data ingest-budget /path/to/2024_Amended.xlsx\ -\ Sheet1.csv
    """
    # Check if the table already has data
    with current_app.app_context():
        if BudgetProject2024.query.first():
            click.echo("❌ Budget data already exists in the database. Skipping ingestion.")
            return

        click.echo(f"Starting one-time budget data ingestion from: {file_path}")
        
        try:
            # Call the ingestion method you placed in data_cleaner.py
            DataCleaner.ingest_and_normalize_budget_data(file_path)
            
            click.echo("\n✅ Budget data successfully ingested and normalized.")
            
        except FileNotFoundError:
            click.echo(f"\n❌ ERROR: File not found at path: {file_path}")
            db.session.rollback()
        except Exception as e:
            click.echo(f"\n❌ FATAL ERROR during ingestion: {e}")
            db.session.rollback()

@db_manage.command('info')
@with_appcontext
def db_info():
    """Display current database configuration and settings."""
    click.echo("=== Database Configuration ===\n")
    
    # Use current_app instead of click context
    info = get_db_info(current_app)
    
    for key, value in info.items():
        click.echo(f"{key:20} : {value}")
    
    click.echo("\n=== Database File Info ===")
    db_uri = current_app.config.get('SQLALCHEMY_DATABASE_URI', '')
    if db_uri.startswith('sqlite:///'):
        db_path = db_uri.replace('sqlite:///', '')
        
        # Convert to absolute path if relative
        if not os.path.isabs(db_path):
            db_path = os.path.join(os.getcwd(), db_path)
            
        if os.path.exists(db_path):
            size_mb = os.path.getsize(db_path) / (1024 * 1024)
            click.echo(f"Database path       : {db_path}")
            click.echo(f"Database size       : {size_mb:.2f} MB")
            
            # Check for WAL and SHM files
            wal_path = db_path + '-wal'
            shm_path = db_path + '-shm'
            
            if os.path.exists(wal_path):
                wal_size_kb = os.path.getsize(wal_path) / 1024
                click.echo(f"WAL file size       : {wal_size_kb:.2f} KB")
            else:
                click.echo("WAL file            : Not found (may not be using WAL mode)")
            
            if os.path.exists(shm_path):
                click.echo(f"SHM file            : Present")
            else:
                click.echo("SHM file            : Not found")
        else:
            click.echo(f"Database file not found at: {db_path}")


@db_manage.command('checkpoint')
@with_appcontext
def checkpoint_wal():
    """
    Manually checkpoint the WAL file.
    
    This forces SQLite to move data from the WAL file back to the main database.
    Useful for reducing WAL file size and ensuring data persistence.
    """
    try:
        result = db.session.execute(text("PRAGMA wal_checkpoint(TRUNCATE)")).fetchone()
        db.session.commit()
        
        click.echo("WAL checkpoint completed successfully")
        if result:
            click.echo(f"Result: busy={result[0]}, log={result[1]}, checkpointed={result[2]}")
    except Exception as e:
        click.echo(f"Error during checkpoint: {str(e)}", err=True)


@db_manage.command('enable-wal')
@with_appcontext
def enable_wal():
    """
    Manually enable WAL mode.
    
    Note: This should happen automatically when the app starts,
    but this command can be used to verify or re-enable it.
    """
    try:
        result = db.session.execute(text("PRAGMA journal_mode=WAL")).fetchone()
        db.session.commit()
        
        if result and result[0] == 'wal':
            click.echo("✓ WAL mode enabled successfully")
        else:
            click.echo(f"Warning: Unexpected result: {result}", err=True)
    except Exception as e:
        click.echo(f"Error enabling WAL: {str(e)}", err=True)


@db_manage.command('optimize')
@with_appcontext
def optimize_db():
    """
    Optimize the database.
    
    Runs VACUUM and ANALYZE to rebuild the database and update statistics.
    WARNING: This can take time on large databases and locks the database.
    """
    click.echo("Starting database optimization...")
    
    try:
        # ANALYZE to update query planner statistics
        click.echo("Running ANALYZE...")
        db.session.execute(text("ANALYZE"))
        db.session.commit()
        click.echo("✓ ANALYZE completed")
        
        # Optional: VACUUM (can be slow on large databases)
        if click.confirm("Run VACUUM? (This may take time and lock the database)", default=False):
            click.echo("Running VACUUM...")
            # VACUUM cannot run in a transaction
            db.session.execute(text("VACUUM"))
            click.echo("✓ VACUUM completed")
        
        click.echo("Database optimization completed successfully")
        
    except Exception as e:
        click.echo(f"Error during optimization: {str(e)}", err=True)


@db_manage.command('test-concurrency')
@with_appcontext
def test_concurrency():
    """
    Test database concurrency by attempting simultaneous reads/writes.
    
    This is a basic test to verify that WAL mode is working correctly.
    """
    
    click.echo("Testing database concurrency with WAL mode...")
    
    results = {'reads': 0, 'writes': 0, 'errors': 0}
    lock = threading.Lock()
    
    # Get the app instance to push context in threads
    app = current_app._get_current_object()
    
    def reader():
        """Perform multiple read operations."""
        # Each thread needs its own app context
        with app.app_context():
            for _ in range(10):
                try:
                    # Simple query
                    db.session.execute(text("SELECT 1")).fetchone()
                    with lock:
                        results['reads'] += 1
                    time.sleep(0.01)
                except Exception as e:
                    with lock:
                        results['errors'] += 1
                        click.echo(f"Read error: {str(e)}", err=True)
    
    def writer():
        """Perform multiple write operations."""
        # Each thread needs its own app context
        with app.app_context():
            for i in range(5):
                try:
                    # Create a temporary table, insert data, and drop it
                    db.session.execute(text(f"CREATE TEMP TABLE IF NOT EXISTS test_table_{i} (id INTEGER)"))
                    db.session.execute(text(f"INSERT INTO test_table_{i} VALUES ({i})"))
                    db.session.commit()
                    with lock:
                        results['writes'] += 1
                    time.sleep(0.02)
                except Exception as e:
                    db.session.rollback()
                    with lock:
                        results['errors'] += 1
                        click.echo(f"Write error: {str(e)}", err=True)
    
    # Start multiple threads
    threads = []
    
    # Start 3 reader threads
    for _ in range(3):
        t = threading.Thread(target=reader)
        threads.append(t)
        t.start()
    
    # Start 1 writer thread
    t = threading.Thread(target=writer)
    threads.append(t)
    t.start()
    
    # Wait for all threads to complete
    for t in threads:
        t.join()
    
    click.echo("\n=== Concurrency Test Results ===")
    click.echo(f"Successful reads  : {results['reads']}")
    click.echo(f"Successful writes : {results['writes']}")
    click.echo(f"Errors            : {results['errors']}")
    
    if results['errors'] == 0:
        click.echo("\n✓ Concurrency test passed! WAL mode is working correctly.")
    else:
        click.echo(f"\n✗ Test completed with {results['errors']} errors. Check configuration.", err=True)


if __name__ == '__main__':
    db_manage()