# cli.py
import click
from flask import current_app
from app.data_cleaner import DataCleaner
from app.models import BudgetProject2024
from app.database import db

@click.group()
def data():
    """Manages foundational data tasks (e.g., budget ingestion)."""
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