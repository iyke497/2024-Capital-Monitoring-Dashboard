import os
from dotenv import load_dotenv
from pathlib import Path

# Get the base directory
BASE_DIR = Path(__file__).resolve().parent.parent

for _env_file in (BASE_DIR / '.env', BASE_DIR / '.flaskenv'):
    if _env_file.exists():
        load_dotenv(dotenv_path=_env_file, override=False)

class Config:
    # Critical for scheduler behavior
    SECRET_KEY = os.getenv('SECRET_KEY')
    
    # IMPORTANT: Control debug mode via environment variable
    DEBUG = os.getenv('FLASK_DEBUG', '0').lower() in ['1', 'true', 'yes']

    # Database - Use absolute path
    SQLALCHEMY_DATABASE_URI = os.getenv(
        'DATABASE_URL', 
        f'sqlite:///{BASE_DIR / "instance" / "survey_data.db"}'
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # WAL and Conncurrent Access Settings
    SQLALCHEMY_ENGINE_OPTIONS = {
        'connect_args': {
            'timeout': 60,  # Increase timeout for busy database
            'check_same_thread': False,  # Allow multi-threaded access
        },
        'pool_pre_ping': True,  # Verify connections before using them
        'pool_recycle': 3600,  # Recycle connections after 1 hour
        'echo': False,  # Set to True for SQL debugging
    }
    
    # API Configuration for Survey 1
    SURVEY1_BASE_URL = os.getenv('SURVEY1_BASE_URL')
    SURVEY1_ENDPOINT = os.getenv('SURVEY1_ENDPOINT')
    SURVEY1_TOKEN = os.getenv('SURVEY1_TOKEN')
    SURVEY1_ORGANIZATION_ID = os.getenv('SURVEY1_ORGANIZATION_ID')
    
    # API Configuration for Survey 2
    SURVEY2_BASE_URL = os.getenv('SURVEY2_BASE_URL')
    SURVEY2_ENDPOINT = os.getenv('SURVEY2_ENDPOINT')
    SURVEY2_TOKEN = os.getenv('SURVEY2_TOKEN')
    SURVEY2_ORGANIZATION_ID = os.getenv('SURVEY2_ORGANIZATION_ID')
    
    # Request configuration
    REQUEST_TIMEOUT = 120
    PAGE_SIZE = 100  # Number of records per API call

    # ====== Scheduler Settings ======
    SCHEDULER_ENABLED = os.getenv('SCHEDULER_ENABLED', 'true').lower() in ['1', 'true', 'yes']
    SCHEDULER_INTERVAL_HOURS = int(os.getenv('SCHEDULER_INTERVAL_HOURS', '1'))
    
    # ====== Development/Production Flags ======
    # These help with environment detection
    ENV = os.getenv('FLASK_ENV', 'production')