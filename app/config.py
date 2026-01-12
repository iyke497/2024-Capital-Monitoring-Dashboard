import os
from dotenv import load_dotenv
from pathlib import Path

# Get the base directory
BASE_DIR = Path(__file__).resolve().parent.parent

load_dotenv()

class Config:
    # Database - Use absolute path
    SQLALCHEMY_DATABASE_URI = os.getenv(
        'DATABASE_URL', 
        f'sqlite:///{BASE_DIR / "instance" / "survey_data.db"}'
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
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