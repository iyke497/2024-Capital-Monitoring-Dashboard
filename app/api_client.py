import requests
import json
from typing import Dict, Any, Optional
from app.config import Config

class APIClient:
    """Client for making API requests to survey endpoints"""
    
    def __init__(self, survey_type: str = 'survey1'):
        """
        Initialize API client for a specific survey type
        
        Args:
            survey_type: 'survey1' or 'survey2'
        """
        self.survey_type = survey_type
        self._setup_config()
        
    def _setup_config(self):
        """Set up configuration based on survey type"""
        if self.survey_type == 'survey1':
            self.base_url = Config.SURVEY1_BASE_URL
            self.endpoint = Config.SURVEY1_ENDPOINT
            self.token = Config.SURVEY1_TOKEN
            self.organization_id = Config.SURVEY1_ORGANIZATION_ID
        elif self.survey_type == 'survey2':
            self.base_url = Config.SURVEY2_BASE_URL
            self.endpoint = Config.SURVEY2_ENDPOINT
            self.token = Config.SURVEY2_TOKEN
            self.organization_id = Config.SURVEY2_ORGANIZATION_ID
        else:
            raise ValueError(f"Invalid survey type: {self.survey_type}")
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with authentication"""
        return {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'organization_id': str(self.organization_id)
        }
    
    def fetch_responses(self, offset: int = 0, limit: int = 100) -> Dict[str, Any]:
        """
        Fetch survey responses from API
        
        Args:
            offset: Starting position for pagination
            limit: Number of records to fetch
            
        Returns:
            API response as dictionary
        """
        url = f"{self.base_url}{self.endpoint}"
        
        params = {
            'limit': limit,
            'offset': offset
        }
        
        try:
            response = requests.get(
                url,
                headers=self._get_headers(),
                params=params,
                timeout=Config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from {self.survey_type}: {e}")
            return {'status': False, 'data': {'results': []}}
    
    def fetch_all_responses(self) -> list:
        """
        Fetch all responses by handling pagination
        
        Returns:
            List of all survey responses
        """
        all_responses = []
        offset = 0
        limit = Config.PAGE_SIZE
        
        while True:
            print(f"Fetching {self.survey_type} responses: offset={offset}, limit={limit}")
            response_data = self.fetch_responses(offset, limit)
            
            if not response_data.get('status'):
                print(f"API returned error: {response_data.get('message', 'Unknown error')}")
                break
            
            data = response_data.get('data', {})
            results = data.get('results', [])
            
            if not results:
                break
            
            all_responses.extend(results)
            
            # Check if there are more pages
            next_url = data.get('next')
            if not next_url:
                break
            
            # Update offset for next iteration
            offset += limit
        
        print(f"Total responses fetched from {self.survey_type}: {len(all_responses)}")
        return all_responses
