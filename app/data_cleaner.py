import re
from typing import Dict, Any, Optional, Tuple
from difflib import get_close_matches


class DataCleaner:
    """
    Handle data cleaning and normalization before database insertion
    """
    
    # Lazy load mapping dictionary
    _mapping_dict = None
    
    @classmethod
    def _get_mapping_dict(cls) -> Dict[str, str]:
        """Get ministry mapping dictionary (lazy loaded)"""
        if cls._mapping_dict is None:
            try:
                from app.ministry_mapping import get_all_mappings
                cls._mapping_dict = get_all_mappings()
                print(f"✓ Loaded {len(cls._mapping_dict)} ministry/agency mappings")
            except ImportError:
                print("⚠ Warning: ministry_mapping.py not found. MDA mapping disabled.")
                cls._mapping_dict = {}
        return cls._mapping_dict
    
    @staticmethod
    def normalize_text(text: str) -> Optional[str]:
        """
        Normalize text for comparison:
            - Uppercase
            - Remove extra whitespace and trim
        
        Args:
            text: Text to normalize
            
        Returns:
            Normalized text
        """
        if not text or not isinstance(text, str):
            return None
        
        # 1. Convert to uppercase and trim leading/trailing whitespace
        text = text.upper().strip()
        
        # 2. Clean up internal extra whitespace (e.g., converting "A  B" to "A B")
        # This uses the regex for robustness against any number of spaces, tabs, etc.
        text = re.sub(r'\s+', ' ', text).strip()
        
        # The final .strip() handles cases where step 2 might have left a space 
        # (though typically not necessary after step 1 and step 2).

        return text if text else None
    
    @classmethod
    def map_mda_to_ministry(cls, mda_name: str) -> Tuple[Optional[str], Optional[str]]:
        """
            Map MDA name to parent ministry using exact match then fuzzy matching.
            
            Args:
                mda_name: Raw MDA name from survey
                
            Returns:
                Tuple of (normalized_mda_name, parent_ministry)
        """
        
        if not mda_name:
            return None, None
        
        # Normalize the MDA name
        normalized_mda = cls.normalize_text(mda_name)
        
        if not normalized_mda:
            return None, None
        
        # Get mapping dictionary
        mapping_dict = cls._get_mapping_dict()
       
        if not mapping_dict:
            return normalized_mda, None
        
        # Try exact match first (case-insensitive)
        mapping_dict_upper = {k.upper(): v for k, v in mapping_dict.items()}
        
        if normalized_mda in mapping_dict_upper:
            return normalized_mda, mapping_dict_upper[normalized_mda]
        
        # Try fuzzy matching (find close matches)
        close_matches = get_close_matches(
            normalized_mda, 
            mapping_dict_upper.keys(), 
            n=1, 
            cutoff=0.8)  # 80% similarity threshold)
        
        if close_matches:
            best_match = close_matches[0]
            parent_ministry = mapping_dict_upper[best_match]
            return normalized_mda, parent_ministry
        
        # Try partial matching (check if MDA name contains or is contained in known names)
        for known_mda, ministry in mapping_dict_upper.items():
                # Check if one contains the other
            if normalized_mda in known_mda or known_mda in normalized_mda:
                # Additional check: they should share significant words
                mda_words = set(normalized_mda.split())
                known_words = set(known_mda.split())
                common_words = mda_words & known_words
                
                    # If they share at least 60% of words, consider it a match
                if len(common_words) >= min(len(mda_words), len(known_words)) * 0.6:
                    return normalized_mda, ministry
        
        # No match found
        return normalized_mda, None
    
    # TODO: Delete this function
    @staticmethod
    def extract_ergp_code_and_project_name_old(project_name: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract ERGP code from project name and return both normalized project name and ERGP code.
        
        ERGP codes typically follow patterns like:
            - ERGP12345678
            - ERGP-12345678
            - ERGP 12345678
        
        Args:
            project_name: Raw project name that may contain ERGP code
            
        Returns:
            Tuple of (cleaned_project_name, ergp_code)
        """
        if not project_name or not isinstance(project_name, str):
            return None, None
        
            # Pattern to match ERGP code: ERGP followed by optional separator and 8 digits
        ergp_pattern = r'\b(ERGP[-\s]?\d{8})\b'
        
            # Search for ERGP code
        match = re.search(ergp_pattern, project_name, re.IGNORECASE)
        
        if match:
            ergp_code = match.group(1)
                # Normalize ERGP code: remove spaces/hyphens, uppercase
            ergp_code_normalized = re.sub(r'[-\s]', '', ergp_code).upper()
            
                # Remove ERGP code from project name
            cleaned_name = re.sub(ergp_pattern, '', project_name, flags=re.IGNORECASE)
            
                # Normalize the project name
            cleaned_name = DataCleaner.normalize_text(cleaned_name)
            
            return cleaned_name, ergp_code_normalized
        else:
                # No ERGP code found, just normalize the project name
            cleaned_name = DataCleaner.normalize_text(project_name)
            return cleaned_name, None
    
    @staticmethod
    def extract_ergp_code_and_project_name(project_name: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract ERGP code from project name using splitting and robustly checking
        the last components for a single-part or two-part code, handling variable
        number of digits in the code.
        
        Args:
            project_name: Raw project name that may contain ERGP code
            
        Returns:
            Tuple of (cleaned_project_name, ergp_code)
        """
        if not project_name or not isinstance(project_name, str):
            # Returns (None, None)
            return None, None
        
        # 1. Convert to uppercase and split by dash
        working_name = project_name.upper()
        components = working_name.split('-')
        
        # Remove empty strings resulting from multiple dashes (e.g., A--B)
        components = [c for c in components if c] 
        
        ergp_code_normalized = None
        
        if not components:
            # Returns (None, None) if the name was only dashes or empty
            return DataCleaner.normalize_text(project_name), None

        # --- Check for ERGP code, starting with the two-component split case ---
        
        # Try to extract the code and remove its components from the list
        
        # Case 1: Code is split into ERGP and digits (e.g., ['ERGP', '12345678'])
        if len(components) >= 2:
            last_component = components[-1]
            second_last_component = components[-2]
            
            # Check if last component contains only digits AND second-last is exactly 'ERGP'
            if re.match(r'^\d+$', last_component) and second_last_component == 'ERGP':
                # Code found: Combine the two parts and normalize
                ergp_code_normalized = second_last_component + last_component
                # *** CRITICAL: Remove the two components from the list ***
                components.pop() 
                components.pop()
        
        # Case 2: Code is single component (e.g., ['ERGP12345678'])
        if ergp_code_normalized is None:
            last_component = components[-1]
            # Pattern: Starts with 'ERGP' and is followed by one or more digits
            ergp_check_pattern = r'^ERGP\d+$' 
            
            if re.match(ergp_check_pattern, last_component):
                # Code found: use the single component
                ergp_code_normalized = last_component
                # *** CRITICAL: Remove the single component from the list ***
                components.pop()
        
        # 3. Reconstruct and clean the project name
        
        # Join the *remaining* components back together, separated by spaces
        # The result will be something like "CONSTRUCTION OF ICU HOSPITAL AND EQUPPING IN FIVE SELECTED HOSPITALS IN OGUN STATE"
        cleaned_name = ' '.join(components)
        
        # Apply the final standard normalization (whitespace and casing cleanup)
        # This will convert the string into the final clean version: 
        # "CONSTRUCTION OF ICU HOSPITAL AND EQUPPING IN FIVE SELECTED HOSPITALS IN OGUN STATE"
        cleaned_name = DataCleaner.normalize_text(cleaned_name)
        
        # The ergp_code_normalized is the second element
        return cleaned_name, ergp_code_normalized
    
    # Apply cleaning
    @classmethod
    def clean_processed_data(cls, processed_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply all cleaning rules to processed survey data before database insertion.
        
        Args:
            processed_data: Dictionary of processed survey response data
            
        Returns:
            Cleaned data dictionary
        """
        # Extract ERGP code and clean project name
        if processed_data.get('project_name'):
            cleaned_name, ergp_code = cls.extract_ergp_code_and_project_name(
                processed_data['project_name']
            )
            processed_data['project_name'] = cleaned_name
            processed_data['ergp_code'] = ergp_code
        else:
            processed_data['ergp_code'] = None
        
        # Map MDA to parent ministry and normalize MDA name
        if processed_data.get('mda_name'):
            normalized_mda, parent_ministry = cls.map_mda_to_ministry(
                processed_data['mda_name']
            )
            processed_data['mda_name'] = normalized_mda
            processed_data['parent_ministry'] = parent_ministry
        else:
            processed_data['parent_ministry'] = None
        
        return processed_data
