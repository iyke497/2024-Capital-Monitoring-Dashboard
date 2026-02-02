# data_cleaner.py - FIXED COLUMN MAPPING
import re
import os
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any, Optional, Tuple, List
from collections import defaultdict
from difflib import SequenceMatcher
from app.models import BudgetProject2024, MinistryAgency
from app.database import db


class DataCleaner:
    """
    Handle data cleaning and normalization using MinistryAgency reference table
    """
    
    @staticmethod
    def normalize_text(text: str) -> Optional[str]:
        """Consistent normalization with MinistryAgency"""
        if not text or not isinstance(text, str):
            return None
        
        # Use the SAME normalization as MinistryAgency
        normalized = MinistryAgency.normalize_name(text)
        
        return normalized if normalized else None
    
    @staticmethod
    def normalize_agency_code(code: Any) -> Optional[str]:
        """
        Normalize agency codes to match GIFMIS database format.
        IMPORTANT: Your GIFMIS database doesn't have leading zeros!
        """
        if code is None or pd.isna(code):
            return None
        
        # Convert to string and strip
        code_str = str(code).strip()
        
        # Remove trailing .0 from floats
        if code_str.endswith('.0'):
            code_str = code_str[:-2]
        
        # Remove any non-digit characters
        code_str = re.sub(r'\D', '', code_str)
        
        # Remove leading zeros (your GIFMIS database doesn't have them)
        code_str = code_str.lstrip('0')
        
        return code_str if code_str else None
    
    @staticmethod
    def extract_ministry_from_agency_code(agency_code: str) -> Optional[str]:
        """
        Extract ministry code from agency code.
        Your agency codes are like: '215001001' where '215' is ministry
        """
        if not agency_code:
            return None
        
        # Ministry code is the first 1-3 digits
        # Find where the ministry part ends (before the agency digits start)
        if len(agency_code) >= 3:
            # Try to get ministry code (first 3 digits max)
            for i in range(min(3, len(agency_code)), 0, -1):
                ministry_part = agency_code[:i]
                # Check if this could be a valid ministry code
                if MinistryAgency.query.filter_by(ministry_code=ministry_part).first():
                    return ministry_part
        
        return None
    
    @staticmethod
    def generate_agency_key(agency_code: str, agency_name: str) -> str:
        """
        Generate a unique key for agency to handle null agency codes.
        """
        normalized_code = DataCleaner.normalize_agency_code(agency_code)
        if normalized_code:
            return f"CODE_{normalized_code}"
        elif agency_name and pd.notna(agency_name):
            normalized = DataCleaner.normalize_text(str(agency_name))
            return f"NAME_{normalized}" if normalized else "UNKNOWN"
        return "UNKNOWN"
    
    @staticmethod
    def aggregate_duplicate_projects(df: pd.DataFrame) -> pd.DataFrame:
        """
        Group duplicate ERGP codes by agency and aggregate appropriations.
        """
        print("🔍 Analyzing duplicates and grouping by agency...")
        
        # Create agency key for grouping
        df['agency_key'] = df.apply(
            lambda row: DataCleaner.generate_agency_key(
                row.get('agency_code'), 
                row.get('agency')
            ), 
            axis=1
        )
        
        # Group by code + agency_key
        grouped = []
        
        for (code, agency_key), group in df.groupby(['code', 'agency_key']):
            if len(group) > 1:
                print(f"   Merging {len(group)} records for ERGP {code} - Agency: {agency_key}")
            
            # Create aggregated record
            aggregated = {
                'code': code,
                'agency_key': agency_key,
                'project_name': group['project_name'].iloc[0] if 'project_name' in group.columns else None,
                'status_type': group['status_type'].iloc[0] if 'status_type' in group.columns else None,
                'appropriation': group['appropriation'].sum(),
                'ministry': group['ministry'].iloc[0] if 'ministry' in group.columns else None,
                'agency': group['agency'].iloc[0] if 'agency' in group.columns else None,
                'agency_code': group['agency_code'].iloc[0] if 'agency_code' in group.columns and pd.notna(group['agency_code'].iloc[0]) else None,
                'ministry_code': group['ministry_code'].iloc[0] if 'ministry_code' in group.columns and pd.notna(group['ministry_code'].iloc[0]) else None,
                'row_count': len(group),
            }
            
            grouped.append(aggregated)
        
        # Convert back to DataFrame
        result_df = pd.DataFrame(grouped)
        
        # Print summary
        original_count = len(df)
        aggregated_count = len(result_df)
        duplicates_removed = original_count - aggregated_count
        
        print(f"\n📊 Duplicate Analysis:")
        print(f"   Original records: {original_count}")
        print(f"   After aggregation: {aggregated_count}")
        print(f"   Duplicates removed: {duplicates_removed}")
        
        return result_df.drop(columns=['agency_key', 'row_count'], errors='ignore')
    
    @staticmethod
    def match_agency_to_gifmis(agency_name: str, agency_code: Optional[str] = None, 
                               ministry_code: Optional[str] = None) -> Tuple[Optional[Dict], str]:
        """
        Match agency to GIFMIS database with improved matching.
        Priority: Code matching > Ministry context > Exact name > Fuzzy
        """
        if not agency_name or pd.isna(agency_name):
            return None, "NO_AGENCY_NAME"
        
        agency_name_str = str(agency_name)
        normalized_name = DataCleaner.normalize_text(agency_name_str)
        
        # ===== 1. PRIORITY: AGENCY CODE MATCHING =====
        if agency_code and pd.notna(agency_code):
            normalized_code = DataCleaner.normalize_agency_code(agency_code)
            
            if normalized_code:
                # Try exact agency code match
                agency = MinistryAgency.query.filter_by(
                    agency_code=normalized_code, 
                    is_active=True
                ).first()
                
                if agency:
                    return {
                        'agency_code': agency.agency_code,
                        'agency_name': agency.agency_name,
                        'ministry_code': agency.ministry_code,
                        'ministry_name': agency.ministry_name,
                        'is_self_accounting': agency.is_self_accounting,
                        'is_parastatal': agency.is_parastatal
                    }, "EXACT_AGENCY_CODE_MATCH"
        
        # ===== 2. MINISTRY CODE + NAME CONTEXT MATCHING =====
        if ministry_code and pd.notna(ministry_code) and normalized_name:
            # Normalize ministry code (remove leading zeros)
            normalized_ministry_code = DataCleaner.normalize_agency_code(ministry_code)
            
            if normalized_ministry_code:
                # Try to find agency within this ministry
                agencies_in_ministry = MinistryAgency.query.filter(
                    MinistryAgency.ministry_code == normalized_ministry_code,
                    MinistryAgency.is_active == True
                ).all()
                
                # First try exact name match within ministry
                for agency in agencies_in_ministry:
                    if agency.agency_name_normalized == normalized_name:
                        return {
                            'agency_code': agency.agency_code,
                            'agency_name': agency.agency_name,
                            'ministry_code': agency.ministry_code,
                            'ministry_name': agency.ministry_name,
                            'is_self_accounting': agency.is_self_accounting,
                            'is_parastatal': agency.is_parastatal
                        }, "EXACT_NAME_WITHIN_MINISTRY"
                
                # Then try fuzzy match within ministry
                best_match = None
                best_score = 0.0
                
                for agency in agencies_in_ministry:
                    score = SequenceMatcher(
                        None, 
                        normalized_name, 
                        agency.agency_name_normalized
                    ).ratio()
                    
                    if score > best_score and score >= 0.85:  # Slightly lower threshold within ministry
                        best_score = score
                        best_match = agency
                
                if best_match:
                    return {
                        'agency_code': best_match.agency_code,
                        'agency_name': best_match.agency_name,
                        'ministry_code': best_match.ministry_code,
                        'ministry_name': best_match.ministry_name,
                        'is_self_accounting': best_match.is_self_accounting,
                        'is_parastatal': best_match.is_parastatal,
                        'similarity_score': best_score
                    }, f"FUZZY_WITHIN_MINISTRY_{int(best_score*100)}%"
        
        # ===== 3. GLOBAL EXACT NAME MATCHING =====
        if normalized_name:
            agency_exact = MinistryAgency.query.filter(
                MinistryAgency.agency_name_normalized == normalized_name,
                MinistryAgency.is_active == True
            ).first()
            
            if agency_exact:
                return {
                    'agency_code': agency_exact.agency_code,
                    'agency_name': agency_exact.agency_name,
                    'ministry_code': agency_exact.ministry_code,
                    'ministry_name': agency_exact.ministry_name,
                    'is_self_accounting': agency_exact.is_self_accounting,
                    'is_parastatal': agency_exact.is_parastatal
                }, "EXACT_NAME_MATCH"
        
        # ===== 4. GLOBAL FUZZY MATCHING =====
        if normalized_name:
            all_agencies = MinistryAgency.query.filter(
                MinistryAgency.is_active == True
            ).all()
            
            best_match = None
            best_score = 0.0
            
            for agency in all_agencies:
                score = SequenceMatcher(
                    None, 
                    normalized_name, 
                    agency.agency_name_normalized
                ).ratio()
                
                if score > best_score:
                    best_score = score
                    best_match = agency
            
            if best_match and best_score >= 0.90:
                return {
                    'agency_code': best_match.agency_code,
                    'agency_name': best_match.agency_name,
                    'ministry_code': best_match.ministry_code,
                    'ministry_name': best_match.ministry_name,
                    'is_self_accounting': best_match.is_self_accounting,
                    'is_parastatal': best_match.is_parastatal,
                    'similarity_score': best_score
                }, f"FUZZY_MATCH_{int(best_score*100)}%"
        
        # ===== 5. NO MATCH =====
        return None, "NO_MATCH"
    
    @classmethod
    def ingest_and_normalize_budget_data(cls, file_path: str):
        """
        Enhanced budget data ingestion with correct column mapping.
        Your Excel columns: ERGP_CODE, PROJECT_NAME, STATUS, APPROPRIATION, 
                           MINISTRY, AGENCY, AGENCY_CODE, MINISTRY_CODE
        """
        print(f"📥 Starting budget data ingestion from {file_path}...")
        print(f"   Expected columns: ERGP_CODE, PROJECT_NAME, STATUS, APPROPRIATION, MINISTRY, AGENCY, AGENCY_CODE, MINISTRY_CODE")
        
        try:
            # 1. Load data - read all as strings to preserve codes
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, dtype=str)
            else:
                df = pd.read_excel(file_path, dtype=str)
            
            print(f"   Loaded {len(df)} rows from file")
            print(f"   Actual columns: {', '.join(df.columns.tolist())}")
            
            # 2. Clean column names - handle various naming conventions
            df.columns = df.columns.str.strip().str.upper()
            
            # CORRECT COLUMN MAPPING FOR YOUR FILE
            column_mapping = {
                'code': ['ERGP_CODE', 'CODE', 'PROJECT_CODE'],
                'project_name': ['PROJECT_NAME', 'PROJECT', 'DESCRIPTION'],
                'status_type': ['STATUS', 'STATUS_TYPE', 'TYPE'],
                'appropriation': ['APPROPRIATION', 'AMOUNT', 'BUDGET', 'ALLOCATED_AMOUNT'],
                'ministry': ['MINISTRY', 'MINISTRY_NAME', 'PARENT_MINISTRY'],
                'agency': ['AGENCY', 'AGENCY_NAME', 'MDA', 'IMPLEMENTING_AGENCY'],
                'agency_code': ['AGENCY_CODE', 'MDA_CODE', 'IMPLEMENTING_AGENCY_CODE'],
                'ministry_code': ['MINISTRY_CODE', 'PARENT_MINISTRY_CODE']
            }
            
            # Find actual column names
            actual_columns = {}
            for target_col, possible_names in column_mapping.items():
                for possible in possible_names:
                    if possible in df.columns:
                        actual_columns[target_col] = possible
                        print(f"   Found '{possible}' -> mapping to '{target_col}'")
                        break
            
            # Check for missing required columns
            required_cols = ['code', 'agency', 'appropriation']
            missing_cols = [col for col in required_cols if col not in actual_columns]
            if missing_cols:
                raise ValueError(f"Required columns missing: {missing_cols}. Available: {df.columns.tolist()}")
            
            # Rename columns to standard names
            df = df.rename(columns={v: k for k, v in actual_columns.items()})
            print(f"   After renaming: {df.columns.tolist()}")
            
            # 3. Basic data cleaning
            initial_count = len(df)
            
            # Drop rows with missing critical data
            df = df.dropna(subset=['code', 'agency', 'appropriation'])
            dropped_count = initial_count - len(df)
            if dropped_count > 0:
                print(f"   Dropped {dropped_count} rows with missing critical data")
            
            # Clean text columns
            text_cols = ['code', 'project_name', 'status_type', 'ministry', 'agency', 'agency_code', 'ministry_code']
            for col in text_cols:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip()
            
            # Normalize codes
            if 'agency_code' in df.columns:
                df['agency_code'] = df['agency_code'].apply(cls.normalize_agency_code)
            
            if 'ministry_code' in df.columns:
                df['ministry_code'] = df['ministry_code'].apply(cls.normalize_agency_code)
            
            # Convert appropriation to numeric
            df['appropriation'] = pd.to_numeric(df['appropriation'], errors='coerce')
            df = df.dropna(subset=['appropriation'])
            
            print(f"   After cleaning: {len(df)} valid rows")
            
            # DEBUG: Show sample data
            print(f"\n🔍 Sample data (first 3 rows):")
            print(df[['code', 'agency', 'agency_code', 'ministry_code']].head(3).to_string())
            
            # Count projects without agency codes
            no_agency_code = df['agency_code'].isna().sum()
            print(f"   Projects without agency codes: {no_agency_code}")
            
            # 4. Handle duplicates by agency
            print("\n🔍 Processing duplicates...")
            aggregated_df = cls.aggregate_duplicate_projects(df)
            
            # 5. Match agencies to GIFMIS with improved logic
            print("\n🔍 Matching agencies to GIFMIS database...")
            
            match_results = []
            match_stats = defaultdict(int)
            unmatched_details = []
            
            for idx, row in aggregated_df.iterrows():
                agency_info, match_type = cls.match_agency_to_gifmis(
                    row.get('agency'),
                    row.get('agency_code'),
                    row.get('ministry_code')
                )
                
                match_stats[match_type] += 1
                
                record = {
                    'code': row['code'],
                    'project_name': row.get('project_name'),
                    'status_type': row.get('status_type'),
                    'appropriation': float(row['appropriation']),
                    'ministry_name': row.get('ministry'),
                    'agency_name': row.get('agency'),
                }
                
                # Add GIFMIS info if matched
                if agency_info:
                    record.update({
                        'ministry_code': agency_info['ministry_code'],
                        'ministry_name': agency_info['ministry_name'],
                        'agency_code': agency_info['agency_code'],
                        'agency_name': agency_info['agency_name'],
                        'agency_normalized': MinistryAgency.normalize_name(agency_info['agency_name']),
                        'match_type': match_type,
                    })
                else:
                    # Use original data for unmatched
                    record.update({
                        'ministry_code': row.get('ministry_code'),
                        'agency_code': row.get('agency_code'),
                        'agency_normalized': MinistryAgency.normalize_name(row.get('agency')),
                        'match_type': match_type,
                    })
                    
                    # Log unmatched details
                    unmatched_details.append({
                        'ergp_code': row['code'],
                        'agency_name': row.get('agency'),
                        'agency_code': row.get('agency_code'),
                        'ministry_name': row.get('ministry'),
                        'ministry_code': row.get('ministry_code'),
                        'match_type': match_type,
                    })
                
                match_results.append(record)
            
            # 6. Print detailed matching statistics
            print(f"\n📊 GIFMIS Matching Results:")
            total_agencies = len(aggregated_df)
            
            # Sort matches by quality
            match_categories = [
                'EXACT_AGENCY_CODE_MATCH',
                'EXACT_NAME_WITHIN_MINISTRY',
                'EXACT_NAME_MATCH',
                'FUZZY_WITHIN_MINISTRY_100%',
                'FUZZY_WITHIN_MINISTRY_99%',
                'FUZZY_WITHIN_MINISTRY_98%',
                'FUZZY_WITHIN_MINISTRY_97%',
                'FUZZY_WITHIN_MINISTRY_96%',
                'FUZZY_WITHIN_MINISTRY_95%',
                'FUZZY_WITHIN_MINISTRY_94%',
                'FUZZY_WITHIN_MINISTRY_93%',
                'FUZZY_WITHIN_MINISTRY_92%',
                'FUZZY_WITHIN_MINISTRY_91%',
                'FUZZY_WITHIN_MINISTRY_90%',
                'FUZZY_WITHIN_MINISTRY_89%',
                'FUZZY_WITHIN_MINISTRY_88%',
                'FUZZY_WITHIN_MINISTRY_87%',
                'FUZZY_WITHIN_MINISTRY_86%',
                'FUZZY_WITHIN_MINISTRY_85%',
                'FUZZY_MATCH_100%',
                'FUZZY_MATCH_99%',
                'FUZZY_MATCH_98%',
                'FUZZY_MATCH_97%',
                'FUZZY_MATCH_96%',
                'FUZZY_MATCH_95%',
                'FUZZY_MATCH_94%',
                'FUZZY_MATCH_93%',
                'FUZZY_MATCH_92%',
                'FUZZY_MATCH_91%',
                'FUZZY_MATCH_90%',
                'NO_MATCH'
            ]
            
            for category in match_categories:
                if category in match_stats:
                    count = match_stats[category]
                    percentage = (count / total_agencies) * 100
                    print(f"   {category}: {count} ({percentage:.1f}%)")
            
            # Calculate summary stats
            exact_matches = sum(match_stats.get(cat, 0) for cat in [
                'EXACT_AGENCY_CODE_MATCH', 
                'EXACT_NAME_WITHIN_MINISTRY',
                'EXACT_NAME_MATCH'
            ])
            
            fuzzy_matches = sum(match_stats.get(cat, 0) for cat in match_stats 
                              if cat.startswith('FUZZY'))
            
            no_matches = match_stats.get('NO_MATCH', 0)
            
            print(f"\n📊 Summary:")
            print(f"   Total agencies: {total_agencies}")
            print(f"   Exact matches: {exact_matches} ({exact_matches/total_agencies*100:.1f}%)")
            print(f"   Fuzzy matches: {fuzzy_matches} ({fuzzy_matches/total_agencies*100:.1f}%)")
            print(f"   No matches: {no_matches} ({no_matches/total_agencies*100:.1f}%)")
            
            # 7. Insert into database
            print(f"\n💾 Inserting {len(match_results)} records into database...")
            
            # Clear existing data first
            BudgetProject2024.query.delete()
            db.session.commit()
            
            inserted = 0
            skipped = 0
            
            for record in match_results:
                try:
                    # Remove match_type from record before saving
                    record.pop('match_type', None)
                    
                    # Create new record
                    budget_project = BudgetProject2024(**record)
                    db.session.add(budget_project)
                    inserted += 1
                        
                except Exception as e:
                    print(f"   Error processing record {record['code']}: {str(e)}")
                    skipped += 1
                    continue
            
            db.session.commit()
            
            # 8. Print final summary
            print(f"\n✅ Budget data ingestion complete!")
            print(f"\n📊 Final Statistics:")
            print(f"   Total records processed: {len(match_results)}")
            print(f"   Records inserted: {inserted}")
            print(f"   Records skipped: {skipped}")
            
            # 9. Save unmatched agencies for review
            if unmatched_details:
                unmatched_file = 'unmatched_agencies_detailed.csv'
                unmatched_df = pd.DataFrame(unmatched_details)
                unmatched_df.to_csv(unmatched_file, index=False)
                
                print(f"\n⚠️  {len(unmatched_details)} agencies not matched to GIFMIS")
                print(f"   Review file: {unmatched_file}")
                
                # Show top unmatched by frequency
                print(f"\n🔴 TOP 10 UNMATCHED AGENCIES:")
                agency_counts = unmatched_df['agency_name'].value_counts().head(10)
                for agency, count in agency_counts.items():
                    print(f"   {agency}: {count} projects")
            
        except FileNotFoundError:
            print(f"\n❌ ERROR: File not found at {file_path}")
            db.session.rollback()
            raise
        except Exception as e:
            print(f"\n❌ FATAL ERROR during ingestion: {str(e)}")
            import traceback
            traceback.print_exc()
            db.session.rollback()
            raise
    
    # ... keep the rest of the methods (map_mda_to_ministry, etc.) ...
    
    @staticmethod
    def map_mda_to_ministry(mda_name: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Map MDA name to parent ministry using MinistryAgency reference table
        
        Args:
            mda_name: Raw MDA name from survey
            
        Returns:
            Tuple of (normalized_mda_name, parent_ministry)
        """
        if not mda_name:
            return None, None
        
        # Normalize the MDA name
        normalized_mda = DataCleaner.normalize_text(mda_name)
        
        if not normalized_mda:
            return None, None
        
        # Try to find agency in reference table
        agency = MinistryAgency.find_agency_by_name(normalized_mda)
        
        if agency:
            # Return official canonical name and ministry
            return agency.agency_name, agency.ministry_name
        else:
            # No match found, return normalized version
            return normalized_mda, None
    
    @staticmethod
    def extract_ergp_code_and_project_name(project_name: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract ERGP code from project name
        """
        # KEEP THIS METHOD AS-IS - it's already good!
        if not project_name or not isinstance(project_name, str):
            return None, None
        
        working_name = project_name.upper()
        components = working_name.split('-')
        components = [c for c in components if c]
        
        ergp_code_normalized = None
        
        if not components:
            return DataCleaner.normalize_text(project_name), None

        # Check for ERGP code...
        
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
    
    # @classmethod
    # def ingest_and_normalize_budget_data(cls, file_path: str):
    #     """
    #     Ingest budget data - UPDATED to use MinistryAgency for matching
    #     """
    #     print(f"Starting ingestion of budget data from {file_path}...")
    #     df = pd.read_excel(file_path)

    #     # 1. Rename and Initial Cleanup
    #     df.rename(columns={
    #         'code': 'code', 
    #         'project_name': 'project_name', 
    #         'status_type': 'status_type', 
    #         'appropriation': 'appropriation', 
    #         'ministry': 'ministry', 
    #         'agency': 'agency'
    #     }, inplace=True)
        
    #     # Drop rows where 'agency' or 'code' is missing
    #     df.dropna(subset=['agency', 'code'], inplace=True)
    #     df['appropriation'] = pd.to_numeric(df['appropriation'], errors='coerce')

    #     # 2. Identify and Isolate Duplicates
    #     is_duplicate = df.duplicated(subset=['code'], keep='first')
    #     df_duplicates = df[is_duplicate].copy()
    #     df_unique = df[~is_duplicate].copy()
        
    #     print(f"Total rows: {len(df)}, Unique: {len(df_unique)}, Duplicates: {len(df_duplicates)}")

    #     # 3. Write Duplicates to Reconciliation File
    #     if len(df_duplicates) > 0:
    #         reconciliation_file = 'budget_duplicates_reconciliation.txt'
    #         with open(reconciliation_file, 'w') as f:
    #             f.write(f"--- Budget Duplicate Reconciliation ---\n\n")
    #             f.write(f"Duplicates: {len(df_duplicates)}\n\n")
    #             f.write(df_duplicates.to_string(index=False))
    #         print(f"Duplicates saved to: {reconciliation_file}")

    #     # 4. Apply NEW Normalization using MinistryAgency
    #     def normalize_agency_name(agency_raw):
    #         agency_obj = MinistryAgency.find_agency_by_name(agency_raw)
    #         if agency_obj:
    #             return agency_obj.agency_name  # Official canonical name
    #         else:
    #             # Fallback to simple normalization
    #             return DataCleaner.normalize_text(agency_raw)
        
    #     # Apply normalization
    #     df_unique['agency_normalized'] = df_unique['agency'].apply(normalize_agency_name)
        
    #     # 5. Also try to extract ministry code if agency is found
    #     def get_ministry_info(agency_raw):
    #         agency_obj = MinistryAgency.find_agency_by_name(agency_raw)
    #         if agency_obj:
    #             return agency_obj.ministry_code, agency_obj.ministry_name
    #         return None, None
        
    #     # Add ministry info if needed
    #     ministry_info = df_unique['agency'].apply(get_ministry_info)
    #     df_unique['ministry_code'] = ministry_info.apply(lambda x: x[0])
    #     df_unique['ministry_name_verified'] = ministry_info.apply(lambda x: x[1])

    #     # 6. Prepare and Save to Database
    #     df_to_insert = df_unique[[
    #         'code', 'project_name', 'status_type', 'appropriation', 
    #         'ministry', 'agency', 'agency_normalized'
    #     ]]
        
    #     print(f"Inserting {len(df_to_insert)} unique budget records...")
    #     db.session.bulk_insert_mappings(BudgetProject2024, df_to_insert.to_dict('records'))
    #     db.session.commit()
    #     print("✅ Budget data ingestion complete.")
        
    #     # Print matching statistics
    #     matched_count = df_unique['ministry_code'].notna().sum()
    #     print(f"\n📊 Budget Matching Statistics:")
    #     print(f"   Total agencies in budget: {len(df_unique)}")
    #     print(f"   Successfully matched to GIFMIS: {matched_count} ({matched_count/len(df_unique)*100:.1f}%)")

    @classmethod
    def clean_processed_data(cls, processed_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply all cleaning rules using MinistryAgency reference
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
        
        # Map MDA to parent ministry using reference table
        if processed_data.get('mda_name'):
            normalized_mda, parent_ministry = cls.map_mda_to_ministry(
                processed_data['mda_name']
            )
            processed_data['mda_name'] = normalized_mda
            processed_data['parent_ministry'] = parent_ministry
        else:
            processed_data['parent_ministry'] = None
        
        return processed_data