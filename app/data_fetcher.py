import json
from datetime import datetime
from typing import Dict, Any
from app.api_client import APIClient
from app.models import SurveyResponse, SurveyMetadata, db
from app.question_normalizer import extract_answer_by_normalized_text


class DataFetcher:
    """Handle data fetching and storage"""
    
    @staticmethod
    def extract_answer_by_question_id(answers: list, question_id: int) -> Any:
        """Extract answer from a list of answers by question ID"""
        for answer in answers:
            if answer.get('question', {}).get('id') == question_id:
                return answer.get('body')
        return None
    
    @staticmethod
    def extract_answer_by_question_text(answers: list, question_text: str) -> Any:
        """Extract answer from a list of answers by question text"""
        for answer in answers:
            if answer.get('question', {}).get('text') == question_text:
                return answer.get('body')
        return None
    
    @classmethod
    def process_survey_response(cls, response_data: Dict[str, Any], survey_type: str) -> Dict[str, Any]:
        """Process raw API response into database record format"""
        
        # Extract all answers from sections
        all_answers = []
        for section in response_data.get('sections', []):
            all_answers.extend(section.get('answers', []))
        
        # Use the new normalized extraction method for ALL fields
        processed_data = {
            'public_id': response_data.get('public_id'),
            'name': response_data.get('name'),
            'survey_public_id': response_data.get('survey', {}).get('public_id'),
            'survey_name': response_data.get('survey', {}).get('name'),
            'survey_type': survey_type,
            'owner_username': response_data.get('owner', {}).get('username'),
            'owner_display_name': response_data.get('owner', {}).get('display_name'),
            'organization_name': response_data.get('organization', {}).get('name'),
            'created': datetime.fromisoformat(response_data.get('created').replace('Z', '+00:00')) if response_data.get('created') else None,
            'updated': datetime.fromisoformat(response_data.get('updated').replace('Z', '+00:00')) if response_data.get('updated') else None,
            'is_draft': response_data.get('is_draft'),
            'is_report_generated': response_data.get('is_report_generated'),
            'has_submitted_report': response_data.get('has_submitted_report'),
            'survey_response_status': response_data.get('survey_response_status'),
            'is_kobo_response': response_data.get('is_kobo_response'),
            
            # === SECTION 2: PROJECT BASIC INFORMATION ===
            'percentage_completed': extract_answer_by_normalized_text(all_answers, 'percentage_completed'),
            'project_categorisation': extract_answer_by_normalized_text(all_answers, 'project_category'),
            'project_name': extract_answer_by_normalized_text(all_answers, 'project_name'),
            'mda_name': extract_answer_by_normalized_text(all_answers, 'mda_name'),
            'sub_projects': extract_answer_by_normalized_text(all_answers, 'sub_projects'),  # NEW
            'strategic_objective': extract_answer_by_normalized_text(all_answers, 'strategic_objectives'),
            'key_performance_indicators': extract_answer_by_normalized_text(all_answers, 'kpis'),
            'project_type': extract_answer_by_normalized_text(all_answers, 'project_type'),
            'project_deliverables': extract_answer_by_normalized_text(all_answers, 'deliverables'),
            'execution_method': extract_answer_by_normalized_text(all_answers, 'execution_method'),  # NEW
            
            # === SECTION 3: CONTRACTOR INFORMATION ===
            'contractor_rc_numbers': extract_answer_by_normalized_text(all_answers, 'contractor_rc_numbers'),
            'contractor_name': extract_answer_by_normalized_text(all_answers, 'contractor_name'),
            'award_certificate': extract_answer_by_normalized_text(all_answers, 'award_certificate'),  # NEW
            
            # === SECTION 4: FINANCIAL INFORMATION ===
            'project_appropriation_2024': extract_answer_by_normalized_text(all_answers, 'appropriation_amount'),
            'amount_released_2024': extract_answer_by_normalized_text(all_answers, 'amount_released'),
            'amount_utilized_2024': extract_answer_by_normalized_text(all_answers, 'amount_utilized'),
            'total_cost_planned': extract_answer_by_normalized_text(all_answers, 'total_planned_cost'),
            'total_financial_commitment': extract_answer_by_normalized_text(all_answers, 'total_financial_commitment'),
            'completion_cert_amount': extract_answer_by_normalized_text(all_answers, 'completion_cert_amount'),  # NEW
            
            # === SECTION 5: PROJECT STATUS & TIMELINE ===
            'project_status': extract_answer_by_normalized_text(all_answers, 'project_status'),  # NEW
            'start_date': extract_answer_by_normalized_text(all_answers, 'start_date'),
            'end_date': extract_answer_by_normalized_text(all_answers, 'end_date'),
            
            # === SECTION 6: IMPLEMENTATION PROGRESS ===
            'project_achievements': extract_answer_by_normalized_text(all_answers, 'achievements'),
            'completion_cert_issued': extract_answer_by_normalized_text(all_answers, 'completion_cert_issued'),  # NEW
            'job_completion_certificate': extract_answer_by_normalized_text(all_answers, 'completion_certificate_issued'),
            
            # === SECTION 7: GEOGRAPHICAL INFORMATION ===
            'state': extract_answer_by_normalized_text(all_answers, 'state'),
            'lga': extract_answer_by_normalized_text(all_answers, 'lga'),
            'ward': extract_answer_by_normalized_text(all_answers, 'ward'),
            'geolocations': extract_answer_by_normalized_text(all_answers, 'geolocations'),
            
            # === SECTION 8: DOCUMENTS & ATTACHMENTS ===
            'project_pictures': extract_answer_by_normalized_text(all_answers, 'project_pictures'),  # NEW
            'other_documents': extract_answer_by_normalized_text(all_answers, 'other_documents'),  # NEW
            
            # === SECTION 9: CHALLENGES & FEEDBACK ===
            'challenges_recommendations': extract_answer_by_normalized_text(all_answers, 'challenges_recommendations'),
            
            # Store raw data for debugging
            'raw_data': json.dumps(response_data)
        }
        
        # Handle date parsing for date fields
        date_fields = ['start_date', 'end_date']
        for field in date_fields:
            if processed_data[field]:
                try:
                    processed_data[field] = datetime.strptime(processed_data[field], '%Y-%m-%d').date()
                except (ValueError, TypeError):
                    processed_data[field] = None
        
        # Handle numeric fields
        numeric_fields = [
            'project_appropriation_2024', 'amount_released_2024', 
            'amount_utilized_2024', 'total_cost_planned', 
            'total_financial_commitment', 'completion_cert_amount',
            'percentage_completed'
        ]
        
        for field in numeric_fields:
            if processed_data[field] is not None:
                try:
                    # Remove commas and convert to float/decimal
                    if isinstance(processed_data[field], str):
                        processed_data[field] = float(processed_data[field].replace(',', ''))
                except (ValueError, TypeError):
                    processed_data[field] = None
        
        return processed_data
    
    @classmethod
    def fetch_and_store_survey(cls, survey_type: str = 'survey1'):
        """Fetch and store data for a specific survey"""
        
        print(f"Starting data fetch for {survey_type}...")
        
        # Initialize API client
        api_client = APIClient(survey_type)
        
        # Fetch all responses
        all_responses = api_client.fetch_all_responses()
        
        if not all_responses:
            print(f"No responses found for {survey_type}")
            return 0
        
        processed_count = 0
        skipped_count = 0
        
        for response in all_responses:
            try:
                # Check if response already exists
                existing = SurveyResponse.query.filter_by(
                    public_id=response.get('public_id')
                ).first()
                
                if existing:
                    print(f"Response {response.get('public_id')} already exists, skipping...")
                    skipped_count += 1
                    continue
                
                # Process the response
                processed_data = cls.process_survey_response(response, survey_type)
                
                # Create new record
                survey_response = SurveyResponse(**processed_data)
                db.session.add(survey_response)
                
                processed_count += 1
                
                # Commit every 50 records to avoid large transactions
                if processed_count % 50 == 0:
                    db.session.commit()
                    print(f"Processed {processed_count} records for {survey_type}...")
            
            except Exception as e:
                print(f"Error processing response {response.get('public_id')}: {e}")
                db.session.rollback()
                continue
        
        # Final commit
        db.session.commit()
        
        print(f"Completed processing {survey_type}: {processed_count} new, {skipped_count} skipped")
        
        # Update survey metadata
        cls._update_survey_metadata(survey_type, all_responses[0].get('survey', {}))
        
        return processed_count
    
    @classmethod
    def _update_survey_metadata(cls, survey_type: str, survey_info: Dict[str, Any]):
        """Update or create survey metadata"""
        survey_public_id = survey_info.get('public_id')
        
        metadata = SurveyMetadata.query.filter_by(
            survey_public_id=survey_public_id
        ).first()
        
        if not metadata:
            metadata = SurveyMetadata(
                survey_public_id=survey_public_id,
                survey_name=survey_info.get('name'),
                survey_type=survey_type,
                total_responses=survey_info.get('no_of_responses', 0)
            )
            db.session.add(metadata)
        else:
            metadata.last_fetched = datetime.utcnow()
            metadata.total_responses = survey_info.get('no_of_responses', 0)
        
        db.session.commit()