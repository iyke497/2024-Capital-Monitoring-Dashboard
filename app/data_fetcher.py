import re
import json
from datetime import datetime
from typing import Dict, Any, Optional, List
from sqlalchemy import func, case, distinct
from app.api_client import APIClient
from app.models import SurveyResponse, SurveyMetadata, BudgetProject2024, db
from app.question_normalizer import extract_answer_by_normalized_text
from app.data_cleaner import DataCleaner

def parse_amount_value(value: Any) -> Optional[float]:
    """
        Normalize various money/amount representations into a float.
        Handles:
        - None / empty -> None
        - int / float
        - plain numeric strings ("100000000" or "100,000,000.00")
        - dicts like {"year": "2024", "amount": "100000000.00"}
        - JSON strings of those dicts
    """
    if value is None:
        return None

    # Already numeric
    if isinstance(value, (int, float)):
        return float(value)

    # Dict: look for 'amount'
    if isinstance(value, dict):
        inner = (
            value.get("amount")
            or value.get("value")
            or value.get("Amount")
        )
        return parse_amount_value(inner)

    # Strings
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None

        # Try JSON object string
        if text.startswith("{") and text.endswith("}"):
            try:
                obj = json.loads(text)
            except Exception:
                obj = None
            if isinstance(obj, dict):
                return parse_amount_value(obj)

        # Fallback: strip non-numeric chars (keep digits, dot, minus)
        cleaned = re.sub(r"[^0-9.\-]", "", text)
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None

    # Anything else (lists etc.) – treat as no value for now
    return None

def convert_to_boolean(value: Any) -> Optional[bool]:
    """
        Convert various representations to boolean.
        Handles:
        - None -> None
        - Boolean values (True/False)
        - Strings: "YES"/"NO", "yes"/"no", "TRUE"/"FALSE", "true"/"false", "1"/"0"
        - Integers: 1/0
    """

    if value is None:
        return None
    
    if isinstance(value, bool):
        return value
    
    if isinstance(value, str):
        normalized = value.strip().upper()
        if normalized in ["YES", "TRUE", "1", "Y"]:
            return True
        if normalized in ["NO", "FALSE", "0", "N"]:
            return False
    
    if isinstance(value, (int, float)):
        return bool(value)
    
    # If we can't convert it, return None
    return None

class DataFetcher:
    """Handle data fetching and storage"""
    
    def extract_answer_by_question_id(answers: list, question_id: int) -> Any:
        """Extract answer from a list of answers by question ID"""
        if not isinstance(answers, list):
            return None

        for answer in answers:
            if not isinstance(answer, dict):
                continue
            question = answer.get("question") or {}
            if question.get("id") == question_id:
                return answer.get("body")
        return None

    @staticmethod
    def extract_answer_by_question_text(answers: list, question_text: str) -> Any:
        """Extract answer from a list of answers by question text"""
        if not isinstance(answers, list):
            return None

        for answer in answers:
            if not isinstance(answer, dict):
                continue
            question = answer.get("question") or {}
            if question.get("text") == question_text:
                return answer.get("body")
        return None

    @classmethod
    def process_survey_response(cls, response_data: Dict[str, Any], survey_type: str) -> Dict[str, Any]:
        """Process raw API response into database record format"""

        if not isinstance(response_data, dict):
            raise ValueError("response_data is not a dict")

        # Extract all answers from sections safely
        all_answers = []
        sections = response_data.get("sections") or []
        if not isinstance(sections, list):
            sections = []

        for section in sections:
            if not isinstance(section, dict):
                # Skip weird section entries
                continue

            answers = section.get("answers") or []
            if not isinstance(answers, list):
                continue

            for ans in answers:
                if isinstance(ans, dict):
                    all_answers.append(ans)

        # Now all_answers is a clean list of dicts, safe for downstream use
        processed_data = {
            "public_id": response_data.get("public_id"),
            "name": response_data.get("name"),
            "survey_public_id": (response_data.get("survey") or {}).get("public_id"),
            "survey_name": (response_data.get("survey") or {}).get("name"),
            "survey_type": survey_type,
            "owner_username": (response_data.get("owner") or {}).get("username"),
            "owner_display_name": (response_data.get("owner") or {}).get("display_name"),
            "organization_name": (response_data.get("organization") or {}).get("name"),
            # created/updated – same as you had, just being explicit:
            "created": (
                datetime.fromisoformat(response_data["created"].replace("Z", "+00:00"))
                if response_data.get("created")
                else None
            ),
            "updated": (
                datetime.fromisoformat(response_data["updated"].replace("Z", "+00:00"))
                if response_data.get("updated")
                else None
            ),
            "is_draft": response_data.get("is_draft"),
            "is_report_generated": response_data.get("is_report_generated"),
            "has_submitted_report": response_data.get("has_submitted_report"),
            "survey_response_status": response_data.get("survey_response_status"),
            "is_kobo_response": response_data.get("is_kobo_response"),

            # === SECTION 2: PROJECT BASIC INFORMATION ===
            "percentage_completed": extract_answer_by_normalized_text(all_answers, "percentage_completed"),
            "project_categorisation": extract_answer_by_normalized_text(all_answers, "project_category"),
            "project_name": extract_answer_by_normalized_text(all_answers, "project_name"),
            "mda_name": extract_answer_by_normalized_text(all_answers, "mda_name"),
            "sub_projects": extract_answer_by_normalized_text(all_answers, "sub_projects"),
            "strategic_objective": extract_answer_by_normalized_text(all_answers, "strategic_objective"),
            "key_performance_indicators": extract_answer_by_normalized_text(all_answers, "key_performance_indicators"),
            "project_type": extract_answer_by_normalized_text(all_answers, "project_type"),
            "project_deliverables": extract_answer_by_normalized_text(all_answers, "project_deliverables"),
            "execution_method": extract_answer_by_normalized_text(all_answers, "execution_method"),

            # === SECTION 3: CONTRACTOR INFORMATION ===
            "contractor_rc_numbers": extract_answer_by_normalized_text(all_answers, "contractor_rc_numbers"),
            "contractor_name": extract_answer_by_normalized_text(all_answers, "contractor_name"),
            "award_certificate": extract_answer_by_normalized_text(all_answers, "award_certificate"),

            # === SECTION 4: FINANCIAL INFORMATION ===
            "project_appropriation_2024": parse_amount_value(
                extract_answer_by_normalized_text(all_answers, "appropriation_amount")
            ),
            "amount_released_2024": parse_amount_value(
                extract_answer_by_normalized_text(all_answers, "amount_released")
            ),
            "amount_utilized_2024": parse_amount_value(
                extract_answer_by_normalized_text(all_answers, "amount_utilized")
            ),
            "total_cost_planned": parse_amount_value(
                extract_answer_by_normalized_text(all_answers, "total_cost_planned")
            ),
            "total_financial_commitment": parse_amount_value(
                extract_answer_by_normalized_text(all_answers, "total_financial_commitment")
            ),
            "completion_cert_amount": parse_amount_value(
                extract_answer_by_normalized_text(all_answers, "completion_cert_amount")
            ),

            # === SECTION 5: PROJECT STATUS AND TIMELINES ===
            "project_status": extract_answer_by_normalized_text(all_answers, "project_status"),
            "start_date": extract_answer_by_normalized_text(all_answers, "start_date"),
            "end_date": extract_answer_by_normalized_text(all_answers, "end_date"),

            # === SECTION 6: ACHIEVEMENTS & CERTIFICATIONS ===
            "project_achievements": extract_answer_by_normalized_text(all_answers, "project_achievements"),

            "completion_cert_issued": convert_to_boolean(extract_answer_by_normalized_text(all_answers, "completion_cert_issued")),
            "job_completion_certificate": extract_answer_by_normalized_text(all_answers, "job_completion_certificate"),

            # === SECTION 7: GEOGRAPHICAL INFORMATION ===
            "state": extract_answer_by_normalized_text(all_answers, "state"),
            "lga": extract_answer_by_normalized_text(all_answers, "lga"),
            "ward": extract_answer_by_normalized_text(all_answers, "ward"),
            "geolocations": extract_answer_by_normalized_text(all_answers, "geolocations"),

            # === SECTION 8 & 9 ===
            "project_pictures": extract_answer_by_normalized_text(all_answers, "project_pictures"),
            "other_documents": extract_answer_by_normalized_text(all_answers, "other_documents"),
            "challenges_recommendations": extract_answer_by_normalized_text(all_answers, "challenges_recommendations"),

            # Store raw data for debugging
            "raw_data": json.dumps(response_data),
        }

        # Date-field parsing stays as you had:
        date_fields = ["start_date", "end_date"]
        for field in date_fields:
            if processed_data.get(field):
                try:
                    processed_data[field] = datetime.strptime(
                        processed_data[field], "%Y-%m-%d"
                    ).date()
                except (ValueError, TypeError):
                    processed_data[field] = None

        # TODO: Apply data cleaning before returning
        processed_data = DataCleaner.clean_processed_data(processed_data)

        return processed_data

    @classmethod
    def fetch_and_store_survey(cls, survey_type: str = "survey1"):
        """Fetch and store data for a specific survey"""
        print(f"Starting data fetch for {survey_type}...")

        api_client = APIClient(survey_type)
        all_responses = api_client.fetch_all_responses()

        if not all_responses:
            print(f"No responses found for {survey_type}")
            return 0

        processed_count = 0
        skipped_count = 0

        for response in all_responses:
            try:
                public_id = (response or {}).get("public_id")
                # Check if response already exists
                existing = SurveyResponse.query.filter_by(public_id=public_id).first()
                if existing:
                    print(f"Response {public_id} already exists, skipping...")
                    skipped_count += 1
                    continue

                processed_data = cls.process_survey_response(response, survey_type)

                survey_response = SurveyResponse(**processed_data)
                db.session.add(survey_response)
                db.session.commit()

                processed_count += 1
                if processed_count % 100 == 0:
                    print(f"Processed {processed_count} records for {survey_type}...")

            except Exception as e:
                # More informative logging
                print(f"Error processing response {public_id}: {e}")
                # Optional: show high-level keys to see what shape this payload has
                try:
                    print("  Response keys:", list((response or {}).keys()))
                except Exception:
                    pass
                db.session.rollback()
                continue

        print(f"Completed processing {survey_type}: {processed_count} new, {skipped_count} skipped")

        # Metadata update (defensive: only if we have at least one survey block)
        first_survey = (all_responses[0] or {}).get("survey") or {}
        if first_survey:
            cls._update_survey_metadata(survey_type, first_survey)

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


class ComplianceMetrics:

    @staticmethod
    def calculate_mda_compliance_data() -> List[Dict[str, Any]]:
        """
        Calculates the MDA-level project compliance rate by joining Survey Responses
        (Numerator) with Budget Projects (Denominator).
        
        Returns:
            A list of dictionaries containing compliance data per MDA.
        """
        # 1. Subquery for Reported Projects (Numerator)
        # Count the number of UNIQUE ERGP codes reported per MDA
        reported_subquery = db.session.query(
            SurveyResponse.mda_name.label('mda_name'),
            func.count(distinct(SurveyResponse.ergp_code)).label('reported_projects'),
            # Counts the total number of survey forms submitted
            func.count(SurveyResponse.id).label('total_responses')
        ).group_by(
            SurveyResponse.mda_name
        ).subquery()
        # 

        # 2. Subquery for Expected Projects (Denominator)
        # Count the number of UNIQUE ERGP codes expected per Agency
        expected_subquery = db.session.query(
            BudgetProject2024.agency_normalized.label('mda_name'),
            func.count(distinct(BudgetProject2024.code)).label('expected_projects')
        ).group_by(
            BudgetProject2024.agency_normalized
        ).subquery()

        # 3. Final Join and Calculation
        # Join the two subqueries and perform the calculation
        
        # We use FULL OUTER JOIN to capture MDAs that only appear in the budget 
        # (compliance rate of 0) and MDAs that only appear in the survey (expected count 0).
        
        results = db.session.query(
            func.coalesce(expected_subquery.c.mda_name, reported_subquery.c.mda_name).label('mda_name'),
            func.coalesce(expected_subquery.c.expected_projects, 0).label('expected'),
            func.coalesce(reported_subquery.c.reported_projects, 0).label('reported'),
            func.coalesce(reported_subquery.c.total_responses, 0).label('total_responses')
        ).outerjoin(
            reported_subquery, 
            expected_subquery.c.mda_name == reported_subquery.c.mda_name
        ).all()
        
        # 4. Post-processing to calculate percentage and format for JS
        compliance_data = []
        for row in results:
            expected = row.expected
            reported = row.reported
            
            # Calculate percentage, handle division by zero
            if expected > 0:
                compliance_rate = (reported / expected) * 100
            else:
                compliance_rate = 0.0 # If 0 projects expected, compliance is not meaningful, set to 0.

            compliance_data.append({
                'mda_name': row.mda_name,
                'expected_projects': expected,
                'reported_projects': reported,
                'total_responses': row.total_responses,
                'compliance_rate_pct': round(compliance_rate, 2)
            })
            
        return compliance_data