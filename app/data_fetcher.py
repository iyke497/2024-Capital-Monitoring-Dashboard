import re
import json
from datetime import datetime
from typing import Dict, Any, Optional
from app.api_client import APIClient
from app.models import SurveyResponse, SurveyMetadata, db
from app.question_normalizer import extract_answer_by_normalized_text

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
            "strategic_objective": extract_answer_by_normalized_text(all_answers, "strategic_objectives"),
            "key_performance_indicators": extract_answer_by_normalized_text(all_answers, "kpis"),
            "project_type": extract_answer_by_normalized_text(all_answers, "project_type"),
            "project_deliverables": extract_answer_by_normalized_text(all_answers, "deliverables"),
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
                extract_answer_by_normalized_text(all_answers, "total_planned_cost")
            ),
            "total_financial_commitment": parse_amount_value(
                extract_answer_by_normalized_text(all_answers, "total_financial_commitment")
            ),
            "completion_cert_amount": parse_amount_value(
                extract_answer_by_normalized_text(all_answers, "completion_cert_amount")
            ),

            # (...rest of your fields unchanged...)

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
