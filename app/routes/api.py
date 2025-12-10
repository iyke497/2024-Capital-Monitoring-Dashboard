# app/routes/api.py
from flask import Blueprint, jsonify, request
from ..models import SurveyResponse, SurveyMetadata
from ..data_fetcher import DataFetcher

api_bp = Blueprint("api", __name__)


@api_bp.post("/fetch/survey1")
def fetch_survey1():
    """API endpoint to fetch survey 1 data"""
    try:
        count = DataFetcher.fetch_and_store_survey("survey1")
        return jsonify(
            {
                "success": True,
                "message": f"Successfully fetched {count} new responses from Survey 1",
                "count": count,
            }
        )
    except Exception as e:
        return (
            jsonify(
                {
                    "success": False,
                    "message": f"Error fetching Survey 1: {str(e)}",
                }
            ),
            500,
        )


@api_bp.post("/fetch/survey2")
def fetch_survey2():
    """API endpoint to fetch survey 2 data"""
    try:
        count = DataFetcher.fetch_and_store_survey("survey2")
        return jsonify(
            {
                "success": True,
                "message": f"Successfully fetched {count} new responses from Survey 2",
                "count": count,
            }
        )
    except Exception as e:
        return (
            jsonify(
                {
                    "success": False,
                    "message": f"Error fetching Survey 2: {str(e)}",
                }
            ),
            500,
        )


@api_bp.post("/fetch/all")
def fetch_all_surveys():
    """API endpoint to fetch both surveys"""
    try:
        count1 = DataFetcher.fetch_and_store_survey("survey1")
        count2 = DataFetcher.fetch_and_store_survey("survey2")

        return jsonify(
            {
                "success": True,
                "message": f"Successfully fetched {count1} from Survey 1 and {count2} from Survey 2",
                "survey1_count": count1,
                "survey2_count": count2,
                "total_count": count1 + count2,
            }
        )
    except Exception as e:
        return (
            jsonify(
                {"success": False, "message": f"Error fetching surveys: {str(e)}"}
            ),
            500,
        )


@api_bp.get("/stats")
def get_stats():
    """Get statistics about stored data"""
    total_responses = SurveyResponse.query.count()
    survey1_count = SurveyResponse.query.filter_by(survey_type="survey1").count()
    survey2_count = SurveyResponse.query.filter_by(survey_type="survey2").count()

    metadata = SurveyMetadata.query.all()

    return jsonify(
        {
            "total_responses": total_responses,
            "survey1_count": survey1_count,
            "survey2_count": survey2_count,
            "metadata": [
                {
                    "survey_name": m.survey_name,
                    "survey_type": m.survey_type,
                    "total_responses": m.total_responses,
                    "last_fetched": m.last_fetched.isoformat()
                    if m.last_fetched
                    else None,
                }
                for m in metadata
            ],
        }
    )


@api_bp.get("/responses")
def get_responses():
    """Get paginated survey responses"""
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 20, type=int)
    survey_type = request.args.get("survey_type")

    query = SurveyResponse.query

    if survey_type:
        query = query.filter_by(survey_type=survey_type)

    paginated = query.paginate(page=page, per_page=per_page, error_out=False)

    responses = []
    for item in paginated.items:
        responses.append(
            {
                "public_id": item.public_id,
                "name": item.name,
                "survey_name": item.survey_name,
                "survey_type": item.survey_type,
                "organization_name": item.organization_name,
                "project_name": item.project_name,
                "mda_name": item.mda_name,
                "project_type": item.project_type,
                "percentage_completed": item.percentage_completed,
                "project_appropriation_2024": float(item.project_appropriation_2024)
                if item.project_appropriation_2024
                else None,
                "amount_released_2024": float(item.amount_released_2024)
                if item.amount_released_2024
                else None,
                "created": item.created.isoformat() if item.created else None,
            }
        )

    return jsonify(
        {
            "responses": responses,
            "total": paginated.total,
            "page": paginated.page,
            "pages": paginated.pages,
            "per_page": paginated.per_page,
        }
    )
