 # app/analytics.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from difflib import SequenceMatcher  # ADD THIS
from collections import defaultdict  # ADD THIS

from sqlalchemy import func, distinct, case, literal, and_, or_
from sqlalchemy.orm import Session

from .database import db
from .models import SurveyResponse, BudgetProject2024, MinistryAgency
from .data_cleaner import DataCleaner


# -------------------------
# Helpers / config
# -------------------------

@dataclass(frozen=True)
class AnalyticsWindow:
    """Common time windows (days) used by dashboard widgets."""
    days: int = 30

    @property
    def sqlite_datetime_expr(self) -> Any:
        # datetime('now', '-30 days')
        return func.datetime("now", f"-{self.days} days")


def _non_empty_text(col):
    """SQLite-friendly: checks a column is not NULL and not empty/whitespace."""
    return and_(col.isnot(None), func.trim(col) != "")


def _safe_int(x: Any) -> int:
    try:
        return int(x or 0)
    except Exception:
        return 0


def _safe_float(x: Any) -> float:
    try:
        return float(x or 0.0)
    except Exception:
        return 0.0

# ========================================
# ADD THESE NEW CLASSES AFTER HELPERS
# ========================================

class AgencyConsolidationRules:
    """
    Rules for consolidating Ministry HQs with their parent ministry
    and handling agency name changes
    """
    
    # Ministry HQ variations that should be consolidated to parent ministry
    MINISTRY_HQ_CONSOLIDATION = {
        '255001001': '255001001',  # Federal Ministry of Tourism
        '451001001': '451001001',  # Federal Ministry of Regional Development
        '521001001': '521001001',  # Federal Ministry of Health
    }
    
    # Historical name changes
    AGENCY_NAME_CHANGES = {
        '451001001': (
            'FEDERAL MINISTRY OF NIGER DELTA DEVELOPMENT',
            'FEDERAL MINISTRY OF REGIONAL DEVELOPMENT',
            '2024-01-01'
        ),
    }
    
    # Suffix variations that should be stripped for matching
    HQ_SUFFIXES = [
        '- HQTRS', '- HQ', 'HQTRS', 'HQ', 'HEADQUARTERS', '- HEADQUARTERS',
    ]
    
    @classmethod
    def get_canonical_agency_code(cls, agency_code: str) -> str:
        """Get the canonical agency code (after consolidation)"""
        return cls.MINISTRY_HQ_CONSOLIDATION.get(agency_code, agency_code)
    
    @classmethod
    def get_current_name(cls, agency_code: str) -> Optional[str]:
        """Get the current name for an agency (handles name changes)"""
        if agency_code in cls.AGENCY_NAME_CHANGES:
            return cls.AGENCY_NAME_CHANGES[agency_code][1]
        return None
    
    @classmethod
    def normalize_ministry_name(cls, name: str) -> str:
        """Normalize ministry name by removing HQ suffixes"""
        if not name:
            return ""
        
        normalized = name.upper().strip()
        
        # Remove HQ suffixes
        for suffix in cls.HQ_SUFFIXES:
            if normalized.endswith(suffix):
                normalized = normalized[:-len(suffix)].strip()
        
        normalized = normalized.replace('&', ' AND ')
        normalized = ' '.join(normalized.split())
        
        return normalized
    
    @classmethod
    def is_ministry_hq(cls, agency_name: str, agency_code: str = None) -> bool:
        """Check if this is a ministry HQ"""
        if agency_code and agency_code in cls.MINISTRY_HQ_CONSOLIDATION:
            return True
        
        name_upper = agency_name.upper()
        for suffix in cls.HQ_SUFFIXES:
            if suffix in name_upper:
                return True
        
        return False


class ImprovedMinistryAgency:
    """Enhanced MinistryAgency operations with consolidation support"""
    
    @staticmethod
    def find_agency_by_name_improved(name: str, threshold: float = 0.90) -> Optional[MinistryAgency]:
        """Enhanced fuzzy matching with 90% threshold"""
        if not name:
            return None
        
        normalized_search = AgencyConsolidationRules.normalize_ministry_name(name)
        
        # Try exact match
        exact_match = MinistryAgency.query.filter(
            func.upper(MinistryAgency.agency_name_normalized) == normalized_search,
            MinistryAgency.is_active == True
        ).first()
        
        if exact_match:
            return exact_match
        
        # Fuzzy match
        all_agencies = MinistryAgency.query.filter(
            MinistryAgency.is_active == True
        ).all()
        
        best_match = None
        best_score = threshold
        
        for agency in all_agencies:
            agency_normalized = AgencyConsolidationRules.normalize_ministry_name(
                agency.agency_name
            )
            
            score = SequenceMatcher(
                None, 
                normalized_search, 
                agency_normalized
            ).ratio()
            
            if score > best_score:
                best_score = score
                best_match = agency
        
        return best_match
    
    @staticmethod
    def link_survey_responses(force_relink: bool = False) -> Dict[str, Any]:
        """Link survey responses to MinistryAgency"""
        if force_relink:
            unlinked = SurveyResponse.query.filter(
                SurveyResponse.mda_name.isnot(None),
                SurveyResponse.mda_name != ''
            ).all()
        else:
            unlinked = SurveyResponse.query.filter(
                SurveyResponse.ministry_agency_id.is_(None),
                SurveyResponse.mda_name.isnot(None),
                SurveyResponse.mda_name != ''
            ).all()
        
        linked_count = 0
        fuzzy_matched = 0
        unmatched = []
        
        for response in unlinked:
            agency = ImprovedMinistryAgency.find_agency_by_name_improved(
                response.mda_name, 
                threshold=0.90
            )
            
            if agency:
                response.ministry_agency_id = agency.id
                
                normalized_response = AgencyConsolidationRules.normalize_ministry_name(
                    response.mda_name
                )
                normalized_agency = AgencyConsolidationRules.normalize_ministry_name(
                    agency.agency_name
                )
                
                if normalized_response == normalized_agency:
                    linked_count += 1
                else:
                    fuzzy_matched += 1
            else:
                unmatched.append({
                    'mda_name': response.mda_name,
                    'ergp_code': response.ergp_code
                })
        
        db.session.commit()
        
        return {
            'linked': linked_count,
            'fuzzy': fuzzy_matched,
            'unmatched': len(unmatched),
            'unmatched_details': unmatched
        }


# -------------------------
# Base class (optional)
# -------------------------

class AnalyticsBase:
    """Base class to share the db session, and small conveniences."""
    def __init__(self, session: Optional[Session] = None):
        self.session: Session = session or db.session


# -------------------------
# Activity / engagement analytics
# -------------------------

class ActivityAnalytics(AnalyticsBase):
    def latest_responding_agencies(self, limit: int = 20) -> List[Dict[str, Any]]:
        rows = (
            self.session.query(
                SurveyResponse.parent_ministry.label("parent_ministry"),
                SurveyResponse.mda_name.label("mda_name"),
                func.max(SurveyResponse.created_at).label("latest_response_at"),
                func.count(SurveyResponse.id).label("total_responses"),
            )
            .group_by(SurveyResponse.parent_ministry, SurveyResponse.mda_name)
            .order_by(func.max(SurveyResponse.created_at).desc())
            .limit(limit)
            .all()
        )

        out: List[Dict[str, Any]] = []
        for r in rows:
            parent_min = r.parent_ministry
            if not parent_min:
                _, parent_min = DataCleaner.map_mda_to_ministry(r.mda_name)

            out.append(
                {
                    "parent_ministry": parent_min or "OTHER INDEPENDENT AGENCIES",
                    "mda_name": r.mda_name,
                    "latest_response_at": r.latest_response_at.isoformat() if r.latest_response_at else None,
                    "total_responses": _safe_int(r.total_responses),
                }
            )
        return out

    def activity_summary_by_mda(self, window_days: int = 30) -> List[Dict[str, Any]]:
        win = AnalyticsWindow(days=window_days).sqlite_datetime_expr

        rows = (
            self.session.query(
                SurveyResponse.parent_ministry.label("parent_ministry"),
                SurveyResponse.mda_name.label("mda_name"),
                func.count(SurveyResponse.id).label("responses"),
                func.count(distinct(SurveyResponse.ergp_code)).label("unique_projects"),
                func.sum(case((SurveyResponse.is_draft == True, 1), else_=0)).label("drafts"),
                func.sum(case((SurveyResponse.has_submitted_report == True, 1), else_=0)).label("submitted"),
                func.count(distinct(func.date(SurveyResponse.created_at))).label("active_days"),
                func.max(SurveyResponse.created_at).label("latest_response_at"),
            )
            .filter(SurveyResponse.created_at >= win)
            .group_by(SurveyResponse.parent_ministry, SurveyResponse.mda_name)
            .order_by(func.count(SurveyResponse.id).desc())
            .all()
        )

        out: List[Dict[str, Any]] = []
        for r in rows:
            parent_min = r.parent_ministry
            if not parent_min:
                _, parent_min = DataCleaner.map_mda_to_ministry(r.mda_name)

            responses = _safe_int(r.responses)
            submitted = _safe_int(r.submitted)

            out.append(
                {
                    "parent_ministry": parent_min or "OTHER INDEPENDENT AGENCIES",
                    "mda_name": r.mda_name,
                    "responses_30d": responses,
                    "unique_projects_30d": _safe_int(r.unique_projects),
                    "drafts_30d": _safe_int(r.drafts),
                    "submitted_30d": submitted,
                    "submission_rate_pct_30d": round((submitted / responses) * 100, 2) if responses > 0 else 0.0,
                    "active_days_30d": _safe_int(r.active_days),
                    "latest_response_at": r.latest_response_at.isoformat() if r.latest_response_at else None,
                }
            )
        return out

    def monthly_activity_summary(self) -> List[Dict[str, Any]]:
        """
        Returns daily response counts for the past 30 days.
        """
        
        # Get responses from the last 30 days
        thirty_days_ago = datetime.utcnow() - timedelta(days=30)
        
        rows = (
            self.session.query(
                func.date(SurveyResponse.updated).label("response_date"),
                func.count(SurveyResponse.id).label("total_responses"),
                func.sum(case((SurveyResponse.survey_type == "survey1", 1), else_=0)).label("survey1_count"),
                func.sum(case((SurveyResponse.survey_type == "survey2", 1), else_=0)).label("survey2_count"),
            )
            .filter(SurveyResponse.updated >= thirty_days_ago)
            .group_by(func.date(SurveyResponse.updated))
            .order_by(func.date(SurveyResponse.updated))
            .all()
        )
        
        # Create a dict of dates with counts
        activity_by_date = {}
        for r in rows:
            date_str = r.response_date.isoformat() if hasattr(r.response_date, 'isoformat') else str(r.response_date)
            activity_by_date[date_str] = {
                "date": date_str,
                "total": _safe_int(r.total_responses),
                "survey1": _safe_int(r.survey1_count),
                "survey2": _safe_int(r.survey2_count),
            }
        
        # Fill in missing dates with zero counts
        result = []
        for i in range(30):
            date = (datetime.utcnow() - timedelta(days=29-i)).date()
            date_str = date.isoformat()
            
            if date_str in activity_by_date:
                result.append(activity_by_date[date_str])
            else:
                result.append({
                    "date": date_str,
                    "total": 0,
                    "survey1": 0,
                    "survey2": 0,
                })
        
        return result

    def weekly_activity_summary(self) -> List[Dict[str, Any]]:
        """
        Returns daily response counts for the past 7 days.
        Useful for activity timeline charts.
        """
        
        # Get responses from the last 7 days
        seven_days_ago = datetime.utcnow() - timedelta(days=7)
        
        rows = (
            self.session.query(
                func.date(SurveyResponse.updated).label("response_date"),
                func.count(SurveyResponse.id).label("total_responses"),
                func.sum(case((SurveyResponse.survey_type == "survey1", 1), else_=0)).label("survey1_count"),
                func.sum(case((SurveyResponse.survey_type == "survey2", 1), else_=0)).label("survey2_count"),
            )
            .filter(SurveyResponse.updated >= seven_days_ago)
            .group_by(func.date(SurveyResponse.updated))
            .order_by(func.date(SurveyResponse.updated))
            .all()
        )
        
        # Create a dict of dates with counts
        activity_by_date = {}
        for r in rows:
            date_str = r.response_date.isoformat() if hasattr(r.response_date, 'isoformat') else str(r.response_date)
            activity_by_date[date_str] = {
                "date": date_str,
                "total": _safe_int(r.total_responses),
                "survey1": _safe_int(r.survey1_count),
                "survey2": _safe_int(r.survey2_count),
            }
        
        # Fill in missing dates with zero counts
        result = []
        for i in range(7):
            date = (datetime.utcnow() - timedelta(days=6-i)).date()
            date_str = date.isoformat()
            
            if date_str in activity_by_date:
                result.append(activity_by_date[date_str])
            else:
                result.append({
                    "date": date_str,
                    "total": 0,
                    "survey1": 0,
                    "survey2": 0,
                })
        
        return result

# -------------------------
# Data quality + evidence
# -------------------------

class QualityAnalytics(AnalyticsBase):
    def evidence_coverage_by_mda(self, window_days: Optional[int] = None) -> List[Dict[str, Any]]:
        q = self.session.query(
            SurveyResponse.parent_ministry.label("parent_ministry"),
            SurveyResponse.mda_name.label("mda_name"),
            func.count(SurveyResponse.id).label("responses"),
            func.sum(case((_non_empty_text(SurveyResponse.project_pictures), 1), else_=0)).label("with_pictures"),
            func.sum(case((_non_empty_text(SurveyResponse.geolocations), 1), else_=0)).label("with_geo"),
            func.sum(case((_non_empty_text(SurveyResponse.other_documents), 1), else_=0)).label("with_docs"),
            func.sum(case((SurveyResponse.award_certificate.isnot(None), 1), else_=0)).label("with_award_cert"),
            func.sum(case((SurveyResponse.job_completion_certificate.isnot(None), 1), else_=0)).label("with_jcc"),
        )

        if window_days is not None:
            win = AnalyticsWindow(days=window_days).sqlite_datetime_expr
            q = q.filter(SurveyResponse.created_at >= win)

        rows = (
            q.group_by(SurveyResponse.parent_ministry, SurveyResponse.mda_name)
            .order_by(func.count(SurveyResponse.id).desc())
            .all()
        )

        out: List[Dict[str, Any]] = []
        for r in rows:
            parent_min = r.parent_ministry
            if not parent_min:
                _, parent_min = DataCleaner.map_mda_to_ministry(r.mda_name)

            responses = _safe_int(r.responses)
            with_any_evidence = _safe_int(r.with_pictures) + _safe_int(r.with_geo) + _safe_int(r.with_docs)

            out.append(
                {
                    "parent_ministry": parent_min or "OTHER INDEPENDENT AGENCIES",
                    "mda_name": r.mda_name,
                    "responses": responses,
                    "with_pictures": _safe_int(r.with_pictures),
                    "with_geo": _safe_int(r.with_geo),
                    "with_docs": _safe_int(r.with_docs),
                    "with_award_cert": _safe_int(r.with_award_cert),
                    "with_jcc": _safe_int(r.with_jcc),
                    "pct_with_pictures": round((_safe_int(r.with_pictures) / responses) * 100, 2) if responses else 0.0,
                    "pct_with_geo": round((_safe_int(r.with_geo) / responses) * 100, 2) if responses else 0.0,
                    "pct_with_docs": round((_safe_int(r.with_docs) / responses) * 100, 2) if responses else 0.0,
                    "pct_with_any_evidence_proxy": round((with_any_evidence / responses) * 100, 2) if responses else 0.0,
                }
            )
        return out

    def data_quality_flags_by_mda(self, limit: int = 200) -> List[Dict[str, Any]]:
        """
        Useful “red flags” you can surface:
        - utilized > released
        - missing ERGP code
        - missing state/LGA
        - missing financials when report submitted
        """
        rows = (
            self.session.query(
                SurveyResponse.parent_ministry.label("parent_ministry"),
                SurveyResponse.mda_name.label("mda_name"),
                func.count(SurveyResponse.id).label("responses"),
                func.sum(
                    case(
                        (
                            func.coalesce(SurveyResponse.amount_utilized_2024, 0)
                            > func.coalesce(SurveyResponse.amount_released_2024, 0),
                            1,
                        ),
                        else_=0,
                    )
                ).label("utilized_gt_released"),
                func.sum(case((_non_empty_text(SurveyResponse.ergp_code) == False, 1), else_=0)).label("missing_ergp"),
                func.sum(case((_non_empty_text(SurveyResponse.state) == False, 1), else_=0)).label("missing_state"),
                func.sum(case((_non_empty_text(SurveyResponse.lga) == False, 1), else_=0)).label("missing_lga"),
                func.sum(
                    case(
                        (
                            and_(
                                SurveyResponse.has_submitted_report == True,
                                func.coalesce(SurveyResponse.project_appropriation_2024, 0) == 0,
                            ),
                            1,
                        ),
                        else_=0,
                    )
                ).label("submitted_missing_appropriation"),
            )
            .group_by(SurveyResponse.parent_ministry, SurveyResponse.mda_name)
            .order_by(func.sum(case((_non_empty_text(SurveyResponse.ergp_code) == False, 1), else_=0)).desc())
            .limit(limit)
            .all()
        )

        out: List[Dict[str, Any]] = []
        for r in rows:
            parent_min = r.parent_ministry
            if not parent_min:
                _, parent_min = DataCleaner.map_mda_to_ministry(r.mda_name)

            out.append(
                {
                    "parent_ministry": parent_min or "OTHER INDEPENDENT AGENCIES",
                    "mda_name": r.mda_name,
                    "responses": _safe_int(r.responses),
                    "utilized_gt_released": _safe_int(r.utilized_gt_released),
                    "missing_ergp": _safe_int(r.missing_ergp),
                    "missing_state": _safe_int(r.missing_state),
                    "missing_lga": _safe_int(r.missing_lga),
                    "submitted_missing_appropriation": _safe_int(r.submitted_missing_appropriation),
                }
            )
        return out


# -------------------------
# Performance analytics (rankings)
# -------------------------

class PerformanceAnalytics(AnalyticsBase):
    """
    Unified compliance and performance analytics using agency codes.
    This replaces the old ComplianceMetrics in data_fetcher.py
    """
    
    def calculate_mda_compliance_data(self) -> List[Dict[str, Any]]:
        """
        Calculate MDA compliance with Ministry HQ consolidation.
        This is the primary compliance calculation method.
        """
        # STEP 1: Get budget counts by canonical agency_code
        budget_by_agency_raw = self.session.query(
            BudgetProject2024.agency_code,
            BudgetProject2024.code
        ).filter(
            BudgetProject2024.agency_code.isnot(None),
            BudgetProject2024.agency_code != ''
        ).distinct().all()
        
        # Group by canonical agency code (consolidating HQs)
        canonical_budget_counts = defaultdict(set)
        
        for agency_code, ergp_code in budget_by_agency_raw:
            canonical_code = AgencyConsolidationRules.get_canonical_agency_code(agency_code)
            canonical_budget_counts[canonical_code].add(ergp_code)
        
        budget_lookup = {
            code: len(ergp_codes) 
            for code, ergp_codes in canonical_budget_counts.items()
        }
        
        # STEP 2: Get survey data grouped by canonical agency
        survey_data_raw = self.session.query(
            MinistryAgency.agency_code,
            MinistryAgency.agency_name,
            MinistryAgency.ministry_name,
            SurveyResponse.ergp_code,
            SurveyResponse.id
        ).join(
            SurveyResponse,
            SurveyResponse.ministry_agency_id == MinistryAgency.id
        ).filter(
            MinistryAgency.is_active == True
        ).all()
        
        # Group by canonical agency code
        canonical_survey_data = defaultdict(lambda: {
            'reported_ergp': set(),
            'total_submissions': 0,
            'agency_name': None,
            'ministry_name': None
        })
        
        for agency_code, agency_name, ministry_name, ergp_code, response_id in survey_data_raw:
            canonical_code = AgencyConsolidationRules.get_canonical_agency_code(agency_code)
            
            if ergp_code:
                canonical_survey_data[canonical_code]['reported_ergp'].add(ergp_code)
            
            canonical_survey_data[canonical_code]['total_submissions'] += 1
            
            if not canonical_survey_data[canonical_code]['agency_name']:
                current_name = AgencyConsolidationRules.get_current_name(canonical_code)
                canonical_survey_data[canonical_code]['agency_name'] = (
                    current_name or agency_name
                )
                canonical_survey_data[canonical_code]['ministry_name'] = ministry_name
        
        # STEP 3: Combine and calculate compliance
        compliance_data = []
        all_canonical_codes = set(budget_lookup.keys()) | set(canonical_survey_data.keys())
        
        for canonical_code in all_canonical_codes:
            agency = MinistryAgency.query.filter_by(
                agency_code=canonical_code,
                is_active=True
            ).first()
            
            if not agency:
                continue
            
            expected = budget_lookup.get(canonical_code, 0)
            
            survey_info = canonical_survey_data.get(canonical_code, {
                'reported_ergp': set(),
                'total_submissions': 0,
                'agency_name': agency.agency_name,
                'ministry_name': agency.ministry_name
            })
            
            reported = len(survey_info['reported_ergp'])
            total_subs = survey_info['total_submissions']
            
            display_name = (
                AgencyConsolidationRules.get_current_name(canonical_code) or 
                agency.agency_name
            )
            
            compliance_rate = 0.0
            if expected > 0:
                compliance_rate = min(100.0, (reported / expected) * 100)
            
            compliance_data.append({
                'mda_name': display_name,
                'agency_code': canonical_code,
                'parent_ministry': agency.ministry_name,
                'expected_projects': expected,
                'reported_projects': reported,
                'total_submissions': total_subs,
                'compliance_rate_pct': round(compliance_rate, 2),
                'avg_submissions_per_project': round(total_subs / reported, 1) if reported > 0 else 0,
                'has_budget': expected > 0,
                'has_survey': reported > 0 or total_subs > 0,
                'is_ministry_hq': AgencyConsolidationRules.is_ministry_hq(
                    display_name, canonical_code
                )
            })
        
        compliance_data.sort(key=lambda x: (x['parent_ministry'], x['mda_name']))
        
        return compliance_data
    
    def calculate_ministry_compliance_data(self) -> List[Dict[str, Any]]:
        """Calculate ministry-level compliance (aggregated from MDA level)"""
        mda_data = self.calculate_mda_compliance_data()
        
        ministry_stats = defaultdict(lambda: {
            'expected': 0,
            'reported': 0,
            'total_submissions': 0,
            'mda_count': 0,
            'mdas': []
        })
        
        for mda in mda_data:
            ministry = mda['parent_ministry']
            ministry_stats[ministry]['expected'] += mda['expected_projects']
            ministry_stats[ministry]['reported'] += mda['reported_projects']
            ministry_stats[ministry]['total_submissions'] += mda['total_submissions']
            ministry_stats[ministry]['mda_count'] += 1
            ministry_stats[ministry]['mdas'].append(mda['mda_name'])
        
        ministry_data = []
        for ministry, stats in ministry_stats.items():
            compliance_rate = 0.0
            if stats['expected'] > 0:
                compliance_rate = (stats['reported'] / stats['expected']) * 100
            
            ministry_data.append({
                'ministry_name': ministry,
                'mda_count': stats['mda_count'],
                'expected_projects': stats['expected'],
                'reported_projects': stats['reported'],
                'total_responses': stats['total_submissions'],
                'avg_completion': round(compliance_rate, 2),
                'total_budget': 0  # TODO: Sum budgets if needed
            })
        
        return sorted(ministry_data, key=lambda x: x['ministry_name'])
    
    def get_mda_project_details(self, agency_code: str) -> List[Dict[str, Any]]:
        """Get detailed project list for a specific MDA"""
        canonical_code = AgencyConsolidationRules.get_canonical_agency_code(agency_code)
        
        agency = MinistryAgency.query.filter_by(
            agency_code=canonical_code,
            is_active=True
        ).first()
        
        if not agency:
            return []
        
        # Get budget projects
        budget_projects = BudgetProject2024.query.filter_by(
            agency_code=canonical_code
        ).all()
        
        # Get survey responses
        survey_responses = self.session.query(
            SurveyResponse
        ).join(
            MinistryAgency,
            SurveyResponse.ministry_agency_id == MinistryAgency.id
        ).filter(
            MinistryAgency.agency_code == canonical_code
        ).all()
        
        reported_ergp = {resp.ergp_code: resp for resp in survey_responses if resp.ergp_code}
        
        project_details = []
        
        for budget_proj in budget_projects:
            reported_response = reported_ergp.get(budget_proj.code)
            
            project_details.append({
                'project_code': budget_proj.code,
                'project_title': budget_proj.project_name,
                'budget_allocation': float(budget_proj.appropriation) if budget_proj.appropriation else 0,
                'reported': reported_response is not None,
                'submission_id': reported_response.public_id if reported_response else None,
                'amount_released': float(reported_response.amount_released_2024) if reported_response and reported_response.amount_released_2024 else 0,
                'amount_utilized': float(reported_response.amount_utilized_2024) if reported_response and reported_response.amount_utilized_2024 else 0,
                'project_status': reported_response.project_status if reported_response else None
            })
        
        return project_details
    
    def mda_performance_table(self) -> List[Dict[str, Any]]:
        """
        Enhanced performance table that uses the new compliance calculation.
        This replaces the old method that used string matching.
        """
        # Get compliance data (already calculates expected/reported correctly)
        compliance_data = self.calculate_mda_compliance_data()
        
        # Now enhance with additional performance metrics
        performance_data = []
        
        for mda in compliance_data:
            agency_code = mda['agency_code']
            
            # Get additional metrics for this MDA
            agency = MinistryAgency.query.filter_by(agency_code=agency_code).first()
            
            if not agency:
                continue
            
            # Get survey responses for performance metrics
            responses = SurveyResponse.query.filter_by(
                ministry_agency_id=agency.id
            ).all()
            
            total_responses = len(responses)
            submitted_count = sum(1 for r in responses if r.has_submitted_report)
            
            completion_scores = [
                float(r.percentage_completed) 
                for r in responses 
                if r.percentage_completed
            ]
            avg_completion_pct = (
                sum(completion_scores) / len(completion_scores) 
                if completion_scores else 0
            )
            
            # Evidence metrics
            with_pictures = sum(1 for r in responses if r.project_pictures)
            with_geo = sum(1 for r in responses if r.geolocations)
            with_docs = sum(1 for r in responses if r.other_documents)
            
            evidence_hits = with_pictures + with_geo + with_docs
            evidence_rate_proxy = (
                (evidence_hits / total_responses) * 100 
                if total_responses > 0 else 0
            )
            
            # Recency
            latest_response_at = max(
                (r.created_at for r in responses if r.created_at),
                default=None
            )
            
            days_since = None
            recency_score_10 = 0.0
            if latest_response_at:
                days_since = int((datetime.utcnow() - latest_response_at).total_seconds() // 86400)
                if days_since <= 3:
                    recency_score_10 = 10.0
                elif days_since <= 7:
                    recency_score_10 = 7.0
                elif days_since <= 14:
                    recency_score_10 = 4.0
            
            # Submission rate
            submission_rate_pct = (
                (submitted_count / total_responses) * 100 
                if total_responses > 0 else 0
            )
            
            # Performance index (compliance-weighted)
            performance_index = mda['compliance_rate_pct']  # Start with compliance
            
            performance_data.append({
                'parent_ministry': mda['parent_ministry'],
                'mda_name': mda['mda_name'],
                'agency_code': agency_code,
                'expected_projects': mda['expected_projects'],
                'reported_projects': mda['reported_projects'],
                'total_responses': total_responses,
                'compliance_rate_pct': mda['compliance_rate_pct'],
                'submission_rate_pct': round(submission_rate_pct, 2),
                'avg_completion_pct': round(avg_completion_pct, 2),
                'evidence_rate_proxy_pct': round(evidence_rate_proxy, 2),
                'latest_response_at': latest_response_at.isoformat() if latest_response_at else None,
                'days_since_last_response': days_since,
                'performance_index': round(performance_index, 2),
            })
        
        return performance_data
    
    # Keep existing methods but update them to use new calculation
    def best_and_worst_within_ministry(
        self,
        parent_ministry: str,
        top_n: int = 10,
        min_expected_projects: int = 1,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Returns best/worst agencies in a ministry"""
        rows = [
            r for r in self.mda_performance_table()
            if (r.get("parent_ministry") or "").strip().lower() == parent_ministry.strip().lower()
            and _safe_int(r.get("expected_projects")) >= min_expected_projects
        ]
        
        rows_sorted = sorted(rows, key=lambda x: _safe_float(x.get("performance_index")), reverse=True)
        
        return {
            "best": rows_sorted[:top_n],
            "worst": list(reversed(rows_sorted[-top_n:])) if rows_sorted else [],
        }
    
    def budget_reporting_overview(self) -> Dict[str, Any]:
        """Overall budget compliance summary"""
        # Get total budget projects
        total_budget_projects = (
            self.session.query(func.count(distinct(BudgetProject2024.code)))
            .scalar() or 0
        )
        
        # Get reported codes
        reported_codes = (
            self.session.query(distinct(SurveyResponse.ergp_code))
            .filter(SurveyResponse.ergp_code.isnot(None))
            .filter(func.trim(SurveyResponse.ergp_code) != "")
            .all()
        )
        reported_codes_set = {code[0] for code in reported_codes if code[0]}
        
        # Count reported budget projects
        reported_budget_projects = (
            self.session.query(func.count(distinct(BudgetProject2024.code)))
            .filter(BudgetProject2024.code.in_(reported_codes_set))
            .scalar() or 0
        )
        
        unreported_projects = total_budget_projects - reported_budget_projects
        
        reported_pct = (reported_budget_projects / total_budget_projects * 100) if total_budget_projects > 0 else 0
        unreported_pct = (unreported_projects / total_budget_projects * 100) if total_budget_projects > 0 else 0
        
        return {
            "total_budget_projects": total_budget_projects,
            "reported_projects": reported_budget_projects,
            "unreported_projects": unreported_projects,
            "reported_percentage": round(reported_pct, 2),
            "unreported_percentage": round(unreported_pct, 2),
        }

# -------------------------
# Facade for routes (single entry point)
# -------------------------

class AnalyticsService:
    """Facade for routes"""
    
    def __init__(self, session: Optional[Session] = None):
        self.session = session or db.session
        self.activity = ActivityAnalytics(self.session)
        self.quality = QualityAnalytics(self.session)
        self.performance = PerformanceAnalytics(self.session)
    
    def dashboard_overview(self) -> Dict[str, Any]:
        """Dashboard payload"""
        return {
            "latest_responders": self.activity.latest_responding_agencies(limit=20),
            "activity_30d": self.activity.activity_summary_by_mda(window_days=30),
            "weekly_activity": self.activity.weekly_activity_summary(),
            "evidence_coverage": self.quality.evidence_coverage_by_mda(window_days=30),
            "quality_flags": self.quality.data_quality_flags_by_mda(limit=200),
            "performance_table": self.performance.mda_performance_table(),
            "budget_reporting": self.performance.budget_reporting_overview(),
        }
    
    # ADD these new methods for compliance endpoints
    def mda_compliance(self) -> List[Dict[str, Any]]:
        """MDA-level compliance data"""
        return self.performance.calculate_mda_compliance_data()
    
    def ministry_compliance(self) -> List[Dict[str, Any]]:
        """Ministry-level compliance data"""
        return self.performance.calculate_ministry_compliance_data()
    
    def mda_projects(self, agency_code: str) -> List[Dict[str, Any]]:
        """Project details for specific MDA"""
        return self.performance.get_mda_project_details(agency_code)
