# app/analytics.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import func, distinct, case, literal, and_
from sqlalchemy.orm import Session

from .database import db
from .models import SurveyResponse, BudgetProject2024
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
    def _expected_projects_by_mda_subquery(self):
        return (
            self.session.query(
                BudgetProject2024.agency_normalized.label("mda_name"),
                func.count(distinct(BudgetProject2024.code)).label("expected_projects"),
            )
            .group_by(BudgetProject2024.agency_normalized)
            .subquery()
        )

    def _reported_projects_by_mda_subquery(self):
        # NOTE: group by mda_name only to avoid splitting due to inconsistent parent_ministry
        return (
            self.session.query(
                SurveyResponse.mda_name.label("mda_name"),
                func.max(SurveyResponse.parent_ministry).label("parent_ministry"),
                func.count(distinct(SurveyResponse.ergp_code)).label("reported_projects"),
                func.count(SurveyResponse.id).label("total_responses"),
                func.avg(func.coalesce(SurveyResponse.percentage_completed, 0)).label("avg_completion_pct"),
                func.sum(case((SurveyResponse.has_submitted_report == True, 1), else_=0)).label("submitted_count"),
                func.max(SurveyResponse.created_at).label("latest_response_at"),
                func.sum(case((_non_empty_text(SurveyResponse.project_pictures), 1), else_=0)).label("with_pictures"),
                func.sum(case((_non_empty_text(SurveyResponse.geolocations), 1), else_=0)).label("with_geo"),
                func.sum(case((_non_empty_text(SurveyResponse.other_documents), 1), else_=0)).label("with_docs"),
            )
            .group_by(SurveyResponse.mda_name)
            .subquery()
        )

    def mda_performance_table(self) -> List[Dict[str, Any]]:
        """
        Returns one row per MDA with:
        - compliance rate proxy (reported/expected)
        - submission_rate, avg_completion
        - evidence proxy
        - recency (days since last response)
        - composite score (simple MVP)
        """
        expected = self._expected_projects_by_mda_subquery()
        reported = self._reported_projects_by_mda_subquery()

        # LEFT JOIN from expected -> reported (budget mdas). This drops survey-only MDAs.
        # If you want FULL OUTER JOIN in SQLite, we can apply the union strategy later.
        rows = (
            self.session.query(
                func.coalesce(expected.c.mda_name, reported.c.mda_name).label("mda_name"),
                reported.c.parent_ministry.label("parent_ministry"),
                func.coalesce(expected.c.expected_projects, 0).label("expected"),
                func.coalesce(reported.c.reported_projects, 0).label("reported"),
                func.coalesce(reported.c.total_responses, 0).label("total_responses"),
                func.coalesce(reported.c.avg_completion_pct, 0).label("avg_completion_pct"),
                func.coalesce(reported.c.submitted_count, 0).label("submitted_count"),
                reported.c.latest_response_at.label("latest_response_at"),
                func.coalesce(reported.c.with_pictures, 0).label("with_pictures"),
                func.coalesce(reported.c.with_geo, 0).label("with_geo"),
                func.coalesce(reported.c.with_docs, 0).label("with_docs"),
            )
            .outerjoin(reported, expected.c.mda_name == reported.c.mda_name)
            .all()
        )

        out: List[Dict[str, Any]] = []
        for r in rows:
            mda_name = r.mda_name
            parent_min = r.parent_ministry
            if not parent_min:
                _, parent_min = DataCleaner.map_mda_to_ministry(mda_name)

            expected_n = _safe_int(r.expected)
            reported_n = _safe_int(r.reported)
            total_responses = _safe_int(r.total_responses)
            submitted = _safe_int(r.submitted_count)

            compliance_pct = (reported_n / expected_n) * 100 if expected_n > 0 else 0.0
            submission_rate_pct = (submitted / total_responses) * 100 if total_responses > 0 else 0.0
            avg_completion_pct = _safe_float(r.avg_completion_pct)

            evidence_hits = _safe_int(r.with_pictures) + _safe_int(r.with_geo) + _safe_int(r.with_docs)
            evidence_rate_proxy = (evidence_hits / total_responses) * 100 if total_responses > 0 else 0.0

            # Recency score: 10 if within 3 days, 7 if within 7 days, 4 within 14 days, else 0
            days_since = None
            recency_score_10 = 0.0
            if r.latest_response_at:
                days_since = int((datetime.utcnow() - r.latest_response_at).total_seconds() // 86400)
                if days_since <= 3:
                    recency_score_10 = 10.0
                elif days_since <= 7:
                    recency_score_10 = 7.0
                elif days_since <= 14:
                    recency_score_10 = 4.0
                else:
                    recency_score_10 = 0.0

            # MVP composite (0-100): tune weights later
            # 40% compliance + 20% submission + 20% completion + 10% evidence + 10% recency(0-10 scaled to 0-100)
            composite = (
                0.40 * compliance_pct
                + 0.20 * submission_rate_pct
                + 0.20 * avg_completion_pct
                + 0.10 * evidence_rate_proxy
                + 0.10 * (recency_score_10 * 10.0)
            )

            out.append(
                {
                    "parent_ministry": parent_min or "OTHER INDEPENDENT AGENCIES",
                    "mda_name": mda_name,
                    "expected_projects": expected_n,
                    "reported_projects": reported_n,
                    "total_responses": total_responses,
                    "compliance_rate_pct": round(compliance_pct, 2),
                    "submission_rate_pct": round(submission_rate_pct, 2),
                    "avg_completion_pct": round(avg_completion_pct, 2),
                    "evidence_rate_proxy_pct": round(evidence_rate_proxy, 2),
                    "latest_response_at": r.latest_response_at.isoformat() if r.latest_response_at else None,
                    "days_since_last_response": days_since,
                    "performance_index": round(composite, 2),
                }
            )

        return out

    def best_and_worst_within_ministry(
        self,
        parent_ministry: str,
        top_n: int = 10,
        min_expected_projects: int = 1,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Returns best/worst agencies in a given ministry based on performance_index.
        """
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
        """
            Calculate overview of 2024 budget projects: how many have been reported vs not reported.
            Returns counts and percentages for visualization.
        """
        
        # Get total unique projects in 2024 budget
        total_budget_projects = (
            self.session.query(func.count(distinct(BudgetProject2024.code)))
            .scalar() or 0
        )
        
        # Get unique ERGP codes that have been reported in survey responses
        reported_codes = (
            self.session.query(distinct(SurveyResponse.ergp_code))
            .filter(SurveyResponse.ergp_code.isnot(None))
            .filter(func.trim(SurveyResponse.ergp_code) != "")
            .all()
        )
        reported_codes_set = {code[0] for code in reported_codes if code[0]}
        
        # Count how many budget projects have matching reports
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
    """
    One place your routes can call to build dashboard payloads.
    Keeps routes clean and makes unit testing easier.
    """

    def __init__(self, session: Optional[Session] = None):
        self.session = session or db.session
        self.activity = ActivityAnalytics(self.session)
        self.quality = QualityAnalytics(self.session)
        self.performance = PerformanceAnalytics(self.session)

    def dashboard_overview(self) -> Dict[str, Any]:
        """
        A starter payload you can expand. Keep it fast.
        """
        return {
            "latest_responders": self.activity.latest_responding_agencies(limit=20),
            "activity_30d": self.activity.activity_summary_by_mda(window_days=30),
            "evidence_coverage": self.quality.evidence_coverage_by_mda(window_days=30),
            "quality_flags": self.quality.data_quality_flags_by_mda(limit=200),
            "performance_table": self.performance.mda_performance_table(),
            "budget_reporting": self.performance.budget_reporting_overview(),
        }
