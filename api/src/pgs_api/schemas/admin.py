"""`/api/v1/admin/*` responses (pgs_db's dashboard schemas, extended)."""

from __future__ import annotations

from datetime import datetime

from pgs_db.schemas import DashboardSummary, SearchTraffic


class SearchTrafficWindow(SearchTraffic):
    since: datetime
    until: datetime


class AdminSummary(DashboardSummary):
    """`StatsRepository.dashboard_summary` plus the last 24 hours of search traffic."""

    search_traffic: SearchTrafficWindow
