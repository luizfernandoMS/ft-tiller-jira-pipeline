import requests
from datetime import timedelta, datetime
import pandas as pd

holiday_cache = {}

def expected_sla_hours(priority: str) -> int:
    """
    Return the expected SLA hours based on the issue priority.
    """
    sla_rules = {
        "High": 24,
        "Medium": 72,
        "Low": 120
    }
    return sla_rules.get(priority, None)

def resolution_time_hours(created_at, resolved_at):
    """
    Calculate the resolution time in hours between created_at and resolved_at.
    """
    return business_hours_between(created_at, resolved_at)

def sla_met(resolution_hours, expected_hours):
    """
    Determine if the SLA was met based on resolution hours and expected hours.
    """
    if resolution_hours is None or expected_hours is None:
        return None
    return resolution_hours <= expected_hours

def is_weekend(date):
    """
    Check if a given date falls on a weekend.
    """
    return date.weekday() >= 5  # 5 = Saturday, 6 = Sunday

def business_hours_between(start, end):
    """
    Calculate the number of business hours between two timestamps, excluding weekends.
    """

    if pd.isna(start) or pd.isna(end):
        return None

    if end <= start:
        return 0

    total_hours = 0
    current = start

    # pegar feriados dos anos envolvidos
    years = set([start.year, end.year])
    holidays = set()

    for year in years:
        holidays.update(get_national_holidays(year))

    while current.date() <= end.date():

        current_date = current.date()

        if (
            not is_weekend(current)
            and current_date not in holidays
        ):

            if current_date == start.date():
                day_start = start
            else:
                day_start = pd.Timestamp(current_date, tz="UTC")

            if current_date == end.date():
                day_end = end
            else:
                day_end = pd.Timestamp(current_date, tz="UTC") + timedelta(days=1)

            delta = day_end - day_start
            total_hours += delta.total_seconds() / 3600

        current += timedelta(days=1)

    return total_hours

def get_national_holidays(year):

    if year in holiday_cache:
        return holiday_cache[year]

    url = f"https://brasilapi.com.br/api/feriados/v1/{year}"

    response = requests.get(url, timeout=10)
    response.raise_for_status()

    holidays = {
        datetime.strptime(item["date"], "%Y-%m-%d").date()
        for item in response.json()
    }

    holiday_cache[year] = holidays

    return holidays