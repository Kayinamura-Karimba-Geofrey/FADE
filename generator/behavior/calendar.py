"""Calendar effects that modulate daily event counts.

All multipliers are applied to the Poisson means of the segment profile
before drawing event counts:

* **festive season** - Dec 15 to Jan 5: x1.6 for everybody
* **month-end** - last two days of a month: x1.15
* **market days** - Tuesday/Friday: x1.35 for traders (their own
  weekday multiplier in the profile, additionally x1.1 on those days)
* **public holidays** - Rwanda public holidays dampen salaried activity
  (x0.55) because shops and salary runs pause
* **weekends** - segment weekday multipliers from the profile (students and
  urban youth are more active on Sat/Sun)

Holidays are fixed (month, day) approximations - Easter moves every year
and is intentionally not modelled; this is documented in docs/phase-1.md.
"""

from __future__ import annotations

from datetime import date

from generator.behavior.profiles import SegmentProfile

#: Fixed Rwanda public holidays as (month, day).
PUBLIC_HOLIDAYS: tuple[tuple[int, int], ...] = (
    (1, 1),  # New Year's Day
    (2, 1),  # National Heroes' Day
    (4, 7),  # Genocide Memorial Day
    (5, 1),  # Labour Day
    (7, 1),  # Independence Day
    (8, 15),  # Assumption Day
    (12, 25),  # Christmas Day
)

FESTIVE_MULTIPLIER = 1.6
MONTH_END_MULTIPLIER = 1.15
MARKET_DAY = 1  # Tuesday (Mon=0)
SECOND_MARKET_DAY = 4  # Friday
HOLIDAY_DAMPEN_SALARIED = 0.55


def is_festive(day: date) -> bool:
    """Dec 15 - Jan 5 inclusive."""
    return (day.month == 12 and day.day >= 15) or (day.month == 1 and day.day <= 5)


def is_public_holiday(day: date) -> bool:
    return (day.month, day.day) in PUBLIC_HOLIDAYS


def is_month_end(day: date) -> bool:
    """Last two days of the month."""
    from calendar import monthrange

    last = monthrange(day.year, day.month)[1]
    return day.day >= last - 1


def day_multiplier(day: date, profile: SegmentProfile) -> float:
    """Combined count multiplier for one account-day of a segment."""
    factor = 1.0
    if is_festive(day):
        factor *= FESTIVE_MULTIPLIER
    if is_month_end(day):
        factor *= MONTH_END_MULTIPLIER
    weekday = day.weekday()
    factor *= profile.weekday_multipliers.get(weekday, 1.0)
    if profile.name == "small_trader" and weekday in (MARKET_DAY, SECOND_MARKET_DAY):
        factor *= 1.10
    if is_public_holiday(day) and profile.name == "salaried":
        factor *= HOLIDAY_DAMPEN_SALARIED
    return factor


def is_salary_window(day: date) -> bool:
    """Salary runs land on days 25-28 of each month."""
    return 25 <= day.day <= 28


def is_term_window(day: date) -> bool:
    """School terms start Jan/May/Sep; fees arrive in the first 10 days."""
    return day.month in (1, 5, 9) and day.day <= 10


def is_weekend(day: date) -> bool:
    return day.weekday() >= 5
