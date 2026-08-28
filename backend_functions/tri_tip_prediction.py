"""
Tri-tip Prediction / ETA Service
================================

Computes the `prediction` object of the cross-surface contract (OQ-1):

    {
      "projected_done_at":   ISO time-of-day meat is projected to reach target temp,
      "minutes_remaining":   relative minutes between now and projected_done_at,
      "curve":               [{ "grill_min": float, "internal_temp_f": float }, ...]
    }

Model (OQ-1 decision):
- Present from initiation (before the meat is placed).
- Shape-aware: the projected rate is blended with the historical mean heating
  rate of prior completed events of the same shape, so all prior data points
  influence each new prediction (learns over time).
- Fallback ordering from least data to most:
    1. Zero readings (initiated): ~15 minutes per pound total cook time.
    2. One reading only: no per-event curve fit -> use historical shape rate,
       else the nominal 15 min/lb rate.
    3. >=2 readings: least-squares fit on (grill_min, internal_temp_f),
       blended with the shape prior.

Pure app-layer computation over a tiny bounded reading set — no SQL regression,
no pandas.
"""

from datetime import datetime, timedelta, timezone
from statistics import mean
from typing import Any, Dict, List, Optional, Sequence


# Nominal fallback minutes per pound when no data exists (OQ-1).
FALLBACK_MINUTES_PER_LB = 15.0
# Starting internal temp at placement (matches backend constant 38F).
START_TEMP_F = 38.0
# Number of sampling points on the generated prediction curve.
CURVE_POINTS = 20


def _as_datetime(value: Any) -> datetime:
    """Coerce an ISO string or datetime into an aware datetime."""
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _elapsed_minutes(t0: datetime, t1: datetime) -> float:
    """Minutes elapsed between two datetimes (float)."""
    return (t1 - t0).total_seconds() / 60.0


def _linear_fit(points: Sequence[tuple[float, float]]) -> Optional[tuple[float, float]]:
    """
    Least-squares slope/intercept on (x, y) points: y = intercept + slope * x.
    Returns (slope, intercept) or None if fewer than 2 distinct x values.
    """
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    n = len(points)
    if n < 2:
        return None
    mx = mean(xs)
    my = mean(ys)
    denom = sum((x - mx) ** 2 for x in xs)
    if denom == 0:
        return None
    slope = sum((xs[i] - mx) * (ys[i] - my) for i in range(n)) / denom
    intercept = my - slope * mx
    return (slope, intercept)


def _shape_prior_rate(prior_events: Sequence[Dict[str, Any]], shape: str) -> Optional[float]:
    """
    Historical mean heating rate (internal °F per grill-minute) for completed
    events of `shape`, derived from first-to-last readings of each event.
    Returns the mean rate, or None if no qualifying prior event exists.
    """
    rates = []
    for prior in prior_events:
        ev = prior.get("event") or {}
        if ev.get("shape") != shape:
            continue
        readings = prior.get("readings") or []
        if len(readings) < 2:
            continue
        first = readings[0]
        last = readings[-1]
        minutes = _elapsed_minutes(_as_datetime(first["recorded_at"]), _as_datetime(last["recorded_at"]))
        if minutes <= 0:
            continue
        delta_temp = float(last["internal_temp_f"]) - float(first["internal_temp_f"])
        rate = delta_temp / minutes
        if rate > 0:
            rates.append(rate)
    return mean(rates) if rates else None


def _fallback_rate(weight_lbs: float, target: float) -> float:
    """Nominal rise rate from the 15 min/lb fallback (START_TEMP_F -> target)."""
    total_minutes = weight_lbs * FALLBACK_MINUTES_PER_LB
    return (target - START_TEMP_F) / total_minutes if total_minutes > 0 else 0.1


def _build_curve(
    start_grill_min: float,
    start_temp: float,
    end_grill_min: float,
    end_temp: float,
) -> List[Dict[str, float]]:
    """Sample the line (start -> end) into CURVE_POINTS points along grill_min."""
    pts = []
    for i in range(CURVE_POINTS):
        frac = i / (CURVE_POINTS - 1) if CURVE_POINTS > 1 else 0.0
        grill_min = start_grill_min + (end_grill_min - start_grill_min) * frac
        temp = start_temp + (end_temp - start_temp) * frac
        pts.append({"grill_min": round(grill_min, 2), "internal_temp_f": round(temp, 2)})
    return pts


def build_tri_tip_prediction(
    event: Dict[str, Any],
    readings: Sequence[Dict[str, Any]],
    prior_events: Sequence[Dict[str, Any]],
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Build the contract prediction dict for a single event.

    Args:
        event:       event row dict (as returned by tri_tip_queries).
        readings:    this event's readings (each with recorded_at/internal_temp_f).
        prior_events: prior completed events for shape-learning; each is
                      {"event": {...}, "readings": [...]}.
        now:         reference time; defaults to utcnow().
    """
    now = now or datetime.now(timezone.utc)
    target = float(event.get("target_internal_temp_f", 125.0))
    weight = float(event.get("weight_lbs", 1.0) or 1.0)
    shape = event.get("shape", "Typical")
    t0 = event.get("started_at") or (readings[0]["recorded_at"] if readings else None)
    t0_dt = _as_datetime(t0) if t0 is not None else None

    # Build (grill_min, temp) points relative to t0 when readings exist.
    points = []
    if t0_dt is not None:
        for r in readings:
            r_dt = _as_datetime(r["recorded_at"])
            points.append((_elapsed_minutes(t0_dt, r_dt), float(r["internal_temp_f"])))

    if not points:
        # Zero readings (initiated): 15 min/lb fallback total cook time.
        total_minutes = weight * FALLBACK_MINUTES_PER_LB
        projected_done_at = now + timedelta(minutes=total_minutes)
        minutes_remaining = float(total_minutes)
        curve = _build_curve(0.0, START_TEMP_F, total_minutes, target)
    else:
        prior_rate = _shape_prior_rate(prior_events, shape)
        fitted = _linear_fit(points)

        if fitted is None:
            # Single reading -> no slope fit; use historical or nominal rate.
            effective_rate = prior_rate or _fallback_rate(weight, target)
            intercept = points[0][1] - effective_rate * points[0][0]
        else:
            slope, intercept = fitted
            if prior_rate is not None:
                effective_rate = 0.5 * slope + 0.5 * prior_rate
            else:
                effective_rate = slope
            if effective_rate <= 0:
                effective_rate = prior_rate or _fallback_rate(weight, target)

        grill_min_target = (target - intercept) / effective_rate if effective_rate > 0 else weight * FALLBACK_MINUTES_PER_LB
        grill_min_target = max(grill_min_target, points[-1][0], 0.0)

        start_temp = intercept  # fit temperature at grill_min = 0
        projected_done_at = t0_dt + timedelta(minutes=grill_min_target)
        minutes_remaining = _elapsed_minutes(now, projected_done_at)
        curve = _build_curve(0.0, start_temp, grill_min_target, target)

    return {
        "projected_done_at": projected_done_at.astimezone(timezone.utc).isoformat(),
        "minutes_remaining": round(float(minutes_remaining), 1),
        "curve": curve,
    }