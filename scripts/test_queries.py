#!/usr/bin/env python3
"""
Pre-Phase 3 Validation: Runtime Test Script
============================================
Imports and calls all query functions from health_queries.py, music_queries.py.
Verifies they return expected data types and handle empty results gracefully.
Records any errors for the validation report.
"""
import sys
import os
import time
import json
import inspect
from datetime import datetime

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

# Load .env
env_path = os.path.join(project_root, 'backend', '.env')
if os.path.exists(env_path):
    from dotenv import load_dotenv
    load_dotenv(env_path)

from backend_functions.database_functions import sql_to_dict, qec

# ============================================================
# Test Results Collector
# ============================================================
test_results = {
    "generated_at": datetime.now().isoformat(),
    "health_queries": [],
    "music_queries": [],
    "summary": {"total": 0, "passed": 0, "failed": 0, "errors": []}
}

def record_test(category, name, passed, details, execution_time_ms=None):
    result = {
        "name": name,
        "passed": passed,
        "details": details,
        "execution_time_ms": execution_time_ms
    }
    test_results[category].append(result)
    test_results["summary"]["total"] += 1
    if passed:
        test_results["summary"]["passed"] += 1
        status = "✅ PASS"
    else:
        test_results["summary"]["failed"] += 1
        test_results["summary"]["errors"].append(f"{category}.{name}: {details}")
        status = "❌ FAIL"
    time_str = f" [{execution_time_ms:.1f}ms]" if execution_time_ms else ""
    print(f"  {status}: {name}{time_str}")
    if not passed:
        print(f"    Error: {details}")

# ============================================================
# Task 1a: Health Query Tests
# ============================================================
print("\n" + "=" * 60)
print("HEALTH QUERY TESTS")
print("=" * 60)

from backend_functions.queries.health_queries import (
    get_weight_targets,
    add_weight_target,
    get_weight_viz_data,
    add_photo_metadata,
    add_body_dimensions
)

# Test 1: get_weight_targets
print("\n--- get_weight_targets ---")
try:
    t0 = time.time()
    result = get_weight_targets()
    elapsed = (time.time() - t0) * 1000
    assert isinstance(result, (list, tuple)), f"Expected list/tuple, got {type(result)}"
    if result:
        assert isinstance(result[0], dict), f"Expected dict items, got {type(result[0])}"
        assert 'ts_utc' in result[0], "Missing 'ts_utc' key"
        assert 'weight_lb' in result[0], "Missing 'weight_lb' key"
    record_test("health_queries", "get_weight_targets", True,
                f"Returned {len(result)} records", elapsed)
except Exception as e:
    record_test("health_queries", "get_weight_targets", False, str(e))

# Test 2: get_weight_viz_data
print("\n--- get_weight_viz_data ---")
try:
    t0 = time.time()
    result = get_weight_viz_data('day_of_year', 'relative_year', ['total_lb', 'tgt_lb'], 2)
    elapsed = (time.time() - t0) * 1000
    assert isinstance(result, (list, tuple)), f"Expected list/tuple, got {type(result)}"
    record_test("health_queries", "get_weight_viz_data", True,
                f"Returned {len(result)} records", elapsed)
except Exception as e:
    record_test("health_queries", "get_weight_viz_data", False, str(e))

# Test 3: add_weight_target (read-only test - verify function exists and has correct signature)
print("\n--- add_weight_target (signature check) ---")
try:
    sig = inspect.signature(add_weight_target)
    params = list(sig.parameters.keys())
    assert 'date' in params, "Missing 'date' param"
    assert 'weight_lb' in params, "Missing 'weight_lb' param"
    record_test("health_queries", "add_weight_target_signature", True,
                f"Signature: add_weight_target({', '.join(params)})")
except Exception as e:
    record_test("health_queries", "add_weight_target_signature", False, str(e))

# Test 4: add_photo_metadata (signature check)
print("\n--- add_photo_metadata (signature check) ---")
try:
    sig = inspect.signature(add_photo_metadata)
    params = list(sig.parameters.keys())
    assert 'photo_type' in params, "Missing 'photo_type' param"
    assert 'file_name' in params, "Missing 'file_name' param"
    record_test("health_queries", "add_photo_metadata_signature", True,
                f"Signature: add_photo_metadata({', '.join(params)})")
except Exception as e:
    record_test("health_queries", "add_photo_metadata_signature", False, str(e))

# Test 5: add_body_dimensions (signature check)
print("\n--- add_body_dimensions (signature check) ---")
try:
    sig = inspect.signature(add_body_dimensions)
    params = list(sig.parameters.keys())
    expected = ['butt_cm', 'waist_cm', 'stomach_cm', 'chest_cm', 'neck_cm']
    for p in expected:
        assert p in params, f"Missing '{p}' param"
    record_test("health_queries", "add_body_dimensions_signature", True,
                f"Signature: add_body_dimensions({', '.join(params)})")
except Exception as e:
    record_test("health_queries", "add_body_dimensions_signature", False, str(e))

# ============================================================
# Task 1b: Music Query Tests
# ============================================================
print("\n" + "=" * 60)
print("MUSIC QUERY TESTS")
print("=" * 60)

from backend_functions.queries.music_queries import (
    get_rating_eligible_count,
    get_isrc_dupe_count,
    get_isrc_dupe_match,
    get_playlist_config,
    get_playlist_isrc_stats,
    get_recent_plays,
    get_rating_eligible_playlists,
    get_playlists_not_containing_isrc,
    process_isrc_dupe_acceptance,
    add_isrc_to_local_playlist,
    record_recommendation_decision,
    remove_recommendation,
    add_into_current_ratings,
    update_playlist_config_weights,
    record_rating_history,
    update_ratings_from_view,
    add_soft_rejection_exclusion
)

# Test 6: get_rating_eligible_count
print("\n--- get_rating_eligible_count ---")
try:
    t0 = time.time()
    count = get_rating_eligible_count()
    elapsed = (time.time() - t0) * 1000
    assert isinstance(count, (int, float)), f"Expected int/float, got {type(count)}"
    assert count >= 0, f"Expected non-negative, got {count}"
    record_test("music_queries", "get_rating_eligible_count", True,
                f"Count: {count}", elapsed)
except Exception as e:
    record_test("music_queries", "get_rating_eligible_count", False, str(e))

# Test 7: get_isrc_dupe_count
print("\n--- get_isrc_dupe_count ---")
try:
    t0 = time.time()
    count = get_isrc_dupe_count()
    elapsed = (time.time() - t0) * 1000
    assert isinstance(count, (int, float)), f"Expected int/float, got {type(count)}"
    assert count >= 0, f"Expected non-negative, got {count}"
    record_test("music_queries", "get_isrc_dupe_count", True,
                f"Count: {count}", elapsed)
except Exception as e:
    record_test("music_queries", "get_isrc_dupe_count", False, str(e))

# Test 8: get_isrc_dupe_match
print("\n--- get_isrc_dupe_match ---")
try:
    t0 = time.time()
    result = get_isrc_dupe_match()
    elapsed = (time.time() - t0) * 1000
    assert isinstance(result, (list, tuple)), f"Expected list/tuple, got {type(result)}"
    record_test("music_queries", "get_isrc_dupe_match", True,
                f"Returned {len(result)} records", elapsed)
except Exception as e:
    record_test("music_queries", "get_isrc_dupe_match", False, str(e))

# Test 9: get_playlist_config
print("\n--- get_playlist_config ---")
try:
    # First get a real playlist_id
    playlists = sql_to_dict("SELECT playlist_id FROM music.playlist_config LIMIT 1")
    if playlists:
        test_id = playlists[0]['playlist_id']
        t0 = time.time()
        result = get_playlist_config(test_id)
        elapsed = (time.time() - t0) * 1000
        assert isinstance(result, (list, tuple)), f"Expected list/tuple, got {type(result)}"
        if result:
            # Check all 17 fields are present
            expected_fields = ['playlist_id', 'playlist_name', 'playlist_description', 'track_count',
                             'last_verified_utc', 'is_active', 'auto_shuffle', 'last_auto_shuffled_utc',
                             'last_synced_utc', 'make_recs', 'manual_shuffle', 'minutes_to_sync',
                             'prior_track_count', 'randomness_weight', 'ratings_weight', 'recency_weight',
                             'seeds_only']
            missing = [f for f in expected_fields if f not in result[0]]
            assert not missing, f"Missing fields: {missing}"
        record_test("music_queries", "get_playlist_config", True,
                    f"Returned {len(result)} records for playlist {test_id}", elapsed)
    else:
        record_test("music_queries", "get_playlist_config", True,
                    "No playlists found in database (empty table)")
except Exception as e:
    record_test("music_queries", "get_playlist_config", False, str(e))

# Test 10: get_playlist_isrc_stats
print("\n--- get_playlist_isrc_stats ---")
try:
    playlists = sql_to_dict("SELECT playlist_id FROM music.playlist_config LIMIT 1")
    if playlists:
        test_id = playlists[0]['playlist_id']
        t0 = time.time()
        result = get_playlist_isrc_stats(test_id)
        elapsed = (time.time() - t0) * 1000
        assert isinstance(result, (list, tuple)), f"Expected list/tuple, got {type(result)}"
        record_test("music_queries", "get_playlist_isrc_stats", True,
                    f"Returned {len(result)} records", elapsed)
    else:
        record_test("music_queries", "get_playlist_isrc_stats", True,
                    "No playlists found (empty table)")
except Exception as e:
    record_test("music_queries", "get_playlist_isrc_stats", False, str(e))

# Test 11: get_recent_plays
print("\n--- get_recent_plays ---")
try:
    t0 = time.time()
    result = get_recent_plays(limit=5)
    elapsed = (time.time() - t0) * 1000
    assert isinstance(result, (list, tuple)), f"Expected list/tuple, got {type(result)}"
    assert len(result) <= 5, f"Expected <=5 records, got {len(result)}"
    record_test("music_queries", "get_recent_plays", True,
                f"Returned {len(result)} records (limit=5)", elapsed)
except Exception as e:
    record_test("music_queries", "get_recent_plays", False, str(e))

# Test 12: get_rating_eligible_playlists
print("\n--- get_rating_eligible_playlists ---")
try:
    t0 = time.time()
    result = get_rating_eligible_playlists()
    elapsed = (time.time() - t0) * 1000
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"
    record_test("music_queries", "get_rating_eligible_playlists", True,
                f"Returned {len(result)} playlists", elapsed)
except Exception as e:
    record_test("music_queries", "get_rating_eligible_playlists", False, str(e))

# Test 13: get_playlists_not_containing_isrc
print("\n--- get_playlists_not_containing_isrc ---")
try:
    t0 = time.time()
    result = get_playlists_not_containing_isrc("USABC1234567")
    elapsed = (time.time() - t0) * 1000
    assert isinstance(result, (list, tuple)), f"Expected list/tuple, got {type(result)}"
    record_test("music_queries", "get_playlists_not_containing_isrc", True,
                f"Returned {len(result)} playlists", elapsed)
except Exception as e:
    record_test("music_queries", "get_playlists_not_containing_isrc", False, str(e))

# Test 14: add_isrc_to_local_playlist (signature check only - would modify data)
print("\n--- add_isrc_to_local_playlist (signature check) ---")
try:
    sig = inspect.signature(add_isrc_to_local_playlist)
    params = list(sig.parameters.keys())
    assert 'playlist_id' in params, "Missing 'playlist_id' param"
    assert 'isrc' in params, "Missing 'isrc' param"
    record_test("music_queries", "add_isrc_to_local_playlist_signature", True,
                f"Signature: add_isrc_to_local_playlist({', '.join(params)})")
except Exception as e:
    record_test("music_queries", "add_isrc_to_local_playlist_signature", False, str(e))

# Test 15: record_recommendation_decision (signature check)
print("\n--- record_recommendation_decision (signature check) ---")
try:
    sig = inspect.signature(record_recommendation_decision)
    params = list(sig.parameters.keys())
    assert 'playlist_id' in params, "Missing 'playlist_id' param"
    assert 'isrc' in params, "Missing 'isrc' param"
    assert 'was_promoted' in params, "Missing 'was_promoted' param"
    record_test("music_queries", "record_recommendation_decision_signature", True,
                f"Signature: record_recommendation_decision({', '.join(params)})")
except Exception as e:
    record_test("music_queries", "record_recommendation_decision_signature", False, str(e))

# Test 16: remove_recommendation (signature check)
print("\n--- remove_recommendation (signature check) ---")
try:
    sig = inspect.signature(remove_recommendation)
    params = list(sig.parameters.keys())
    assert 'playlist_id' in params, "Missing 'playlist_id' param"
    assert 'isrc' in params, "Missing 'isrc' param"
    record_test("music_queries", "remove_recommendation_signature", True,
                f"Signature: remove_recommendation({', '.join(params)})")
except Exception as e:
    record_test("music_queries", "remove_recommendation_signature", False, str(e))

# Test 17: add_into_current_ratings (signature check)
print("\n--- add_into_current_ratings (signature check) ---")
try:
    sig = inspect.signature(add_into_current_ratings)
    params = list(sig.parameters.keys())
    assert 'playlist_id' in params, "Missing 'playlist_id' param"
    assert 'isrc' in params, "Missing 'isrc' param"
    assert 'current_elo' in params, "Missing 'current_elo' param"
    record_test("music_queries", "add_into_current_ratings_signature", True,
                f"Signature: add_into_current_ratings({', '.join(params)})")
except Exception as e:
    record_test("music_queries", "add_into_current_ratings_signature", False, str(e))

# Test 18: update_playlist_config_weights (signature check)
print("\n--- update_playlist_config_weights (signature check) ---")
try:
    sig = inspect.signature(update_playlist_config_weights)
    params = list(sig.parameters.keys())
    expected = ['playlist_id', 'target_playlist_id', 'ratings_weight', 'recency_weight', 'randomness_weight', 'minutes_to_sync']
    for p in expected:
        assert p in params, f"Missing '{p}' param"
    record_test("music_queries", "update_playlist_config_weights_signature", True,
                f"Signature: update_playlist_config_weights({', '.join(params)})")
except Exception as e:
    record_test("music_queries", "update_playlist_config_weights_signature", False, str(e))

# Test 19: record_rating_history (signature check)
print("\n--- record_rating_history (signature check) ---")
try:
    sig = inspect.signature(record_rating_history)
    params = list(sig.parameters.keys())
    expected = ['playlist_id', 'isrc', 'isrc_vs', 'isrc_elo', 'isrc_vs_elo', 'home_new_elo', 'away_new_elo', 'margin']
    for p in expected:
        assert p in params, f"Missing '{p}' param"
    record_test("music_queries", "record_rating_history_signature", True,
                f"Signature: record_rating_history({', '.join(params)})")
except Exception as e:
    record_test("music_queries", "record_rating_history_signature", False, str(e))

# Test 20: update_ratings_from_view (signature check)
print("\n--- update_ratings_from_view (signature check) ---")
try:
    sig = inspect.signature(update_ratings_from_view)
    params = list(sig.parameters.keys())
    assert len(params) == 0, f"Expected no params, got {params}"
    record_test("music_queries", "update_ratings_from_view_signature", True,
                "Signature: update_ratings_from_view()")
except Exception as e:
    record_test("music_queries", "update_ratings_from_view_signature", False, str(e))

# Test 21: add_soft_rejection_exclusion (signature check)
print("\n--- add_soft_rejection_exclusion (signature check) ---")
try:
    sig = inspect.signature(add_soft_rejection_exclusion)
    params = list(sig.parameters.keys())
    expected = ['playlist_id', 'isrc', 'current_elo']
    for p in expected:
        assert p in params, f"Missing '{p}' param"
    record_test("music_queries", "add_soft_rejection_exclusion_signature", True,
                f"Signature: add_soft_rejection_exclusion({', '.join(params)})")
except Exception as e:
    record_test("music_queries", "add_soft_rejection_exclusion_signature", False, str(e))

# ============================================================
# Summary
# ============================================================
print("\n" + "=" * 60)
print("TEST SUMMARY")
print("=" * 60)
s = test_results["summary"]
print(f"  Total:  {s['total']}")
print(f"  Passed: {s['passed']}")
print(f"  Failed: {s['failed']}")
if s['errors']:
    print(f"\n  Errors:")
    for err in s['errors']:
        print(f"    - {err}")

# Save results
output_path = os.path.join(project_root, 'memory-bank', 'query_test_results.json')
with open(output_path, 'w') as f:
    json.dump(test_results, f, indent=2, default=str)
print(f"\nResults saved to {output_path}")

# Exit with error code if any tests failed
sys.exit(0 if s['failed'] == 0 else 1)