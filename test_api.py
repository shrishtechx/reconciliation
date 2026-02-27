"""Quick end-to-end API test script — covers re-upload flow."""
import requests
import json

base = "http://localhost:8000"

# Test 1: Load sample data (first upload)
print("=== Test 1: Load Sample Data (1st upload) ===")
r = requests.post(f"{base}/api/sample")
assert r.status_code == 200
d = r.json()
print(f"  A: {d['file_a']} ({d['rows_a']} rows) | B: {d['file_b']} ({d['rows_b']} rows)")
first_rows_a = d["rows_a"]

# Test 2: Reconcile (1st)
print("\n=== Test 2: Reconcile (1st) ===")
r = requests.post(f"{base}/api/reconcile")
assert r.status_code == 200
d = r.json()
first_matched = d["matched_count"]
first_exceptions = d["exception_count"]
print(f"  Matched: {first_matched}, Exceptions: {first_exceptions}")

# Test 3: Full results (1st)
print("\n=== Test 3: Full Results (1st) ===")
r = requests.get(f"{base}/api/results")
assert r.status_code == 200
d = r.json()
assert len(d["matched"]) == first_matched
raw = json.dumps(d)
assert "NaN" not in raw, "NaN found in JSON!"
print(f"  {len(d['matched'])} matched, {len(d['exceptions'])} exceptions, 0 NaN")

# Test 4: Re-upload sample (simulates uploading different company data)
print("\n=== Test 4: Re-upload Sample (2nd upload) ===")
r = requests.post(f"{base}/api/sample")
assert r.status_code == 200
d = r.json()
print(f"  A: {d['file_a']} ({d['rows_a']} rows)")

# Test 5: Old results should be cleared
print("\n=== Test 5: Old results cleared? ===")
r = requests.get(f"{base}/api/results")
print(f"  Status: {r.status_code} (expect 400 since results cleared)")
assert r.status_code == 400, f"Expected 400, got {r.status_code}"

# Test 6: Reconcile again (2nd - should use NEW data)
print("\n=== Test 6: Reconcile (2nd) ===")
r = requests.post(f"{base}/api/reconcile")
assert r.status_code == 200
d = r.json()
second_matched = d["matched_count"]
second_exceptions = d["exception_count"]
print(f"  Matched: {second_matched}, Exceptions: {second_exceptions}")

# Test 7: Full results (2nd) — verify data integrity
print("\n=== Test 7: Full Results (2nd) ===")
r = requests.get(f"{base}/api/results")
assert r.status_code == 200
d = r.json()
assert len(d["matched"]) == second_matched
raw = json.dumps(d)
assert "NaN" not in raw, "NaN found in JSON!"
print(f"  {len(d['matched'])} matched, {len(d['exceptions'])} exceptions, 0 NaN")

# Test 8: Preview shows normalized
print("\n=== Test 8: Preview (Normalized) ===")
r = requests.get(f"{base}/api/preview")
assert r.status_code == 200
d = r.json()
print(f"  A: {d['company_a']['name']} ({d['company_a']['rows']} rows)")
assert "Normalized" in d["company_a"]["name"]

# Test 9: Config get/update
print("\n=== Test 9: Config ===")
r = requests.get(f"{base}/api/config")
assert r.status_code == 200
cfg = r.json()
print(f"  date_tolerance_days: {cfg['date_tolerance_days']}")
r = requests.put(f"{base}/api/config", json={"date_tolerance_days": 10})
assert r.status_code == 200
cfg2 = r.json()
assert cfg2["date_tolerance_days"] == 10
print(f"  Updated to: {cfg2['date_tolerance_days']}")

# Test 10: Report download
print("\n=== Test 10: Report Download ===")
r = requests.get(f"{base}/api/report")
assert r.status_code == 200
assert len(r.content) > 1000
print(f"  Report size: {len(r.content)} bytes")

# Test 11: Reset
print("\n=== Test 11: Reset ===")
r = requests.post(f"{base}/api/reset")
assert r.status_code == 200
r = requests.get(f"{base}/api/results")
assert r.status_code == 400
print(f"  Reset OK, results return 400")

print("\n=== ALL 11 TESTS PASSED ===")
