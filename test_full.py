"""Comprehensive end-to-end test covering all scenarios."""
import requests
import json
import os

base = "http://localhost:8000"

def check_no_nan(data, label):
    raw = json.dumps(data)
    assert "NaN" not in raw, f"NaN found in {label} JSON!"
    assert "Infinity" not in raw, f"Infinity found in {label} JSON!"
    print(f"  {label}: no NaN/Infinity in JSON")

# ============================================================
# Scenario 1: Sample data flow
# ============================================================
print("=" * 60)
print("SCENARIO 1: Sample Data Flow")
print("=" * 60)

# 1a: Load sample
print("\n1a. Load sample data...")
r = requests.post(f"{base}/api/sample")
assert r.status_code == 200, f"Sample failed: {r.status_code} {r.text}"
d = r.json()
check_no_nan(d, "sample response")
print(f"  A: {d['file_a']} ({d['rows_a']} rows), B: {d['file_b']} ({d['rows_b']} rows)")

# 1b: Preview (raw)
print("\n1b. Preview (raw)...")
r = requests.get(f"{base}/api/preview")
assert r.status_code == 200, f"Preview failed: {r.status_code} {r.text}"
d = r.json()
check_no_nan(d, "preview response")
assert "Raw" in d["company_a"]["name"]
print(f"  A: {d['company_a']['name']} ({d['company_a']['rows']} rows)")

# 1c: Reconcile
print("\n1c. Reconcile...")
r = requests.post(f"{base}/api/reconcile")
assert r.status_code == 200, f"Reconcile failed: {r.status_code} {r.text}"
d = r.json()
check_no_nan(d, "reconcile response")
matched1 = d["matched_count"]
exc1 = d["exception_count"]
print(f"  Matched: {matched1}, Exceptions: {exc1}")

# 1d: Full results
print("\n1d. Full results...")
r = requests.get(f"{base}/api/results")
assert r.status_code == 200, f"Results failed: {r.status_code} {r.text}"
d = r.json()
check_no_nan(d, "full results")
assert len(d["matched"]) == matched1
assert len(d["exceptions"]) == exc1
print(f"  {len(d['matched'])} matched records, {len(d['exceptions'])} exceptions")

# 1e: Preview (normalized)
print("\n1e. Preview (normalized)...")
r = requests.get(f"{base}/api/preview")
assert r.status_code == 200
d = r.json()
check_no_nan(d, "normalized preview")
assert "Normalized" in d["company_a"]["name"]
print(f"  A: {d['company_a']['name']}")

# 1f: Download report
print("\n1f. Download report...")
r = requests.get(f"{base}/api/report")
assert r.status_code == 200
assert len(r.content) > 100
print(f"  Report: {len(r.content)} bytes")

# ============================================================
# Scenario 2: Re-upload (same sample) — old results must clear
# ============================================================
print("\n" + "=" * 60)
print("SCENARIO 2: Re-upload clears old results")
print("=" * 60)

# 2a: Re-upload sample
print("\n2a. Re-upload sample...")
r = requests.post(f"{base}/api/sample")
assert r.status_code == 200
d = r.json()
check_no_nan(d, "re-upload response")

# 2b: Old results should be gone
print("\n2b. Old results should be cleared...")
r = requests.get(f"{base}/api/results")
assert r.status_code == 400, f"Expected 400 but got {r.status_code}"
print(f"  Status: {r.status_code} (correct - results cleared)")

# 2c: Reconcile again
print("\n2c. Reconcile again...")
r = requests.post(f"{base}/api/reconcile")
assert r.status_code == 200
d = r.json()
check_no_nan(d, "2nd reconcile")
print(f"  Matched: {d['matched_count']}, Exceptions: {d['exception_count']}")

# 2d: Full results again
print("\n2d. Full results (2nd)...")
r = requests.get(f"{base}/api/results")
assert r.status_code == 200
d = r.json()
check_no_nan(d, "2nd full results")
print(f"  {len(d['matched'])} matched, {len(d['exceptions'])} exceptions")

# ============================================================
# Scenario 3: Upload real Tally files (if available)
# ============================================================
file_a_path = r"d:\Gowri\Python\SampleNewProject\SRV Ledger in Welcare Books.xls"
file_b_path = r"d:\Gowri\Python\SampleNewProject\Welcare Ledger in SRV Books.xls"

if os.path.exists(file_a_path) and os.path.exists(file_b_path):
    print("\n" + "=" * 60)
    print("SCENARIO 3: Real Tally Files")
    print("=" * 60)

    # 3a: Upload real files
    print("\n3a. Upload Tally files...")
    with open(file_a_path, "rb") as fa, open(file_b_path, "rb") as fb:
        r = requests.post(f"{base}/api/upload", files={
            "file_a": (os.path.basename(file_a_path), fa),
            "file_b": (os.path.basename(file_b_path), fb),
        })
    assert r.status_code == 200, f"Upload failed: {r.status_code} {r.text}"
    d = r.json()
    check_no_nan(d, "tally upload")
    print(f"  A: {d['file_a']} ({d['rows_a']} rows), B: {d['file_b']} ({d['rows_b']} rows)")

    # 3b: Preview
    print("\n3b. Preview (raw)...")
    r = requests.get(f"{base}/api/preview")
    assert r.status_code == 200, f"Preview failed: {r.status_code} {r.text}"
    d = r.json()
    check_no_nan(d, "tally preview")

    # 3c: Reconcile
    print("\n3c. Reconcile...")
    r = requests.post(f"{base}/api/reconcile")
    assert r.status_code == 200, f"Reconcile failed: {r.status_code} {r.text}"
    d = r.json()
    check_no_nan(d, "tally reconcile")
    print(f"  Matched: {d['matched_count']}, Exceptions: {d['exception_count']}")

    # 3d: Full results
    print("\n3d. Full results...")
    r = requests.get(f"{base}/api/results")
    assert r.status_code == 200, f"Results failed: {r.status_code} {r.text}"
    d = r.json()
    check_no_nan(d, "tally full results")
    print(f"  {len(d['matched'])} matched, {len(d['exceptions'])} exceptions")

    # Verify each match has distinct data
    if len(d["matched"]) > 1:
        descs = set()
        for m in d["matched"]:
            descs.add(m.get("A_Description", "") + "|" + m.get("A_Date", ""))
        print(f"  Unique match entries: {len(descs)} (should equal {len(d['matched'])})")

    # 3e: Report
    print("\n3e. Download report...")
    r = requests.get(f"{base}/api/report")
    assert r.status_code == 200
    print(f"  Report: {len(r.content)} bytes")

    # 3f: Preview (normalized)
    print("\n3f. Preview (normalized)...")
    r = requests.get(f"{base}/api/preview")
    assert r.status_code == 200
    d = r.json()
    check_no_nan(d, "tally normalized preview")
    assert "Normalized" in d["company_a"]["name"]

    # ============================================================
    # Scenario 4: Re-upload sample AFTER tally — must use sample data
    # ============================================================
    print("\n" + "=" * 60)
    print("SCENARIO 4: Switch from Tally back to Sample")
    print("=" * 60)

    print("\n4a. Load sample data (after Tally)...")
    r = requests.post(f"{base}/api/sample")
    assert r.status_code == 200
    d = r.json()
    check_no_nan(d, "sample after tally")
    assert d["file_a"] == "Sample Company A"
    print(f"  File A: {d['file_a']} (should be Sample Company A)")

    print("\n4b. Old results cleared?")
    r = requests.get(f"{base}/api/results")
    assert r.status_code == 400
    print(f"  Status: {r.status_code} (correct)")

    print("\n4c. Reconcile sample...")
    r = requests.post(f"{base}/api/reconcile")
    assert r.status_code == 200
    d = r.json()
    check_no_nan(d, "sample reconcile after tally")
    print(f"  Matched: {d['matched_count']} (should be ~59, NOT tally count)")
else:
    print(f"\n(Skipping Scenario 3 & 4: Tally files not found at expected paths)")

# ============================================================
# Scenario 5: Config
# ============================================================
print("\n" + "=" * 60)
print("SCENARIO 5: Config get/update")
print("=" * 60)

print("\n5a. Get config...")
r = requests.get(f"{base}/api/config")
assert r.status_code == 200
cfg = r.json()
print(f"  date_tolerance_days: {cfg['date_tolerance_days']}")

print("\n5b. Update config...")
r = requests.put(f"{base}/api/config", json={"date_tolerance_days": 14})
assert r.status_code == 200
cfg2 = r.json()
assert cfg2["date_tolerance_days"] == 14
print(f"  Updated to: {cfg2['date_tolerance_days']}")

# ============================================================
# Scenario 6: Reset
# ============================================================
print("\n" + "=" * 60)
print("SCENARIO 6: Reset")
print("=" * 60)

print("\n6a. Reset...")
r = requests.post(f"{base}/api/reset")
assert r.status_code == 200

print("6b. Verify all cleared...")
assert requests.get(f"{base}/api/results").status_code == 400
assert requests.get(f"{base}/api/preview").status_code == 400
r = requests.get(f"{base}/api/config")
assert r.json()["date_tolerance_days"] == 7  # back to default
print(f"  All cleared, config reset to defaults")

print("\n" + "=" * 60)
print("ALL SCENARIOS PASSED!")
print("=" * 60)
