#!/usr/bin/env python3
import os, sys, csv, json, time, copy, math, pathlib, datetime
from typing import Any, Dict, List, Tuple
import requests

# ======== Config via env (no dry-run flags) ========
BASE_URL   = "https://dashapi.xe.works"
AUTH_PATH  = "/playdigo/auth"
LIST_DSPS  = "/playdigo/dsp"
GET_DSP    = "/playdigo/dsp/{id}"
PUT_DSP    = "/playdigo/dsp/{id}"

EMAIL     = (os.getenv("PLAYDIGO_EMAIL") or "").strip()
PASSWORD  = (os.getenv("PLAYDIGO_PASSWORD") or "").strip()
TIMEOUT   = int(os.getenv("HTTP_TIMEOUT", "30"))

# Exclusions (by DSP name, case-insensitive). Default excludes "Media.Net".
# You can override with: EXCLUDED_DSPS="Media.Net,Another DSP"
EXCLUDED_NAMES = {
    x.strip().lower()
    for x in (os.getenv("EXCLUDED_DSPS") or "Media.Net").split(",")
    if x.strip()
}

OUTDIR = pathlib.Path("outputs")
OUTDIR.mkdir(parents=True, exist_ok=True)

# ======== HTTP ========
def _req(method, url, **kw):
    return requests.request(method, url, timeout=TIMEOUT, **kw)

def auth_token() -> str:
    r = _req("POST", f"{BASE_URL}{AUTH_PATH}",
             json={"email": EMAIL, "password": PASSWORD},
             headers={"Accept":"application/json","Content-Type":"application/json"})
    if not (200 <= r.status_code < 300):
        raise RuntimeError(f"Auth failed [{r.status_code}]: {r.text[:300]}")
    data = r.json()
    tok = data.get("token") or data.get("access_token") or data.get("data",{}).get("token")
    if not tok:
        raise RuntimeError(f"Auth OK but token missing → {data}")
    return tok

def list_dsps(token: str) -> List[Dict[str, Any]]:
    r = _req("GET", f"{BASE_URL}{LIST_DSPS}",
             headers={"Authorization": f"Bearer {token}", "Accept":"application/json"})
    if not (200 <= r.status_code < 300):
        raise RuntimeError(f"GET {LIST_DSPS} failed [{r.status_code}]: {r.text[:300]}")
    j = r.json()
    return j if isinstance(j, list) else (j.get("data") or j.get("items") or [])

def get_detail(dsp_id: int, token: str) -> Dict[str, Any]:
    r = _req("GET", f"{BASE_URL}{GET_DSP.format(id=dsp_id)}",
             headers={"Authorization": f"Bearer {token}", "Accept":"application/json"})
    if not (200 <= r.status_code < 300):
        raise RuntimeError(f"GET /dsp/{dsp_id} failed [{r.status_code}]: {r.text[:300]}")
    j = r.json()
    return j.get("data", j)

def put_update(dsp_id: int, payload: Dict[str, Any], token: str) -> requests.Response:
    return _req("PUT", f"{BASE_URL}{PUT_DSP.format(id=dsp_id)}", json=payload,
                headers={"Authorization": f"Bearer {token}",
                         "Accept":"application/json", "Content-Type":"application/json"})

# ======== payload hardening ========
def ensure_inventory(obj: dict) -> dict:
    inv = obj.get("Inventory") or {}
    allowed = inv.get("allowed") or {}
    blocked = inv.get("blocked") or {}
    def arr(x): return x if isinstance(x, list) else []
    return {
        "allowed": {
            "app":            arr(allowed.get("app")),
            "site":           arr(allowed.get("site")),
            "publisher":      arr(allowed.get("publisher")),
            "crid":           arr(allowed.get("crid")),
            "adomain":        arr(allowed.get("adomain")),
            "displaymanager": arr(allowed.get("displaymanager")),
        },
        "blocked": {
            "app":            arr(blocked.get("app")),
            "site":           arr(blocked.get("site")),
            "publisher":      arr(blocked.get("publisher")),
            "crid":           arr(blocked.get("crid")),
            "adomain":        arr(blocked.get("adomain")),
            "displaymanager": arr(blocked.get("displaymanager")),
        },
    }

def scrub_readonly(d: Dict[str, Any]) -> Dict[str, Any]:
    ro = {"created_at","updated_at","createdAt","updatedAt","last_update","lastUpdate","_id"}
    return {k:v for k,v in d.items() if k not in ro}

def build_put_body(detail: Dict[str, Any], dsp_id: int, new_qps: int) -> Dict[str, Any]:
    base = scrub_readonly(detail)
    old_data = {
        "id": int(dsp_id),
        "api_endpoint": base.get("api_endpoint", base.get("endpoint","")),
        "Company": base.get("Company", {
            "id": base.get("company_id", 0),
            "name": base.get("company_name", ""),
            "api_key": base.get("api_key", "")
        }),
        "Size": base.get("Size", [{"code": "string"}]),
        "OperatingSystem": base.get("OperatingSystem", [{"key": "string", "name": "string"}]),
        "Country": base.get("Country", [{"country_code": "str"}]),
        "blockedSsp": base.get("blockedSsp", []),
        "Inventory": ensure_inventory(base),
    }
    updated = copy.deepcopy(base)
    updated["id"] = int(dsp_id)
    updated["Inventory"] = ensure_inventory(base)
    updated["Size"] = base.get("Size", [{"code": "string"}])
    updated["OperatingSystem"] = base.get("OperatingSystem", [{"key": "string", "name": "string"}])
    updated["Country"] = base.get("Country", [{"country_code": "str"}])
    updated["blockedSsp"] = base.get("blockedSsp", [])
    # set both keys to be safe
    updated["qps_limit"] = int(new_qps)
    updated["qps_Limit"] = int(new_qps)
    return {"oldData": old_data, "updatedData": updated}

# ======== rule engine ========
def decide_new_limit(srpm: float, real_qps: float, current_limit: int) -> Tuple[str, int, str]:
    """
    Rules:
    1) sRPM == 0 → set 50
    2) sRPM > 0.3 & real_qps >= 50% of limit → +15% (cap 30000)
    3) sRPM > 3   & real_qps >= 70% of limit → +15% (no cap)
    4) sRPM < 0.2 → −15% (floor 500)
    """
    srpm = float(srpm or 0)
    real_qps = float(real_qps or 0)
    current = int(current_limit or 0)

    if srpm == 0:
        return ("set", 50, "sRPM==0 → set 50")
    if srpm > 3 and current > 0 and real_qps >= 0.70 * current:
        return ("increase", math.ceil(current * 1.15), "sRPM>3 & ≥70% → +15% (no cap)")
    if srpm > 0.3 and current > 0 and real_qps >= 0.50 * current:
        return ("increase", min(30000, math.ceil(current * 1.15)), "sRPM>0.3 & ≥50% → +15% (cap 30000)")
    if srpm < 0.2 and current > 0:
        return ("decrease", max(500, math.ceil(current * 0.85)), "sRPM<0.2 → −15% (floor 500)")
    return ("hold", current, "no change")

# ======== main (always live) ========
def main() -> int:
    if not EMAIL or not PASSWORD:
        print("Missing creds in env (PLAYDIGO_EMAIL / PLAYDIGO_PASSWORD).", file=sys.stderr)
        return 2

    print("Authenticating…")
    token = auth_token()
    print("Auth OK. (LIVE mode)")

    print("Fetching all DSPs…")
    summaries = list_dsps(token)
    print(f"Total DSPs: {len(summaries)}")

    results = []
    updated = 0
    failed  = 0

    for s in summaries:
        dsp_id   = int(s.get("id"))
        dsp_name = s.get("name", "") or ""
        srpm     = float(s.get("sRPM") or 0)

        # ---- Exclude certain DSPs by name ----
        if dsp_name.strip().lower() in EXCLUDED_NAMES:
            print(f"Skipping DSP {dsp_name} (excluded)")
            results.append({
                "dsp_id": dsp_id,
                "name": dsp_name,
                "sRPM": srpm,
                "status": "skipped_excluded"
            })
            continue
        # --------------------------------------

        try:
            detail = get_detail(dsp_id, token)
        except Exception as e:
            failed += 1
            results.append({
                "dsp_id": dsp_id, "name": dsp_name, "error": f"detail_get_failed: {e}"
            })
            continue

        current_limit = int((detail.get("qps_limit") or detail.get("qps_Limit") or 0))
        real_qps      = float(detail.get("real_qps") or s.get("real_qps") or 0)

        action, new_limit, reason = decide_new_limit(srpm, real_qps, current_limit)
        http_status = ""; resp_text = ""; status = "skipped"

        if action != "hold" and new_limit != current_limit:
            payload = build_put_body(detail, dsp_id, new_limit)
            r = put_update(dsp_id, payload, token)
            http_status = str(r.status_code)
            resp_text   = r.text[:200]
            if 200 <= r.status_code < 300:
                status = "updated"; updated += 1
            else:
                status = "failed";  failed  += 1

        results.append({
            "dsp_id": dsp_id,
            "name": dsp_name,
            "sRPM": srpm,
            "real_qps": real_qps,
            "current_limit": current_limit,
            "action": action,
            "new_limit": new_limit,
            "reason": reason,
            "status": status,
            "http_status": http_status,
            "response": resp_text
        })

        time.sleep(0.1)  # polite

    # audit CSV
    ts = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    out_path = OUTDIR / f"qps_rule_engine_audit-{ts}.csv"
    if results:
        keys = sorted({k for r in results for k in r.keys()})
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            w.writerows(results)

    print("\n===== Summary =====")
    print(f"Evaluated: {len(results)} | Updated: {updated} | Failed: {failed}")
    print(f"Audit CSV → {out_path}")
    return 0 if failed == 0 else 0  # never fail the workflow unless fatal

if __name__ == "__main__":
    sys.exit(main())
