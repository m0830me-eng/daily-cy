import os
import sys
import json
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from curl_cffi import requests

# GitHub Actions에서도 로그를 즉시 표시
try:
    sys.stdout.reconfigure(line_buffering=True, write_through=True)
    sys.stderr.reconfigure(line_buffering=True, write_through=True)
except Exception:
    pass


KST = ZoneInfo("Asia/Seoul")

SITE_NO = "0059"
SITE_NAME = "CGV 영등포타임스퀘어"

DAYS = 43

# 오늘 포함 43일(+0~+42일)을 날짜 구간별로 분산 감시한다.
# +0일(오늘) : 300초
# +1일(내일) : 20초  (가까운 날짜 빠른 감시)
# +2~+4일    : 90초
# +5~+14일   : 30초
# +15~+30일  : 60초
# +31~+42일  : 300초
# 단, 예매 준비중 상태가 잡힌 날짜는 20초로 승격한다.
INTERVAL_TODAY = 300.0
INTERVAL_TOMORROW = 20.0
INTERVAL_EARLY = 90.0
INTERVAL_HOT = 30.0
INTERVAL_MID = 60.0
INTERVAL_FAR = 300.0
PRIORITY_INTERVAL = 20.0
MIN_REQUEST_GAP = 0.35
RATE_LIMIT_COOLDOWN = 60.0
SUMMARY_SECONDS = 600.0  # 정상 감시 요약: 10분마다 1회

# 매시 00분 / 30분: +4~+21일(18일) 완전 동시 빠른점검
FAST_SCAN_MINUTES = {0, 30}
FAST_SCAN_START_OFFSET = 4
FAST_SCAN_END_OFFSET = 21
FAST_SCAN_WORKERS = 18
START_DELAY = float(os.environ.get("START_DELAY", "0"))
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "120"))

BOOKING_PAGE = "https://cgv.co.kr/cnm/movieBook"
API_URL = "https://cgv.co.kr/api/v1/booking/searchMovScnInfo"

STATE_FILE = "seen_cgv_yeongdeungpo.json"
BASELINE_FILE = "baseline_cgv_yeongdeungpo.done"
BOOKING_STATE_FILE = "cgv_yeongdeungpo_booking_state.json"
BOOKING_STATE_SCHEMA = "CGV_YEONGDEUNGPO_BOOKING_STATE_V1"

# GitHub Secrets
COMMON_WEBHOOK = os.environ.get("CY_WEBHOOK", "").strip()
GV_WEBHOOK = COMMON_WEBHOOK
STAGE_WEBHOOK = COMMON_WEBHOOK
DISCORD_USER_ID = os.environ.get("DISCORD_MENTION_ID", "").strip()


HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
    "Referer": BOOKING_PAGE,
}

BLOCK_STATUSES = {403, 429, 500, 502, 503, 504}



# ============================================================
# Basic helpers
# ============================================================

def now_kst():
    return datetime.now(KST)


def clean_text(value):
    return " ".join(str(value or "").split())


def all_row_text(value):
    parts = []

    def walk(item):
        if isinstance(item, dict):
            for key, val in item.items():
                if val is not None:
                    key_text = clean_text(key)
                    val_text = clean_text(val) if not isinstance(val, (dict, list, tuple, set)) else ""
                    if key_text and val_text:
                        parts.append(f"{key_text}={val_text}")
                walk(val)
        elif isinstance(item, (list, tuple, set)):
            for val in item:
                walk(val)
        elif item is not None:
            text = clean_text(item)
            if text:
                parts.append(text)

    walk(value)
    return " | ".join(parts)


def make_dates():
    today = now_kst()
    return [
        (today + timedelta(days=i)).strftime("%Y%m%d")
        for i in range(DAYS)
    ]


def pretty_date(date):
    dt = datetime.strptime(date, "%Y%m%d")
    weekdays = ["월", "화", "수", "목", "금", "토", "일"]
    return f"{dt.year}-{dt.month:02d}-{dt.day:02d} ({weekdays[dt.weekday()]})"


def pretty_time(value):
    text = clean_text(value).replace(":", "")
    if len(text) == 4 and text.isdigit():
        return text[:2] + ":" + text[2:]
    return clean_text(value)


def parse_int(value):
    if value is None:
        return None
    # "4", "4석", "4/624" 모두 앞의 실제 잔여값 4로 읽는다.
    match = re.search(r"-?\d+", str(value))
    if not match:
        return None
    try:
        return int(match.group(0))
    except Exception:
        return None


# ============================================================
# Discord
# ============================================================

def webhook_for_type(event_type):
    if event_type == "무대인사":
        return STAGE_WEBHOOK or COMMON_WEBHOOK
    return GV_WEBHOOK or COMMON_WEBHOOK


def send_discord(webhook, message):
    if not webhook:
        print("⚠️ DISCORD WEBHOOK MISSING")
        return False

    payload = {
        "content": message,
        "flags": 4,
    }

    if DISCORD_USER_ID:
        payload["allowed_mentions"] = {
            "users": [DISCORD_USER_ID]
        }

    try:
        r = requests.post(
            webhook,
            json=payload,
            impersonate="chrome",
            timeout=15,
        )
        r.raise_for_status()
        return True
    except Exception as e:
        print("❌ DISCORD ERROR:", repr(e))
        return False


# ============================================================
# Persistent state
# ============================================================

def load_seen():
    if not os.path.exists(STATE_FILE):
        return set()

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data) if isinstance(data, list) else set()
    except Exception as e:
        print("⚠️ STATE LOAD ERROR:", repr(e))
        return set()


def save_seen(seen, quiet=True):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(seen), f, ensure_ascii=False, indent=2)
        if not quiet:
            print("STATE SAVED:", len(seen))
    except Exception as e:
        print("⚠️ STATE SAVE ERROR:", repr(e))


def baseline_done():
    return os.path.exists(BASELINE_FILE)


def mark_baseline_done():
    with open(BASELINE_FILE, "w", encoding="utf-8") as f:
        f.write(now_kst().isoformat())
    print("BASELINE MARKER CREATED")


def load_booking_state():
    if not os.path.exists(BOOKING_STATE_FILE):
        return {}, False

    try:
        with open(BOOKING_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            return {}, False

        if data.get("schema") != BOOKING_STATE_SCHEMA:
            return {}, False

        shows = data.get("shows")
        if not isinstance(shows, dict):
            return {}, False

        return shows, True

    except Exception as e:
        print("⚠️ BOOKING STATE LOAD ERROR:", repr(e))
        return {}, False


def save_booking_state(shows):
    payload = {
        "schema": BOOKING_STATE_SCHEMA,
        "updated_at_kst": now_kst().isoformat(),
        "shows": shows,
    }

    try:
        temp = BOOKING_STATE_FILE + ".tmp"
        with open(temp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(temp, BOOKING_STATE_FILE)
    except Exception as e:
        print("⚠️ BOOKING STATE SAVE ERROR:", repr(e))


# ============================================================
# CGV target classification
# ============================================================

def detect_event_type(row):
    event_fields = [
        "videoAddexpCdNm",
        "videoAddexpNm",
        "videoAddexpCd",
        "eventNm",
        "eventName",
        "specialEventNm",
        "specialEventName",
        "addexpNm",
        "addexpName",
        "movNm",
        "movName",
    ]

    event_text = " | ".join(
        clean_text(row.get(field))
        for field in event_fields
        if clean_text(row.get(field))
    )
    full_text = all_row_text(row)

    compact_event = re.sub(r"\s+", "", event_text)
    compact_full = re.sub(r"\s+", "", full_text)

    if (
        "무대인사" in compact_event
        or "무대인사" in compact_full
        or "舞台挨拶" in full_text
    ):
        return "무대인사"

    if (
        "관객과의대화" in compact_event
        or "관객과의대화" in compact_full
    ):
        return "GV"

    if re.search(r"(?<![A-Z0-9])GV(?![A-Z0-9])", event_text.upper()):
        return "GV"

    if re.search(r"(?<![A-Z0-9])GV(?![A-Z0-9])", full_text.upper()):
        return "GV"

    return None


def detect_format(row):
    format_fields = [
        "scnsNm",
        "scnsName",
        "screenNm",
        "screenName",
        "playKindNm",
        "playKindName",
        "screenTypeNm",
        "screenTypeName",
        "formatNm",
        "formatName",
    ]

    format_text = " | ".join(
        clean_text(row.get(field))
        for field in format_fields
        if clean_text(row.get(field))
    )
    full_text = all_row_text(row)

    format_upper = format_text.upper()
    full_upper = full_text.upper()

    # GV/무대인사 우선 분류 후, 일반 특별관 중 IMAX만 추적한다.
    if (
        "IMAX" in format_upper
        or "아이맥스" in format_text
        or "IMAX" in full_upper
        or "아이맥스" in full_text
    ):
        return "IMAX"

    return None




def get_target_type(row):
    # 이벤트 회차가 특별관이어도 GV/무대인사를 우선해 중복 알림 방지.
    event_type = detect_event_type(row)
    if event_type:
        return event_type
    return detect_format(row)


# ============================================================
# Booking-state classification
# ============================================================

def classify_booking_state(row):
    """
    사용자에게 보여주는 상태:
      PREPARING -> 예매 준비중
      OPEN      -> 예매 오픈 가능
      SOLD_OUT  -> 매진 저장(알림 없음)
      UNKNOWN   -> 확실한 상태값 없음

    우선순위:
      1) 실제 API 문구의 예매준비중
      2) 실제 매진 문구
      3) cntlYn=Y (CGV 제어/비예매 상태) -> 준비중으로 추적
      4) 명시적 잔여석 > 0 -> OPEN
      5) cntlYn=N/예매가능 플래그 + 잔여석 0 -> SOLD_OUT
    """
    full_text = all_row_text(row)
    compact = re.sub(r"\s+", "", full_text)
    upper = full_text.upper()

    if "예매준비중" in compact:
        return "PREPARING", "text:예매준비중"

    if (
        "매진" in compact
        or "SOLD OUT" in upper
        or "SOLDOUT" in upper
    ):
        return "SOLD_OUT", "text:매진"

    cntl = clean_text(row.get("cntlYn")).upper()
    if cntl == "Y":
        return "PREPARING", "cntlYn=Y"

    seat_fields = [
        "frSeatCnt",
        "restSeatCnt",
        "remainSeatCnt",
        "remainSeats",
        "seatCnt",
    ]

    seat_count = None
    seat_source = ""
    for field in seat_fields:
        if field in row and row.get(field) is not None:
            parsed = parse_int(row.get(field))
            if parsed is not None:
                seat_count = parsed
                seat_source = field
                break

    if seat_count is not None and seat_count > 0:
        return "OPEN", f"{seat_source}={seat_count}"

    book_flag = clean_text(
        row.get("bookYn")
        or row.get("bookingYn")
        or row.get("rsvYn")
    ).upper()

    if seat_count == 0 and (cntl == "N" or book_flag == "Y"):
        return "SOLD_OUT", f"{seat_source}=0"

    return "UNKNOWN", "no-explicit-status"


# ============================================================
# Event normalization
# ============================================================

def event_key(date, row, event_type):
    return "|".join([
        SITE_NO,
        date,
        str(row.get("movNo") or ""),
        str(row.get("scnsNo") or ""),
        str(row.get("scnSseq") or ""),
        str(row.get("scnsrtTm") or ""),
        event_type,
    ])


def make_booking_link(date, row):
    params = {
        "movNo": str(row.get("movNo") or ""),
        "scnYmd": date,
        "siteNo": SITE_NO,
        "scnsNo": str(row.get("scnsNo") or ""),
        "siteNm": SITE_NAME,
        "scnSseq": str(row.get("scnSseq") or ""),
    }
    return "https://cgv.co.kr/cnm/movieBook/movie?" + urlencode(params)


def normalize_event(date, row, target_type):
    status, status_source = classify_booking_state(row)

    return {
        "date": date,
        "type": target_type,
        "movie": clean_text(row.get("movNm") or row.get("movName")),
        "screen": clean_text(
            row.get("scnsNm")
            or row.get("scnsName")
            or row.get("screenNm")
            or row.get("screenName")
        ),
        "time": clean_text(row.get("scnsrtTm")),
        "status": status,
        "status_source": status_source,
        "link": make_booking_link(date, row),
        "row": row,
    }


def state_record(event, status=None):
    return {
        "status": status or event.get("status", "UNKNOWN"),
        "date": event.get("date", ""),
        "type": event.get("type", ""),
        "movie": event.get("movie", ""),
        "screen": event.get("screen", ""),
        "time": event.get("time", ""),
        "status_source": event.get("status_source", ""),
        "updated_at_kst": now_kst().isoformat(),
    }


# ============================================================
# API
# ============================================================

def extract_rows(data):
    if isinstance(data, dict):
        direct = data.get("data")
        if isinstance(direct, list):
            return [row for row in direct if isinstance(row, dict)]

    rows = []

    def walk(item):
        if isinstance(item, dict):
            looks_like_show = (
                (item.get("movNo") or item.get("movNm"))
                and (item.get("scnsrtTm") or item.get("scnSseq"))
            )
            if looks_like_show:
                rows.append(item)
            for value in item.values():
                walk(value)
        elif isinstance(item, list):
            for value in item:
                walk(value)

    walk(data)
    return rows


def check_one_date(session, date):
    events = {}

    try:
        started = time.monotonic()
        r = session.get(
            API_URL,
            params={
                "coCd": "A420",
                "siteNo": SITE_NO,
                "scnYmd": date,
                "rtctlScopCd": "08",
            },
            headers=HEADERS,
            timeout=20,
        )
        elapsed = time.monotonic() - started

        if r.status_code in BLOCK_STATUSES:
            return None, (
                f"HTTP {r.status_code} | DATE={date} | {elapsed:.2f}s"
            )

        if r.status_code != 200:
            return None, (
                f"HTTP {r.status_code} | DATE={date} | {elapsed:.2f}s"
            )

        try:
            data = r.json()
        except Exception as e:
            return None, f"JSON ERROR | DATE={date} | {repr(e)}"

        rows = extract_rows(data)

        for row in rows:
            target_type = get_target_type(row)
            if not target_type:
                continue

            key = event_key(date, row, target_type)
            events[key] = normalize_event(date, row, target_type)

        return events, None

    except Exception as e:
        return None, f"REQUEST ERROR | DATE={date} | {repr(e)}"


# ============================================================
# Notifications / transitions
# ============================================================

def detected_title(event_type):
    if event_type == "GV":
        return f"🎬 {SITE_NAME} · GV가 감지됐습니다"
    if event_type == "무대인사":
        return f"🎬 {SITE_NAME} · 무대인사가 감지됐습니다"
    if event_type == "IMAX":
        return f"🎬 {SITE_NAME} · IMAX가 감지됐습니다"
    return f"🎬 {SITE_NAME} · 특별 상영 일정이 감지됐습니다"


def notification_title(event_type, alert_kind):
    if alert_kind == "PREPARING":
        return "⏳ 예매준비중"
    if alert_kind == "OPEN":
        return "🎟️ 예매가 열렸습니다"

    if event_type == "GV":
        return "🔎 GV가 감지됐습니다"
    if event_type == "무대인사":
        return "🔎 무대인사가 감지됐습니다"
    if event_type == "IMAX":
        return "🔎 IMAX가 감지됐습니다"

    return "🔎 특별 상영 일정이 감지됐습니다"


def send_event_alert(event, alert_kind):
    start = pretty_time(event.get("time", ""))
    movie = event.get("movie", "") or "영화명 미확인"
    screen = event.get("screen", "")
    event_type = event.get("type", "")
    link = event.get("link", "")

    lines = []

    if DISCORD_USER_ID:
        lines.append(f"<@{DISCORD_USER_ID}>")

    lines.extend([
        notification_title(event_type, alert_kind),
        f"[🎬 {SITE_NAME} · {event_type}]({link})",
        f"📅 {pretty_date(event['date'])}",
    ])

    if screen:
        lines.append(f"🎟 {start} · {movie} · {screen}")
    else:
        lines.append(f"🎟 {start} · {movie}")

    ok = send_discord(
        webhook_for_type(event_type),
        "\n".join(lines),
    )

    if ok:
        label = notification_title(event_type, alert_kind)
        print(
            f"🔔 {label} | {event['date']} | {event_type} | "
            f"{start} | {movie} | {screen}"
        )

    return ok


def process_event(key, event, seen, booking_state):
    """
    Returns: (discord_alerts, soldout_changes)

    SOLD_OUT은 내부 상태 판별용으로만 저장한다.
    사용자 로그/Discord에는 매진이나 취소표 알림을 보내지 않는다.
    """
    current = event.get("status", "UNKNOWN")
    previous_record = booking_state.get(key)
    previous = (
        previous_record.get("status", "UNKNOWN")
        if isinstance(previous_record, dict)
        else "UNKNOWN"
    )

    is_new_key = key not in seen
    alerts = 0
    soldout_changes = 0

    if current == "UNKNOWN":
        if is_new_key:
            if send_event_alert(event, "NEW"):
                seen.add(key)
                booking_state[key] = state_record(event, "UNKNOWN")
                alerts += 1
        return alerts, soldout_changes

    # 최초 발견이 이미 매진이면 사용자에게 알리지 않고 내부 상태만 기억.
    if current == "SOLD_OUT" and is_new_key:
        seen.add(key)
        booking_state[key] = state_record(event, "SOLD_OUT")
        return alerts, soldout_changes

    # 새 회차가 준비중/오픈 상태로 처음 API에 등장.
    if is_new_key:
        alert_kind = "PREPARING" if current == "PREPARING" else "OPEN"
        if send_event_alert(event, alert_kind):
            seen.add(key)
            booking_state[key] = state_record(event, current)
            alerts += 1
        return alerts, soldout_changes

    if previous_record is None:
        previous = "UNKNOWN"

    if current == previous:
        booking_state[key] = state_record(event, current)
        return alerts, soldout_changes

    if previous == "UNKNOWN" and current in {"PREPARING", "OPEN"}:
        alert_kind = "PREPARING" if current == "PREPARING" else "OPEN"
        if send_event_alert(event, alert_kind):
            booking_state[key] = state_record(event, current)
            alerts += 1
        return alerts, soldout_changes

    if previous == "PREPARING" and current == "OPEN":
        if send_event_alert(event, "OPEN"):
            booking_state[key] = state_record(event, "OPEN")
            alerts += 1
        return alerts, soldout_changes

    # 어떤 상태든 -> 매진: 사용자에게는 아무것도 보여주지 않고 내부 상태만 갱신.
    if current == "SOLD_OUT":
        booking_state[key] = state_record(event, "SOLD_OUT")
        return alerts, soldout_changes

    # 매진 -> OPEN: 취소표 알림 없이 내부 상태만 변경.
    if previous == "SOLD_OUT" and current == "OPEN":
        booking_state[key] = state_record(event, "OPEN")
        return alerts, soldout_changes

    # OPEN/SOLD_OUT 뒤 PREPARING 흔들림은 기존 확정 상태 기억 유지.
    if current == "PREPARING" and previous in {"SOLD_OUT", "OPEN"}:
        return alerts, soldout_changes

    booking_state[key] = state_record(event, current)
    return alerts, soldout_changes


# ============================================================
# Scan / compact logging
# ============================================================

def count_targets(events):
    counts = {
        "GV": 0,
        "무대인사": 0,
        "IMAX": 0,
    }
    for event in events.values():
        event_type = event.get("type")
        if event_type in counts:
            counts[event_type] += 1
    return counts


def collect_full_scan(session, progress=False):
    all_events = {}

    for index, date in enumerate(make_dates(), start=1):
        events, error = check_one_date(session, date)
        if error:
            print(f"❌ CGV API 오류 | {error}")
            return None, error

        all_events.update(events)

        if progress and index % 10 == 0:
            print(f"⏳ 기준값 진행: {index}/{DAYS} 날짜 처리 완료")

    return all_events, None


def initialize_missing_state(session, seen, booking_state, booking_state_ready):
    need_seen_baseline = not baseline_done()
    need_booking_baseline = not booking_state_ready

    if not need_seen_baseline and not need_booking_baseline:
        return seen, booking_state, True

    print()
    print("=" * 72)
    print(f"INITIAL {DAYS}-DAY BASELINE")
    print("=" * 72)

    if need_seen_baseline and need_booking_baseline:
        print(
            f"현재 {DAYS}일 전체 GV / 무대인사 / IMAX와 예매 상태를 "
            "알림 없이 기준값으로 등록합니다."
        )
    elif need_booking_baseline:
        print(
            f"기존 회차 기준값은 유지하고, 현재 {DAYS}일 전체 예매 상태만 "
            "알림 없이 새 기준값으로 등록합니다."
        )
    else:
        print(
            f"현재 {DAYS}일 전체 GV / 무대인사 / IMAX를 "
            "알림 없이 기준값으로 등록합니다."
        )

    events, error = collect_full_scan(session, progress=True)
    if error or events is None:
        print("BASELINE FAILED - 불완전한 기준값은 저장하지 않습니다.")
        return seen, booking_state, False

    if need_seen_baseline:
        seen = set(events.keys())
        save_seen(seen)
        mark_baseline_done()

    if need_booking_baseline:
        booking_state = {
            key: state_record(event, event.get("status", "UNKNOWN"))
            for key, event in events.items()
        }
        save_booking_state(booking_state)

    counts = count_targets(events)
    print("BASELINE EVENT COUNT:", len(events))
    print(
        "BASELINE COUNTS: "
        f"GV={counts['GV']} | 무대인사={counts['무대인사']} | "
        f"IMAX={counts['IMAX']}"
    )
    print("BASELINE COMPLETE")
    print("이번 기준값 등록에서는 Discord 알림을 보내지 않았습니다.")
    print(
        "✅ 기준값 등록 완료 - 이 실행을 종료하지 않고 "
        "다음 정규 자동실행 1분 전까지 계속 감시합니다."
    )

    return seen, booking_state, True


def base_interval_for_offset(offset):
    if offset <= 0:
        return INTERVAL_TODAY
    if offset == 1:
        return INTERVAL_TOMORROW
    if offset <= 4:
        return INTERVAL_EARLY
    if offset <= 14:
        return INTERVAL_HOT
    if offset <= 30:
        return INTERVAL_MID
    return INTERVAL_FAR


def has_priority_state_for_date(date, booking_state):
    for record in booking_state.values():
        if not isinstance(record, dict):
            continue
        if record.get("date") != date:
            continue
        if record.get("status") == "PREPARING":
            return True
    return False


def interval_for_date(date, booking_state):
    today = now_kst().date()
    target = datetime.strptime(date, "%Y%m%d").date()
    offset = max(0, (target - today).days)
    base = base_interval_for_offset(offset)

    if has_priority_state_for_date(date, booking_state):
        return min(base, PRIORITY_INTERVAL)

    return base


def merged_event_cache(date_event_cache):
    merged = {}
    for events in date_event_cache.values():
        if isinstance(events, dict):
            merged.update(events)
    return merged


def stagger_schedule(dates, booking_state, start_at=None):
    """같은 주기의 날짜들이 한 순간에 몰리지 않도록 고르게 분산한다."""
    if start_at is None:
        start_at = time.monotonic()

    groups = {}
    for date in dates:
        interval = interval_for_date(date, booking_state)
        groups.setdefault(interval, []).append(date)

    next_due = {}
    for interval, group_dates in groups.items():
        count = max(1, len(group_dates))
        spacing = interval / count
        for index, date in enumerate(group_dates):
            next_due[date] = start_at + (index * spacing)

    return next_due



def make_fast_scan_dates():
    today = now_kst().date()
    return [
        (today + timedelta(days=offset)).strftime("%Y%m%d")
        for offset in range(FAST_SCAN_START_OFFSET, FAST_SCAN_END_OFFSET + 1)
    ]


def run_0030_fast_scan(seen, booking_state, date_event_cache):
    """
    매시 00/30분에 +4~+21일 18개 날짜를 동시에 확인한다.
    - 18 workers
    - 각 worker는 독립 curl_cffi Session 사용
    - 상태/Discord 처리는 응답 수집 후 메인 스레드에서 순차 처리
    - 실패 날짜는 기존 캐시/상태를 절대 덮어쓰지 않는다.
    """
    dates = make_fast_scan_dates()
    barrier = threading.Barrier(len(dates))
    started = time.monotonic()

    def worker(date):
        session = requests.Session(impersonate="chrome")
        try:
            try:
                barrier.wait(timeout=10)
            except threading.BrokenBarrierError:
                pass
            events, error = check_one_date(session, date)
            return date, events, error
        finally:
            try:
                session.close()
            except Exception:
                pass

    results = []
    with ThreadPoolExecutor(max_workers=FAST_SCAN_WORKERS) as executor:
        futures = [executor.submit(worker, date) for date in dates]
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                results.append(("", None, f"WORKER ERROR | {repr(e)}"))

    elapsed = time.monotonic() - started
    results.sort(key=lambda item: item[0])

    success = 0
    errors = 0
    alerts = 0
    soldout_changes = 0

    for date, events, error in results:
        if error or events is None:
            errors += 1
            if error:
                print(f"❌ CGV 00/30 동시스캔 오류 | {error}")
            continue

        success += 1
        date_event_cache[date] = events

        for key, event in events.items():
            a, s = process_event(key, event, seen, booking_state)
            alerts += a
            soldout_changes += s

    save_seen(seen)
    save_booking_state(booking_state)

    icon = "⚡" if errors == 0 else "⚠️"
    print(
        f"{icon} {now_kst().strftime('%H:%M')} 00/30 동시스캔 완료 | "
        f"+4~+21일 | 성공 {success}/{len(dates)} | "
        f"{elapsed:.2f}초 | Discord 알림 {alerts} | "
        f"오류 {errors}"
    )

    return success, errors, alerts, soldout_changes

def run_initial_monitor_scan(session, seen, booking_state):
    """감시 시작 시 현재 감시 범위를 딱 한 번 훑어 캐시를 만든다."""
    date_event_cache = {}
    dates = make_dates()
    alerts = 0
    soldout = 0
    errors = 0
    rate_limited = False
    last_request_started = 0.0

    scan_started = time.monotonic()

    for index, date in enumerate(dates, start=1):
        now = time.monotonic()
        gap_wait = MIN_REQUEST_GAP - (now - last_request_started)
        if gap_wait > 0:
            time.sleep(gap_wait)
        last_request_started = time.monotonic()

        events, error = check_one_date(session, date)

        if error:
            errors += 1
            if "HTTP 429" in error:
                print(
                    "⚠️ CGV API 제한(429) 감지 - "
                    f"{RATE_LIMIT_COOLDOWN:.0f}초 쉬고 분산 감시로 전환합니다."
                )
                rate_limited = True
                break

            print(f"❌ CGV API 오류 | {error}")
            continue

        date_event_cache[date] = events

        for key, event in events.items():
            a, s = process_event(key, event, seen, booking_state)
            alerts += a
            soldout += s

        if index % 10 == 0:
            print(f"⏳ 첫 감시 준비: {index}/{DAYS} 날짜 확인 완료")

    elapsed = time.monotonic() - scan_started
    counts = count_targets(merged_event_cache(date_event_cache))

    if not rate_limited:
        print(
            f"✅ 첫 {DAYS}일 감시 확인 완료 | "
            f"{elapsed:.2f}초 | "
            f"GV {counts['GV']} | 무대인사 {counts['무대인사']} | "
            f"IMAX {counts['IMAX']}"
        )

    return date_event_cache, alerts, soldout, errors, rate_limited


def run_monitor(session, seen, booking_state, program_started):
    dates = make_dates()
    report_started = time.monotonic()
    window_requests = 0
    window_success = 0
    window_alerts = 0
    window_soldout = 0
    window_errors = 0
    total_requests = 0
    last_request_started = 0.0
    last_fast_scan_slot = None

    date_event_cache, init_alerts, init_soldout, init_errors, rate_limited = (
        run_initial_monitor_scan(session, seen, booking_state)
    )
    window_alerts += init_alerts
    window_soldout += init_soldout
    window_errors += init_errors

    save_seen(seen)
    save_booking_state(booking_state)

    now_mono = time.monotonic()
    if rate_limited:
        cooldown_until = now_mono + RATE_LIMIT_COOLDOWN
        print(
            f"⏸️ {RATE_LIMIT_COOLDOWN:.0f}초 API 휴식 후 "
            "날짜별 분산 감시를 시작합니다."
        )
        next_due = stagger_schedule(
            dates,
            booking_state,
            start_at=cooldown_until,
        )
    else:
        next_due = stagger_schedule(
            dates,
            booking_state,
            start_at=now_mono,
        )

    print(
        "📡 날짜별 분산 감시 시작 | "
        "오늘 5분 / 내일(+1) 20초 / +2~+4일 90초 / "
        "+5~+14일 30초 / +15~+30일 60초 / +31~+42일 5분 | "
        "예매준비중 날짜는 20초"
    )
    print(
        "⚡ 고정 동시스캔 | 매시 00분 / 30분 | "
        "+4~+21일 18일 | 18 workers 완전 동시"
    )

    while time.monotonic() - program_started < RUN_SECONDS:
        now_mono = time.monotonic()
        elapsed_total = now_mono - program_started
        remaining = RUN_SECONDS - elapsed_total
        if remaining <= 0:
            break

        # 매시 00분/30분에는 +4~+21일 18일을 한 번에 동시 확인한다.
        # 같은 분 안에서는 딱 한 번만 실행한다.
        wall_now = now_kst()
        if wall_now.minute in FAST_SCAN_MINUTES:
            slot_key = wall_now.strftime("%Y%m%d%H%M")
            if slot_key != last_fast_scan_slot:
                last_fast_scan_slot = slot_key
                fs_success, fs_errors, fs_alerts, fs_soldout = run_0030_fast_scan(
                    seen,
                    booking_state,
                    date_event_cache,
                )
                total_requests += len(make_fast_scan_dates())
                window_requests += len(make_fast_scan_dates())
                window_success += fs_success
                window_errors += fs_errors
                window_alerts += fs_alerts
                window_soldout += fs_soldout
                continue

        # 10분 요약 시점이면 조회보다 먼저 요약을 출력한다.
        if now_mono - report_started >= SUMMARY_SECONDS:
            counts = count_targets(merged_event_cache(date_event_cache))
            status_icon = "💚" if window_errors == 0 else "⚠️"
            status_text = "정상 감시중" if window_errors == 0 else "감시중(API 오류 있음)"
            print(
                f"{status_icon} {status_text} | "
                f"최근 10분 날짜조회 {window_requests}회 / 성공 {window_success}회 | "
                f"누적 조회 {total_requests}회 | "
                f"GV {counts['GV']} | 무대인사 {counts['무대인사']} | "
                f"IMAX {counts['IMAX']} | "
                f"Discord 알림 {window_alerts} | 오류 {window_errors}"
            )
            report_started = now_mono
            window_requests = 0
            window_success = 0
            window_alerts = 0
            window_soldout = 0
            window_errors = 0
            continue

        due_date = min(next_due, key=next_due.get)
        due_at = next_due[due_date]

        if due_at > now_mono:
            until_due = due_at - now_mono
            until_report = max(0.0, SUMMARY_SECONDS - (now_mono - report_started))
            sleep_for = min(until_due, until_report, remaining, 1.0)
            if sleep_for > 0:
                time.sleep(sleep_for)
            continue

        # API 요청 시작 자체도 최소 간격을 둬서 순간 버스트를 막는다.
        gap_wait = MIN_REQUEST_GAP - (time.monotonic() - last_request_started)
        if gap_wait > 0:
            time.sleep(min(gap_wait, remaining))
        last_request_started = time.monotonic()

        events, error = check_one_date(session, due_date)
        total_requests += 1
        window_requests += 1

        if error:
            window_errors += 1

            if "HTTP 429" in error:
                print(
                    "⚠️ CGV API 제한(429) 감지 - "
                    f"{RATE_LIMIT_COOLDOWN:.0f}초 전체 휴식 후 분산 재개"
                )
                cooldown_until = time.monotonic() + RATE_LIMIT_COOLDOWN
                next_due = stagger_schedule(
                    dates,
                    booking_state,
                    start_at=cooldown_until,
                )
                continue

            print(f"❌ CGV API 오류 | {error}")
            next_due[due_date] = (
                time.monotonic()
                + interval_for_date(due_date, booking_state)
            )
            continue

        window_success += 1
        date_event_cache[due_date] = events

        cycle_alerts = 0
        cycle_soldout = 0
        for key, event in events.items():
            alerts, soldout_changes = process_event(
                key,
                event,
                seen,
                booking_state,
            )
            cycle_alerts += alerts
            cycle_soldout += soldout_changes

        window_alerts += cycle_alerts
        window_soldout += cycle_soldout

        save_seen(seen)
        save_booking_state(booking_state)

        # 방금 상태가 PREPARING으로 바뀌었으면 그 날짜는 자동 20초 승격.
        next_due[due_date] = (
            time.monotonic()
            + interval_for_date(due_date, booking_state)
        )

    save_seen(seen)
    save_booking_state(booking_state)

    print(
        "✅ CGV 감시 종료 | "
        f"누적 날짜조회 {total_requests}회 | "
        "다음 정규 실행에 상태를 이어갑니다."
    )


# ============================================================
# Main
# ============================================================

def main():
    program_started = time.monotonic()

    print("=" * 72)
    print("CGV YONGSAN MONITOR")
    print("=" * 72)
    print("BRANCH:", SITE_NAME)
    print("TARGET: GV / 무대인사 / IMAX")
    print(f"DATE RANGE: TODAY ~ +{DAYS - 1} DAYS ({DAYS} DAYS TOTAL)")
    print(f"SCAN MODE: {DAYS} DAYS / DATE-BY-DATE STAGGERED")
    print("INTERVAL: 오늘 300s / 내일(+1) 20s / +2~+4일 90s / +5~+14일 30s / +15~+30일 60s / +31~+42일 300s")
    print("PRIORITY: 예매준비중이 잡힌 날짜는 20s")
    print(f"MIN REQUEST START GAP: {MIN_REQUEST_GAP:.2f}s")
    print(f"HTTP 429: {RATE_LIMIT_COOLDOWN:.0f}s 전체 휴식 후 분산 재개")
    print("EARLY DETECTION: 화면 표시 전이라도 CGV API에 대상 회차/종류가 있으면 추적")
    print("PREPARING: '예매준비중' 문구 또는 cntlYn=Y 감지")
    print("OPEN: 명시적 잔여좌석 > 0")
    print("FAST SCAN: 매시 00/30분 +4~+21일 18일 / 18 workers 완전 동시")
    print("LOG MODE: 기준값/첫확인 진행 + 정상 감시는 10분 요약")
    print("RUN SECONDS:", RUN_SECONDS)
    print("KST NOW:", now_kst().strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 72)

    if START_DELAY > 0:
        print(f"START STAGGER: {START_DELAY:.2f}s")
        time.sleep(START_DELAY)

    session = requests.Session(impersonate="chrome")

    try:
        r = session.get(
            BOOKING_PAGE,
            headers=HEADERS,
            timeout=20,
        )
        print("BOOKING PAGE STATUS:", r.status_code)
        if r.status_code != 200:
            print("⚠️ CGV BOOKING PAGE CHECK FAILED - API 감시는 계속합니다.")
    except Exception as e:
        print("⚠️ CGV BOOKING PAGE CHECK WARNING:", repr(e))
        print("BOOKING PAGE CHECK FAILED - API 감시는 계속합니다.")

    seen = load_seen()
    booking_state, booking_state_ready = load_booking_state()

    seen, booking_state, ready = initialize_missing_state(
        session,
        seen,
        booking_state,
        booking_state_ready,
    )

    if not ready:
        return

    # 기준값 등록에 걸린 시간도 RUN_SECONDS에 포함한다.
    if time.monotonic() - program_started >= RUN_SECONDS:
        print("RUN TIME FINISHED AFTER BASELINE")
        return

    run_monitor(
        session,
        seen,
        booking_state,
        program_started,
    )


if __name__ == "__main__":
    main()
