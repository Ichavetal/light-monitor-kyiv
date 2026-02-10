import os
import json
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional

# === Configuration ===
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID")
CONFIG_FILE = "config.json"
CACHE_FILE = "last_schedules.json"
MESSAGES_FILE = "message_ids.json"

KYIV_TZ = timezone(timedelta(hours=2))

GITHUB_URL = "https://raw.githubusercontent.com/Baskerville42/outage-data-ua/main/data/{region}.json"
YASNO_URL = "https://app.yasno.ua/api/blackout-service/public/shutdowns/regions/{region_id}/dsos/{dso_id}/planned-outages"

MAX_MESSAGES = 3
DAYS_UA = {0: "Понеділок", 1: "Вівторок", 2: "Середа", 3: "Четвер", 4: "П'ятниця", 5: "Субота", 6: "Неділя"}


def load_config() -> dict:
    """Load config with validation"""
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            content = f.read()
            try:
                return json.loads(content)
            except json.JSONDecodeError as e:
                print(f"JSON Error in {CONFIG_FILE}:")
                print(f"  Line {e.lineno}, Column {e.colno}: {e.msg}")
                lines = content.split('\n')
                if e.lineno <= len(lines):
                    print(f"  → {lines[e.lineno - 1]}")
                raise SystemExit(1)
    except FileNotFoundError:
        print(f"Config file not found: {CONFIG_FILE}")
        raise SystemExit(1)


def get_kyiv_now() -> datetime:
    return datetime.now(KYIV_TZ)


def format_hours_full(hours: float) -> str:
    """Format hours with full Ukrainian declension"""
    if hours == int(hours):
        hours = int(hours)
    
    if isinstance(hours, float):
        return f"{hours} години"
    if hours % 10 == 1 and hours % 100 != 11:
        return f"{hours} година"
    if hours % 10 in [2, 3, 4] and hours % 100 not in [12, 13, 14]:
        return f"{hours} години"
    return f"{hours} годин"


def format_hours_short(hours: float) -> str:
    """Format hours short (for table)"""
    if hours == int(hours):
        return f"{int(hours)}г"
    return f"{hours}г"


def format_slot_time(slot: int) -> str:
    mins = slot * 30
    h, m = mins // 60, mins % 60
    return "24:00" if h == 24 else f"{h:02d}:{m:02d}"


# === Data Fetching ===

def fetch_github(cfg: dict) -> Optional[dict]:
    if not cfg['sources']['github']['enabled']:
        return None
    try:
        url = GITHUB_URL.format(region=cfg['settings']['region'])
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"GitHub error: {e}")
        return None


def fetch_yasno(cfg: dict) -> Optional[dict]:
    if not cfg['sources']['yasno']['enabled']:
        return None
    try:
        url = YASNO_URL.format(
            region_id=cfg['settings']['yasno_region_id'],
            dso_id=cfg['settings']['yasno_dso_id']
        )
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Yasno error: {e}")
        return None


# === Parsing ===

def parse_github_day(day_data: dict) -> list[bool]:
    slots = []
    for h in range(1, 25):
        s = day_data.get(str(h), "yes")
        if s == "yes":
            slots.extend([True, True])
        elif s == "no":
            slots.extend([False, False])
        elif s == "first":
            slots.extend([False, True])
        elif s == "second":
            slots.extend([True, False])
        else:
            slots.extend([True, True])
    return slots


def extract_github(data: dict, cfg: dict) -> dict:
    res = {}
    if not data:
        return res
    fact = data.get("fact", {}).get("data", {})
    
    for grp in cfg['settings']['groups']:
        res[grp] = {}
        for ts in sorted(fact.keys(), key=int)[:2]:
            d = fact.get(ts, {}).get(grp)
            if not d:
                continue
            
            dt = datetime.fromtimestamp(int(ts), tz=KYIV_TZ)
            d_str = dt.strftime("%Y-%m-%d")
            
            if all(d.get(str(h), "yes") == "yes" for h in range(1, 25)):
                res[grp][d_str] = {"slots": None, "date": dt, "status": "pending"}
            else:
                res[grp][d_str] = {"slots": parse_github_day(d), "date": dt, "status": "normal"}
    return res


def extract_yasno(data: dict, cfg: dict) -> dict:
    res = {}
    if not data:
        return res
    
    for grp in cfg['settings']['groups']:
        key = grp.replace("GPV", "")
        if key not in data:
            continue
        
        res[grp] = {}
        for day in ["today", "tomorrow"]:
            d = data[key].get(day)
            if not d or "date" not in d:
                continue
            
            dt = datetime.fromisoformat(d["date"])
            d_str = dt.strftime("%Y-%m-%d")
            status = d.get("status", "")
            
            if status == "EmergencyShutdowns":
                res[grp][d_str] = {"slots": None, "date": dt, "status": "emergency"}
                continue
            
            if not d.get("slots"):
                res[grp][d_str] = {"slots": None, "date": dt, "status": "pending"}
                continue
            
            slots = [True] * 48
            for s in d["slots"]:
                start, end = s.get("start", 0) // 30, s.get("end", 0) // 30
                is_on = (s.get("type") == "NotPlanned")
                for i in range(start, min(end, 48)):
                    slots[i] = is_on
            
            res[grp][d_str] = {"slots": slots, "date": dt, "status": "normal"}
    return res


# === Processing ===

def slots_to_periods(slots: list[bool]) -> list[dict]:
    if not slots:
        return []
    periods = []
    curr, start = slots[0], 0
    for i in range(1, len(slots)):
        if slots[i] != curr:
            periods.append({
                "start": format_slot_time(start),
                "end": format_slot_time(i),
                "is_on": curr,
                "hours": (i - start) * 0.5
            })
            curr, start = slots[i], i
    periods.append({
        "start": format_slot_time(start),
        "end": format_slot_time(len(slots)),
        "is_on": curr,
        "hours": (len(slots) - start) * 0.5
    })
    return periods


def get_cache() -> dict:
    try:
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    except:
        return {"github": {}, "yasno": {}}


def save_cache(cache: dict):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


# === Formatting ===

def render_table(periods: list[dict], cfg: dict) -> str:
    """Render aligned ASCII table"""
    icons = cfg['ui']['icons']
    txt = cfg['ui']['text']
    
    # Column widths (fixed for alignment)
    COL1 = 13  # "Нема" column
    COL2 = 13  # "Є" column
    COL3 = 8   # "Час" column
    
    total_width = COL1 + COL2 + COL3 + 4  # +4 for separators
    
    # Build header
    header1 = f"{txt['off']:^{COL1}}|{txt['on']:^{COL2}}|{txt['time_header']:^{COL3}}"
    sep_line = "-" * total_width
    
    lines = [sep_line, header1, sep_line]
    
    total_on = 0.0
    total_off = 0.0
    
    for p in periods:
        time_range = f"{p['start']}-{p['end']}"
        dur = format_hours_short(p['hours'])
        
        if p['is_on']:
            # Empty | Time | Duration
            row = f"{'':{COL1}}|{time_range:^{COL2}}|{dur:^{COL3}}"
            total_on += p['hours']
        else:
            # Time | Empty | Duration
            row = f"{time_range:^{COL1}}|{'':{COL2}}|{dur:^{COL3}}"
            total_off += p['hours']
        
        lines.append(row)
    
    lines.append(sep_line)
    
    # Summary outside table with icons
    summary = [
        "",
        f"{icons['on']} {txt['on']}: {format_hours_full(total_on)}",
        f"{icons['off']} {txt['off']}: {format_hours_full(total_off)}"
    ]
    
    # Wrap table in <pre> for monospace
    table_text = "\n".join(lines)
    return f"<pre>{table_text}</pre>" + "\n".join(summary)


def render_list(periods: list[dict], cfg: dict) -> str:
    """Render simple list format"""
    icons = cfg['ui']['icons']
    txt = cfg['ui']['text']
    
    lines = []
    total_on = 0.0
    total_off = 0.0
    
    for p in periods:
        ico = icons['on'] if p['is_on'] else icons['off']
        lines.append(f"{ico} {p['start']} - {p['end']} … ({format_hours_full(p['hours'])})")
        if p['is_on']:
            total_on += p['hours']
        else:
            total_off += p['hours']
    
    lines.append("")
    lines.append(f"{icons['on']} {txt['on']}: {format_hours_full(total_on)}")
    lines.append(f"{icons['off']} {txt['off']}: {format_hours_full(total_off)}")
    return "\n".join(lines)


def format_day(data: dict, date: datetime, src: str, cfg: dict) -> str:
    """Format single day message"""
    ui = cfg['ui']
    d_str = date.strftime("%d.%m")
    day_name = DAYS_UA[date.weekday()]
    src_name = cfg['sources'].get(src, {}).get('name', src)
    
    lines = [f"{ui['icons']['calendar']}  {d_str} ({day_name}) [{src_name}]:", ""]
    
    st = data.get("status")
    if st == "emergency":
        lines.append(ui['text']['emergency'])
    elif st == "pending":
        lines.append(ui['text']['pending'])
    elif data.get("slots"):
        periods = slots_to_periods(data["slots"])
        if cfg['settings']['style'] == "table":
            lines.append(render_table(periods, cfg))
        else:
            lines.append(render_list(periods, cfg))
    
    return "\n".join(lines)


def format_msg(gh: dict, ya: dict, cfg: dict) -> Optional[str]:
    """Format complete message"""
    groups = cfg['settings']['groups']
    blocks = []
    
    for grp in groups:
        grp_num = grp.replace("GPV", "")
        header = cfg['ui']['format']['header_template'].format(group=grp_num)
        
        dates = set()
        if grp in gh:
            dates.update(gh[grp].keys())
        if grp in ya:
            dates.update(ya[grp].keys())
        
        if not dates:
            continue
        
        day_msgs = []
        for d_str in sorted(dates)[:2]:
            g_d = gh.get(grp, {}).get(d_str)
            y_d = ya.get(grp, {}).get(d_str)
            dt = (g_d or y_d)["date"]
            
            src_msgs = []
            
            # Check if both match
            match = False
            if g_d and y_d:
                if g_d['status'] == 'normal' and y_d['status'] == 'normal':
                    if g_d['slots'] == y_d['slots']:
                        match = True
            
            if match:
                # Combined source name
                names = f"{cfg['sources']['github']['name']}, {cfg['sources']['yasno']['name']}"
                base = format_day(g_d, dt, "github", cfg)
                base = base.replace(f"[{cfg['sources']['github']['name']}]", f"[{names}]")
                src_msgs.append(base)
            else:
                if g_d:
                    src_msgs.append(format_day(g_d, dt, "github", cfg))
                if y_d:
                    src_msgs.append(format_day(y_d, dt, "yasno", cfg))
            
            if src_msgs:
                day_msgs.append(f"\n{cfg['ui']['format']['separator_source']}\n".join(src_msgs))
        
        if day_msgs:
            body = f"\n{cfg['ui']['format']['separator_day']}\n".join(day_msgs)
            blocks.append(f"{header}\n{body}")
    
    if not blocks:
        return None
    
    now = get_kyiv_now().strftime("%d.%m.%Y %H:%M")
    footer = f"\n\n{cfg['ui']['icons']['clock']} {cfg['ui']['text']['updated']}: {now} (Київ)"
    return "\n\n\n".join(blocks) + footer


# === Telegram ===

def send_tg(text: str) -> Optional[int]:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID:
        return None
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHANNEL_ID, "text": text, "parse_mode": "HTML"},
            timeout=30
        )
        r.raise_for_status()
        return r.json()["result"]["message_id"]
    except Exception as e:
        print(f"Send failed: {e}")
        return None


def manage_msgs(mid: int):
    try:
        with open(MESSAGES_FILE, "r") as f:
            ids = json.load(f)
    except:
        ids = []
    
    # Pin new message
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/pinChatMessage",
        json={"chat_id": TELEGRAM_CHANNEL_ID, "message_id": mid, "disable_notification": True}
    )
    
    ids.append(mid)
    
    # Delete old messages
    while len(ids) > MAX_MESSAGES:
        old = ids.pop(0)
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteMessage",
            json={"chat_id": TELEGRAM_CHANNEL_ID, "message_id": old}
        )
    
    with open(MESSAGES_FILE, "w") as f:
        json.dump(ids, f)


# === Main ===

def main():
    cfg = load_config()
    
    print(f"Style: {cfg['settings']['style']}")
    print(f"Sources: github={cfg['sources']['github']['enabled']}, yasno={cfg['sources']['yasno']['enabled']}")
    
    print("\nFetching data...")
    gh_data = fetch_github(cfg)
    ya_data = fetch_yasno(cfg)
    
    print(f"GitHub: {'OK' if gh_data else 'SKIP/FAIL'}")
    print(f"Yasno: {'OK' if ya_data else 'SKIP/FAIL'}")
    
    if not gh_data and not ya_data:
        print("No data from any source")
        return
    
    gh_sched = extract_github(gh_data, cfg)
    ya_sched = extract_yasno(ya_data, cfg)
    
    # Serialize for cache comparison
    def serialize(s):
        r = {}
        for g, d in s.items():
            r[g] = {k: {"status": v["status"], "slots": v["slots"]} for k, v in d.items()}
        return r
    
    new_c = {"github": serialize(gh_sched), "yasno": serialize(ya_sched)}
    old_c = get_cache()
    
    if new_c == old_c:
        print("No changes.")
        return
    
    print("Updates detected!")
    msg = format_msg(gh_sched, ya_sched, cfg)
    
    if msg:
        print("\n" + "=" * 50)
        print(msg)
        print("=" * 50 + "\n")
        
        mid = send_tg(msg)
        if mid:
            manage_msgs(mid)
            save_cache(new_c)
            print("Done.")
        else:
            print("Failed to send message")
    else:
        print("No message generated")


if __name__ == "__main__":
    main()
