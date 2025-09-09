from notion_client.errors import APIResponseError
import re

def parse_db_id(val: str) -> str:
    """Accept plain ID or full Notion URL; return hyphenated UUID."""
    if not val:
        return val
    # Strip hyphens for detection, then re-hyphenate
    m = re.search(r'([0-9a-f]{32})', val.replace('-', ''), re.I)
    if not m:
        return val  # let Notion error out; at least we tried to parse
    raw = m.group(1).lower()
    return f"{raw[0:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:32]}"

def obf(s: str) -> str:
    return (s[:4] + "..." + s[-4:]) if s and len(s) > 8 else ("set" if s else "EMPTY")

def assert_db_access(db_id: str, label: str):
    try:
        info = notion.databases.retrieve(db_id)
        name = "".join(t.get("plain_text","") for t in info.get("title", []))
        print(f"[OK] {label}: {name!r} ({obf(db_id)})")
    except APIResponseError as e:
        raise RuntimeError(
            f"[ERROR] Cannot access {label} ({obf(db_id)}). "
            f"HTTP 404 usually means wrong ID or not shared.\n"
            f"Fix:\n"
            f"• Use the database ID (not view/page); if you pasted a full URL, store just the DB link or raw ID.\n"
            f"• Share the DB with this integration in Notion.\n"
            f"• Ensure token & DB are in the same workspace."
        ) from e
def main():
    # Read envs and normalize IDs (handles full URLs too)
    raw_master = os.environ.get("MASTER_DB_ID")
    raw_mirror = os.environ.get("MIRROR_DB_ID")
    if not os.environ.get("NOTION_TOKEN"):
        raise RuntimeError("Missing NOTION_TOKEN")
    if not raw_master or not raw_mirror:
        raise RuntimeError("MASTER_DB_ID and/or MIRROR_DB_ID are missing")

    master_db = parse_db_id(raw_master)
    mirror_db = parse_db_id(raw_mirror)

    print(f"Preflight… MASTER_DB_ID={obf(master_db)}  MIRROR_DB_ID={obf(mirror_db)}")
    assert_db_access(master_db, "MASTER_DB")
    assert_db_access(mirror_db, "MIRROR_DB")

    print("Fetching Master and Mirror pages...")
    def get_db_schema_types(db_id: str) -> dict:
    info = notion.databases.retrieve(db_id)
    props = info.get("properties", {})
    return {name: meta.get("type") for name, meta in props.items()}
    # Use master_db/mirror_db variables instead of the constants below





import os
from notion_client import Client
from tenacity import retry, wait_exponential, stop_after_attempt

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
MASTER_DB_ID = os.environ["MASTER_DB_ID"]
MIRROR_DB_ID = os.environ["MIRROR_DB_ID"]

# Property names in BOTH databases
PROP_ACTIVITY     = "Activity"     # Title
PROP_STATUS       = "Status"       # Select
PROP_START_DATE   = "Start Date"   # Date
PROP_DUE_DATE     = "Due Date"     # Date
PROP_PRIORITY     = "Priority"     # Select
PROP_RAISED_BY    = "Raised By"    # Select
PROP_ASSIGNED_TO  = "Assigned To"  # People in MASTER -> Multi-select in MIRROR


notion = Client(auth=NOTION_TOKEN)

@retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
def query_database(db_id, **kwargs):
    return notion.databases.query(database_id=db_id, **kwargs)

@retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
def create_page(parent_db_id, properties):
    return notion.pages.create(
        parent={"type": "database_id", "database_id": parent_db_id},
        properties=properties
    )

@retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
def update_page(page_id, properties):
    return notion.pages.update(page_id=page_id, properties=properties)

def get_all_pages(db_id):
    pages, start_cursor = [], None
    while True:
        resp = query_database(db_id, page_size=100, **({"start_cursor": start_cursor} if start_cursor else {}))
        pages.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")
    return pages

def get_title_text(page, title_prop_name):
    props = page.get("properties", {})
    title_prop = props.get(title_prop_name, {})
    if title_prop.get("type") != "title":
        return ""
    parts = title_prop.get("title", [])
    return "".join([t.get("plain_text", "") for t in parts]).strip()

def build_mirror_index_by_activity(mirror_pages):
    idx = {}
    for p in mirror_pages:
        activity = get_title_text(p, PROP_ACTIVITY)
        if activity:
            idx[activity.lower()] = p["id"]
    return idx

def prop_or_none(page, name):
    return page.get("properties", {}).get(name)

def extract_sync_properties_from_master(master_page):
    """
    Build properties for MIRROR from MASTER fields.
    - Raised By: Select -> Select
    - Assigned To: People (MASTER) -> Multi-select (MIRROR) using each person's display name
    """
    props = {}

    # STATUS (select)
    {"status": {"name": "<option>"}}
    def get_db_schema_types(db_id: str) -> dict:
    info = notion.databases.retrieve(db_id)
    return {name: meta.get("type") for name, meta in info["properties"].items()}
    # STATUS: read from Master (status or select), write to Mirror as its actual type
if "Status" in mirror_schema_types:
    st = prop_or_none(master_page, "Status")
    name = None
    if st:
        t = st.get("type")
        if t == "select":
            name = st.get("select", {}).get("name")
        elif t == "status":
            name = st.get("status", {}).get("name")

    mt = mirror_schema_types["Status"]  # "status", "select", or "multi_select"
    if mt == "status":
        props["Status"] = {"status": ({"name": name} if name else None)}
    elif mt == "select":
        props["Status"] = {"select": ({"name": name} if name else None)}
    elif mt == "multi_select":
        props["Status"] = {"multi_select": ([{"name": name}] if name else [])}

    # START DATE (date)
    sd_prop = prop_or_none(master_page, PROP_START_DATE)
    if sd_prop and sd_prop.get("type") == "date":
        props[PROP_START_DATE] = {"date": sd_prop.get("date")}

    # DUE DATE (date)
    dd_prop = prop_or_none(master_page, PROP_DUE_DATE)
    if dd_prop and dd_prop.get("type") == "date":
        props[PROP_DUE_DATE] = {"date": dd_prop.get("date")}

    # PRIORITY (select)
    pr_prop = prop_or_none(master_page, PROP_PRIORITY)
    if pr_prop and pr_prop.get("type") == "select":
        sel = pr_prop.get("select")
        props[PROP_PRIORITY] = {"select": {"name": sel["name"]}} if sel else {"select": None}

    # RAISED BY (select)
    rb_prop = prop_or_none(master_page, PROP_RAISED_BY)
    if rb_prop and rb_prop.get("type") == "select":
        sel = rb_prop.get("select")
        props[PROP_RAISED_BY] = {"select": {"name": sel["name"]}} if sel else {"select": None}

    # ASSIGNED TO: People (MASTER) -> Multi-select (MIRROR)
    at_prop = prop_or_none(master_page, PROP_ASSIGNED_TO)
    if at_prop and at_prop.get("type") == "people":
        people = at_prop.get("people", [])
        names = []
        for u in people:
            # Prefer 'name'; fall back to 'id' if missing.
            nm = u.get("name") or u.get("id")
            if nm:
                names.append(nm)
        props[PROP_ASSIGNED_TO] = {"multi_select": [{"name": n} for n in names]}
    else:
        props[PROP_ASSIGNED_TO] = {"multi_select": []}

    return props

def to_title_property(text):
    return {"title": [{"type": "text", "text": {"content": text or ""}}]}

def main():
    print("Fetching Master and Mirror pages...")
    master_pages = get_all_pages(MASTER_DB_ID)
    mirror_pages = get_all_pages(MIRROR_DB_ID)
    mirror_index = build_mirror_index_by_activity(mirror_pages)

    created, updated, skipped = 0, 0, 0

    for mp in master_pages:
        activity = get_title_text(mp, PROP_ACTIVITY)
        if not activity:
            skipped += 1
            continue

        mirror_props = extract_sync_properties_from_master(mp)
        mirror_id = mirror_index.get(activity.lower())

        if mirror_id:
            if mirror_props:
                update_page(mirror_id, mirror_props)
            updated += 1
            print(f"Updated: {activity}")
        else:
            new_props = {PROP_ACTIVITY: to_title_property(activity)}
            new_props.update(mirror_props)
            create_page(MIRROR_DB_ID, new_props)
            created += 1
            print(f"Created: {activity}")

    print(f"Done. Created: {created}, Updated: {updated}, Skipped (no title): {skipped}")

if __name__ == "__main__":
    main()
