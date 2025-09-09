import os
import re
from notion_client import Client
from notion_client.errors import APIResponseError
from tenacity import retry, wait_exponential, stop_after_attempt

# ========= Config: property names in BOTH DBs =========
PROP_ACTIVITY     = "Activity"     # Title
PROP_STATUS       = "Status"       # Status or Select in Master; any in Mirror
PROP_START_DATE   = "Start Date"   # Date
PROP_DUE_DATE     = "Due Date"     # Date
PROP_PRIORITY     = "Priority"     # Select in Master; optional in Mirror
PROP_RAISED_BY    = "Raised By"    # Select in Master
PROP_ASSIGNED_TO  = "Assigned To"  # People in Master

# Optional mapping if Master values differ from Mirror (esp. Status)
STATUS_MAP = {
    # "In progress": "In Progress",
    # "WIP": "In Progress",
}

# ========= Env / Client =========
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
RAW_MASTER_DB_ID = os.environ.get("MASTER_DB_ID")
RAW_MIRROR_DB_ID = os.environ.get("MIRROR_DB_ID")

if not NOTION_TOKEN:
    raise RuntimeError("Missing NOTION_TOKEN")
if not RAW_MASTER_DB_ID or not RAW_MIRROR_DB_ID:
    raise RuntimeError("MASTER_DB_ID and/or MIRROR_DB_ID are missing")

notion = Client(auth=NOTION_TOKEN)


# ========= Helpers: IDs, preflight, schema =========
def parse_db_id(val: str) -> str:
    """Accept plain ID or full Notion URL; return hyphenated UUID."""
    if not val:
        return val
    m = re.search(r'([0-9a-f]{32})', val.replace("-", ""), re.I)
    if not m:
        return val  # let Notion error out; at least we tried to parse
    raw = m.group(1).lower()
    return f"{raw[0:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:32]}"

def obf(s: str) -> str:
    return (s[:4] + "..." + s[-4:]) if s and len(s) > 8 else ("set" if s else "EMPTY")

def assert_db_access(db_id: str, label: str):
    """Fail fast if the DB is not accessible to this integration."""
    try:
        info = notion.databases.retrieve(db_id)
        name = "".join(t.get("plain_text", "") for t in info.get("title", []))
        print(f"[OK] {label}: {name!r} ({obf(db_id)})")
    except APIResponseError as e:
        raise RuntimeError(
            f"[ERROR] Cannot access {label} ({obf(db_id)}). "
            f"HTTP {getattr(e, 'status', 'n/a')}: {getattr(e, 'message', str(e))}\n"
            f"Fix:\n"
            f"• Use the DATABASE ID (not a view/page ID). URLs: use the part before ?v=...\n"
            f"• Share the DB with this integration (Share → Invite → your integration → Can edit)\n"
            f"• Ensure the token’s workspace matches the DB’s workspace"
        ) from e

def get_db_schema_types(db_id: str) -> dict:
    """Return {prop_name: type} for a database."""
    info = notion.databases.retrieve(db_id)
    props = info.get("properties", {})
    return {name: meta.get("type") for name, meta in props.items()}

def coerce_choice_payload(name: str | None, mirror_type: str):
    """
    Build a payload for choice-like properties depending on Mirror type.
    - select       -> {"select": {"name": name}} or {"select": None}
    - multi_select -> {"multi_select": [{"name": name}]} or {"multi_select": []}
    - status       -> {"status": {"name": name}} or {"status": None}
    """
    if mirror_type == "select":
        return {"select": ({"name": name} if name else None)}
    if mirror_type == "multi_select":
        return {"multi_select": ([{"name": name}] if name else [])}
    if mirror_type == "status":
        return {"status": ({"name": name} if name else None)}
    return None  # unsupported type


# ========= Notion API wrappers =========
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


# ========= Page utilities =========
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


# ========= Property extraction / mapping =========
def extract_sync_properties_from_master(master_page, mirror_schema_types: dict):
    """
    Build properties for MIRROR from MASTER fields, adapting to Mirror schema:
    - Skips properties missing in Mirror
    - Chooses status/select/multi_select based on Mirror types
    - Assigned To: People (Master) -> Select or Multi-select (Mirror)
    - Raised By:  Select (Master)  -> Select or Multi-select (Mirror)
    """
    props = {}

    # STATUS (Master may be select or status) -> Mirror's actual type
    if PROP_STATUS in mirror_schema_types:
        st = prop_or_none(master_page, PROP_STATUS)
        name = None
        if st:
            t = st.get("type")
            if t == "select":
                name = st.get("select", {}).get("name")
            elif t == "status":
                name = st.get("status", {}).get("name")
        if name in STATUS_MAP:
            name = STATUS_MAP[name]
        payload = coerce_choice_payload(name, mirror_schema_types[PROP_STATUS])
        if payload: props[PROP_STATUS] = payload

    # START DATE
    if PROP_START_DATE in mirror_schema_types:
        sd = prop_or_none(master_page, PROP_START_DATE)
        if sd and sd.get("type") == "date" and mirror_schema_types[PROP_START_DATE] == "date":
            props[PROP_START_DATE] = {"date": sd.get("date")}

    # DUE DATE
    if PROP_DUE_DATE in mirror_schema_types:
        dd = prop_or_none(master_page, PROP_DUE_DATE)
        if dd and dd.get("type") == "date" and mirror_schema_types[PROP_DUE_DATE] == "date":
            props[PROP_DUE_DATE] = {"date": dd.get("date")}

    # PRIORITY (Select in Master; optional in Mirror)
    if PROP_PRIORITY in mirror_schema_types:
        pr = prop_or_none(master_page, PROP_PRIORITY)
        name = pr.get("select", {}).get("name") if (pr and pr.get("type") == "select") else None
        payload = coerce_choice_payload(name, mirror_schema_types[PROP_PRIORITY])
        if payload: props[PROP_PRIORITY] = payload

    # RAISED BY (Select in Master) -> Mirror select or multi_select
    if PROP_RAISED_BY in mirror_schema_types:
        rb = prop_or_none(master_page, PROP_RAISED_BY)
        name = rb.get("select", {}).get("name") if (rb and rb.get("type") == "select") else None
        mt = mirror_schema_types[PROP_RAISED_BY]
        if mt == "multi_select":
            props[PROP_RAISED_BY] = {"multi_select": ([{"name": name}] if name else [])}
        elif mt == "select":
            props[PROP_RAISED_BY] = {"select": ({"name": name} if name else None)}
        # else: unsupported type → skip

    # ASSIGNED TO: People (MASTER) -> Select or Multi-select (MIRROR)
    if PROP_ASSIGNED_TO in mirror_schema_types:
        at = prop_or_none(master_page, PROP_ASSIGNED_TO)
        mt = mirror_schema_types[PROP_ASSIGNED_TO]
        if at and at.get("type") == "people":
            people = at.get("people", [])
            names = []
            for u in people:
                nm = u.get("name") or u.get("id")
                if nm:
                    names.append(nm)
            if mt == "multi_select":
                props[PROP_ASSIGNED_TO] = {"multi_select": [{"name": n} for n in names]}
            elif mt == "select":
                first = names[0] if names else None
                props[PROP_ASSIGNED_TO] = {"select": ({"name": first} if first else None)}
        else:
            # no people value in master; clear mirror multi/select
            if mt == "multi_select":
                props[PROP_ASSIGNED_TO] = {"multi_select": []}
            elif mt == "select":
                props[PROP_ASSIGNED_TO] = {"select": None}

    return props

def to_title_property(text):
    return {"title": [{"type": "text", "text": {"content": text or ""}}]}


# ========= Main =========
def main():
    master_db = parse_db_id(RAW_MASTER_DB_ID)
    mirror_db = parse_db_id(RAW_MIRROR_DB_ID)

    print(f"Preflight… MASTER_DB_ID={obf(master_db)}  MIRROR_DB_ID={obf(mirror_db)}")
    assert_db_access(master_db, "MASTER_DB")
    assert_db_access(mirror_db, "MIRROR_DB")

    print("Fetching Master and Mirror pages...")
    master_pages = get_all_pages(master_db)
    mirror_pages = get_all_pages(mirror_db)
    mirror_schema_types = get_db_schema_types(mirror_db)

    mirror_index = build_mirror_index_by_activity(mirror_pages)

    created, updated, skipped = 0, 0, 0

    for mp in master_pages:
        activity = get_title_text(mp, PROP_ACTIVITY)
        if not activity:
            skipped += 1
            continue

        mirror_props = extract_sync_properties_from_master(mp, mirror_schema_types)
        mirror_id = mirror_index.get(activity.lower())

        if mirror_id:
            if mirror_props:
                update_page(mirror_id, mirror_props)
            updated += 1
            print(f"Updated: {activity}")
        else:
            new_props = {PROP_ACTIVITY: to_title_property(activity)}
            new_props.update(mirror_props)
            create_page(mirror_db, new_props)
            created += 1
            print(f"Created: {activity}")

    print(f"Done. Created: {created}, Updated: {updated}, Skipped (no title): {skipped}")


if __name__ == "__main__":
    main()
