"""
BEAMON - Signups by Email Domain checker
Replicates the HubSpot report filters and alerts #sales_inbound on Slack
when a new email domain appears in the results.

Filters applied (matching the HubSpot report exactly):
  1. Create date updated in the last 30 days
  2. Email domain doesn't contain: gmail, hotmail, bryter, icloud, googlemail, outlook.com
  3. Recent conversion contains: "hubspot signup", "hubspot-signup", "BRYTER: F130"
     OR Recent conversion is unknown (null or empty)

All 3 filters are applied in Python after fetching from the API.
The API query uses only the createdate filter to stay within HubSpot's
6-filter-per-group limit, then Python replicates filters 2 and 3 exactly.
"""

import os
import json
import requests
from datetime import datetime, timedelta, timezone

HUBSPOT_API_TOKEN = os.environ["HUBSPOT_API_TOKEN"]
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
STATE_FILE = "known_domains.json"

EXCLUDED_DOMAIN_KEYWORDS = ["gmail", "hotmail", "bryter", "icloud", "googlemail", "outlook.com"]
ALLOWED_CONVERSIONS = {"hubspot signup", "hubspot-signup", "bryter: f130"}
REPORT_URL = "https://app-eu1.hubspot.com/reports-list/26891171/258485427/"


# ---------------------------------------------------------------------------
# HubSpot — fetch all contacts created in the last 30 days
# ---------------------------------------------------------------------------

def fetch_all_contacts():
    """
    Fetch contacts using only the createdate filter (filter 1).
    Filters 2 and 3 are applied in Python to exactly match the report.
    """
    url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_TOKEN}",
        "Content-Type": "application/json",
    }
    thirty_days_ago_ms = int(
        (datetime.now(timezone.utc) - timedelta(days=30)).timestamp() * 1000
    )
    filter_group = {
        "filters": [
            {"propertyName": "createdate", "operator": "GTE", "value": str(thirty_days_ago_ms)}
        ]
    }
    contacts = []
    after = None
    while True:
        payload = {
            "filterGroups": [filter_group],
            "properties": ["email", "hs_email_domain", "recent_conversion_event_name"],
            "limit": 100,
        }
        if after:
            payload["after"] = after
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        if not resp.ok:
            print(f"HubSpot API error {resp.status_code}: {resp.text}")
        resp.raise_for_status()
        data = resp.json()
        contacts.extend(data.get("results", []))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
    return contacts


# ---------------------------------------------------------------------------
# Python-side filtering — replicates report filters 2 and 3 exactly
# ---------------------------------------------------------------------------

def extract_domain(email):
    if email and "@" in email:
        return email.split("@", 1)[1].lower().strip()
    return None


def passes_filters(contact):
    props = contact.get("properties", {})

    # Filter 2: email domain must not contain excluded keywords
    domain = props.get("hs_email_domain") or extract_domain(props.get("email") or "")
    if not domain:
        return False
    if any(kw in domain for kw in EXCLUDED_DOMAIN_KEYWORDS):
        return False

    # Filter 3: recent conversion must be in allowed list OR unknown (null/empty)
    conversion = (props.get("recent_conversion_event_name") or "").strip().lower()
    if conversion and conversion not in ALLOWED_CONVERSIONS:
        return False

    return True


def compute_domain_counts(contacts):
    counts = {}
    for contact in contacts:
        props = contact.get("properties", {})
        domain = props.get("hs_email_domain") or extract_domain(props.get("email") or "")
        if domain:
            counts[domain] = counts.get(domain, 0) + 1
    return counts


# ---------------------------------------------------------------------------
# State file
# ---------------------------------------------------------------------------

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"known_domains": [], "last_checked": None}


def save_state(all_domains):
    state = {
        "known_domains": sorted(all_domains),
        "last_checked": datetime.now(timezone.utc).isoformat(),
    }
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)
    print(f"State saved - {len(all_domains)} total known domain(s).")


# ---------------------------------------------------------------------------
# Slack
# ---------------------------------------------------------------------------

def send_slack_alert(new_domains, domain_counts):
    count = len(new_domains)
    heading = "🚨 *New trial domain" + ("s" if count > 1 else "") + " detected in BEAMON*"
    lines = "\n".join(
        "• `" + d + "` — " + str(domain_counts.get(d, "?")) + " contact(s)"
        for d in sorted(new_domains)
    )
    text = (
        heading + "\n\n" +
        "The following email domain" + ("s" if count > 1 else "") +
        " ha" + ("ve" if count > 1 else "s") +
        " appeared in the *Signups by Email Domain* report:\n\n" +
        lines + "\n\n<" + REPORT_URL + "|View the full report>"
    )
    if SLACK_WEBHOOK_URL:
        resp = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)
        resp.raise_for_status()
        print("Slack alert sent for: " + str(sorted(new_domains)))
    else:
        print("--- DRY RUN (SLACK_WEBHOOK_URL not set) ---")
        print("Would have sent:\n" + text)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Fetching contacts from HubSpot (last 30 days)...")
    all_contacts = fetch_all_contacts()
    print(f"  {len(all_contacts)} contact(s) returned by API.")

    # Apply filters 2 and 3 in Python
    filtered = [c for c in all_contacts if passes_filters(c)]
    print(f"  {len(filtered)} contact(s) after applying report filters.")

    domain_counts = compute_domain_counts(filtered)
    current_domains = set(domain_counts.keys())
    print(f"  {len(current_domains)} unique domain(s).")
    for d, c in sorted(domain_counts.items()):
        print(f"    {d}: {c}")

    state = load_state()
    known_domains = set(state.get("known_domains", []))
    is_first_run = not known_domains
    new_domains = current_domains - known_domains

    if is_first_run:
        print("First run - seeding state file. No Slack alerts will be sent.")
    elif new_domains:
        print(f"  New domain(s): {sorted(new_domains)}")
        send_slack_alert(new_domains, domain_counts)
    else:
        print("  No new domains found.")

    save_state(known_domains | current_domains)

if __name__ == "__main__":
    main()
