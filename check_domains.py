"""
BEAMON - Signups by Email Domain checker
Replicates the HubSpot report filters and alerts #sales_inbound on Slack
when a new email domain appears in the results.

Filters applied (matching the HubSpot report):
  1. Create date updated in the last 30 days
  2. Email domain doesn't contain: gmail, hotmail, bryter, icloud, googlemail, outlook.com
  3. Recent conversion contains: "hubspot signup", "hubspot-signup", "BRYTER: F130"
     OR Recent conversion is unknown
"""

import os
import json
import requests
from datetime import datetime, timedelta, timezone

HUBSPOT_API_TOKEN = os.environ["HUBSPOT_API_TOKEN"]
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
STATE_FILE = "known_domains.json"

EXCLUDED_DOMAIN_KEYWORDS = ["gmail", "hotmail", "bryter", "icloud", "googlemail", "outlook.com"]
REPORT_URL = "https://app-eu1.hubspot.com/reports-list/26891171/258485427/"


# ---------------------------------------------------------------------------
# HubSpot
# ---------------------------------------------------------------------------

def build_filter_groups():
    thirty_days_ago_ms = int(
        (datetime.now(timezone.utc) - timedelta(days=30)).timestamp() * 1000
    )
    base_filters = [
        {"propertyName": "createdate", "operator": "GTE", "value": str(thirty_days_ago_ms)}
    ]
    for keyword in EXCLUDED_DOMAIN_KEYWORDS:
        base_filters.append({"propertyName": "email", "operator": "NOT_CONTAINS_TOKEN", "value": keyword})

    group_a = {
        "filters": base_filters + [
            {"propertyName": "recent_conversion_event_name", "operator": "IN",
             "values": ["hubspot signup", "hubspot-signup", "BRYTER: F130"]}
        ]
    }
    group_b = {
        "filters": base_filters + [
            {"propertyName": "recent_conversion_event_name", "operator": "NOT_HAS_PROPERTY"}
        ]
    }
    return [group_a, group_b]


def fetch_all_contacts():
    url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    headers = {"Authorization": f"Bearer {HUBSPOT_API_TOKEN}", "Content-Type": "application/json"}
    contacts = []
    after = None
    while True:
        payload = {
            "filterGroups": build_filter_groups(),
            "properties": ["email", "hs_email_domain"],
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
# Domain helpers
# ---------------------------------------------------------------------------

def extract_domain(email):
    if email and "@" in email:
        return email.split("@", 1)[1].lower().strip()
    return None


def is_excluded(domain):
    return any(kw in domain for kw in EXCLUDED_DOMAIN_KEYWORDS)


def compute_domain_counts(contacts):
    counts = {}
    for contact in contacts:
        props = contact.get("properties", {})
        domain = props.get("hs_email_domain") or extract_domain(props.get("email") or "")
        if domain and not is_excluded(domain):
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
    print("Fetching contacts from HubSpot...")
    contacts = fetch_all_contacts()
    print(f"  {len(contacts)} contact(s) returned by API.")
    domain_counts = compute_domain_counts(contacts)
    current_domains = set(domain_counts.keys())
    print(f"  {len(current_domains)} unique domain(s) after filtering.")
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
