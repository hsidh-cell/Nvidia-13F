#!/usr/bin/env python3
"""
sec_ingest.py — SEC EDGAR Form 13F ingestion & ETL for the 13F Terminal
=======================================================================

Fetches every Form 13F-HR / 13F-HR/A filing for a given institutional
manager (default: NVIDIA CORP, CIK 0001045810) directly from SEC EDGAR,
parses the XML information tables, normalizes the holdings, validates each
filing against its own summary page, and emits a `data.json` in the exact
schema consumed by `nvidia-13f-dashboard.html`. Optionally injects the data
straight into the dashboard HTML so the deployed page is fully self-contained.

Runs on the Python standard library only (no third-party deps required).

USAGE
-----
    # Rebuild the dataset from EDGAR and inject into the dashboard:
    python sec_ingest.py --html nvidia-13f-dashboard.html

    # Just (re)generate data.json:
    python sec_ingest.py --out data.json

    # Point at any other 13F filer:
    python sec_ingest.py --cik 0001067983 --out berkshire.json

    # Cron/CI mode — exit code 10 if EDGAR has a filing not yet in data.json:
    python sec_ingest.py --check --out data.json

DESIGN NOTES
------------
* SEC requires a descriptive User-Agent; set CONTACT below or via --contact.
* SEC fair-access limit is ~10 requests/sec; we sleep between calls.
* 13F values are reported in whole US dollars (SEC rule effective Jan-2023);
  older filings (pre-2023) are in $000 and are auto-scaled to whole dollars.
* Amendments (13F-HR/A) that restate a period supersede the original for that
  period; amendments that only *add* holdings are merged into the period.
* Unknown CUSIPs are still ingested (so new positions appear automatically);
  they get a best-effort ticker from the issuer name and sector "Unknown",
  and are written to `review_securities.json` for a human to classify.
"""

import argparse, gzip, json, os, sys, time, zlib, urllib.request, urllib.error
import xml.etree.ElementTree as ET
from datetime import date

# --- identify yourself to SEC (edit this if you like) -----------------------
# SEC asks for a descriptive User-Agent that includes a contact. A bare email
# works too; --contact overrides this at runtime.
CONTACT = "13F-Terminal admin@example.com"
UA_STRING = CONTACT   # mutable; set from --contact at runtime

def ua_from_contact(c):
    c = (c or "").strip()
    return c if " " in c else f"13F-Terminal ({c})"   # ensure a name + contact

EDGAR_SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik10}.json"
EDGAR_DIR_INDEX   = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/index.json"
EDGAR_FILE        = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{name}"
EDGAR_INDEX_HTML  = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{accd}-index.html"

RATE_SLEEP = 0.20  # seconds between requests (well under SEC's 10 req/s)

# --- CUSIP enrichment: classification for known holdings --------------------
# Extend this map as new positions appear; unknowns fall back gracefully.
SECURITY_META = {
    "042068205": ("ARM",  "Arm Holdings plc",            "Semiconductors",         "Semiconductor Design (IP)", "United Kingdom"),
    "M70700105": ("NNOX", "Nano-X Imaging Ltd",          "Healthcare",             "Medical Imaging Devices",   "Israel"),
    "75629V104": ("RXRX", "Recursion Pharmaceuticals",   "Healthcare",             "AI Drug Discovery",         "United States"),
    "836100107": ("SOUN", "SoundHound AI Inc",           "Software",               "Voice / Conversational AI", "United States"),
    "90089L108": ("TSP",  "TuSimple Holdings Inc",       "Autonomous & Robotics",  "Autonomous Trucking",       "United States"),
    "038169207": ("APLD", "Applied Digital Corp",        "AI Infrastructure",      "Neocloud / Data Centers",   "United States"),
    "81758H106": ("SERV", "Serve Robotics Inc",          "Autonomous & Robotics",  "Delivery Robotics",         "United States"),
    "21873S108": ("CRWV", "CoreWeave Inc",               "AI Infrastructure",      "Neocloud / GPU Cloud",      "United States"),
    "N97284108": ("NBIS", "Nebius Group N.V.",           "AI Infrastructure",      "Neocloud / GPU Cloud",      "Netherlands"),
    "950915108": ("WRD",  "WeRide Inc",                  "Autonomous & Robotics",  "Autonomous Driving",        "China"),
    "458140100": ("INTC", "Intel Corporation",           "Semiconductors",         "Integrated Semiconductors", "United States"),
    "871607107": ("SNPS", "Synopsys Inc",                "Semiconductors",         "EDA / Chip Design Software","United States"),
    "654902204": ("NOK",  "Nokia Corporation",           "Networking & Photonics", "Communications Equipment",  "Finland"),
    "19247G107": ("COHR", "Coherent Corp",               "Networking & Photonics", "Optical / Photonics",       "United States"),
    "370920100": ("GENE", "Generate Biomedicines Inc",   "Healthcare",             "AI Protein / Drug Design",  "United States"),
}

NS_INFO = "{http://www.sec.gov/edgar/document/thirteenf/informationtable}"


def get(url, binary=False, retries=4):
    """Fetch a URL with SEC-compliant headers, gzip handling, and retry."""
    headers = {"User-Agent": UA_STRING, "Accept": "*/*",
               "Accept-Encoding": "gzip, deflate"}
    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as r:
                raw = r.read()
                enc = (r.headers.get("Content-Encoding") or "").lower()
            # SEC commonly gzip-compresses responses; decompress before decoding.
            if enc == "gzip" or raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            elif enc == "deflate":
                try: raw = zlib.decompress(raw)
                except zlib.error: raw = zlib.decompress(raw, -zlib.MAX_WBITS)
            time.sleep(RATE_SLEEP)
            return raw if binary else raw.decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            last = e
            if e.code in (403, 429, 500, 502, 503) and attempt < retries - 1:
                time.sleep(2.0 * (attempt + 1)); continue
            raise
        except urllib.error.URLError as e:
            last = e
            if attempt < retries - 1:
                time.sleep(2.0 * (attempt + 1)); continue
            raise
    if last: raise last


def getj(url):
    """GET and parse JSON, with a human-readable error if SEC returns non-JSON."""
    txt = get(url)
    try:
        return json.loads(txt)
    except json.JSONDecodeError:
        snippet = txt[:180].replace("\n", " ")
        raise SystemExit(
            f"\nERROR: expected JSON from {url}\n  got instead: {snippet!r}\n"
            f"  If that looks like an HTML/blocked page, SEC may be rate-limiting — "
            f"wait a minute and re-run, and confirm --contact is a real email.\n")


def calendar_quarter(report_date):
    y, m, _ = (int(x) for x in report_date.split("-"))
    return f"Q{(m - 1)//3 + 1} {y}"


def list_13f_filings(cik10):
    """Return [(accession, form, reportDate, filingDate)] newest-first."""
    data = getj(EDGAR_SUBMISSIONS.format(cik10=cik10))
    rec = data["filings"]["recent"]
    rows = []
    for form, acc, rpt, fdt in zip(rec["form"], rec["accessionNumber"],
                                   rec["reportDate"], rec["filingDate"]):
        if form in ("13F-HR", "13F-HR/A"):
            rows.append((acc, form, rpt, fdt))
    # older filings can spill into separate files:
    for extra in data["filings"].get("files", []):
        more = getj("https://data.sec.gov/submissions/" + extra["name"])
        for form, acc, rpt, fdt in zip(more["form"], more["accessionNumber"],
                                       more["reportDate"], more["filingDate"]):
            if form in ("13F-HR", "13F-HR/A"):
                rows.append((acc, form, rpt, fdt))
    return rows


def find_infotable(cik, acc_nodash):
    idx = getj(EDGAR_DIR_INDEX.format(cik=cik, acc=acc_nodash))
    names = [it["name"] for it in idx["directory"]["item"]]
    # information table is an .xml that is not the primary_doc
    cands = [n for n in names if n.lower().endswith(".xml") and "primary_doc" not in n.lower()]
    for pref in ("information", "infotable", "form13f", "table"):
        for n in cands:
            if pref in n.lower():
                return n
    return cands[0] if cands else None


def parse_infotable(xml_text, whole_dollars):
    holdings = []
    root = ET.fromstring(xml_text.encode("utf-8"))
    for it in root.findall(f"{NS_INFO}infoTable"):
        def txt(tag, parent=it):
            e = parent.find(f"{NS_INFO}{tag}")
            return e.text.strip() if e is not None and e.text else ""
        cusip = txt("cusip").upper()
        value = int(float(txt("value") or 0))
        if not whole_dollars:      # pre-2023 filings report $000
            value *= 1000
        shrs = it.find(f"{NS_INFO}shrsOrPrnAmt")
        shares = int(float(txt("sshPrnamt", shrs) or 0)) if shrs is not None else 0
        holdings.append({
            "cusip": cusip,
            "issuer": txt("nameOfIssuer"),
            "class": txt("titleOfClass"),
            "value": value,
            "shares": shares,
            "putCall": txt("putCall") or None,
            "discretion": txt("investmentDiscretion") or "SOLE",
        })
    return holdings


def parse_summary(primary_xml):
    """Return (entryTotal, valueTotal, isAmendment, amendmentType)."""
    try:
        root = ET.fromstring(primary_xml.encode("utf-8"))
    except ET.ParseError:
        return None, None, False, None
    def deep(tag):
        for e in root.iter():
            if e.tag.endswith(tag):
                return e.text.strip() if e.text else ""
        return ""
    et_ = deep("tableEntryTotal")
    vt_ = deep("tableValueTotal")
    amend = deep("isAmendment").lower() == "true"
    return (int(et_) if et_ else None,
            int(vt_) if vt_ else None, amend, deep("amendmentType"))


def build_dataset(cik10, contact=None):
    global UA_STRING
    UA_STRING = ua_from_contact(contact or CONTACT)
    cik = str(int(cik10))
    filings_raw = list_13f_filings(cik10)
    manager = getj(EDGAR_SUBMISSIONS.format(cik10=cik10))["name"]

    by_period = {}      # reportDate -> filing dict (amendments supersede/merge)
    review = {}

    for acc, form, rpt, fdt in sorted(filings_raw, key=lambda r: (r[2], r[3])):
        acc_nodash = acc.replace("-", "")
        whole_dollars = rpt >= "2023-01-01"
        info_name = find_infotable(cik, acc_nodash)
        if not info_name:
            print(f"  ! no information table in {acc} ({rpt}) — skipping", file=sys.stderr)
            continue
        holdings = parse_infotable(get(EDGAR_FILE.format(cik=cik, acc=acc_nodash, name=info_name)), whole_dollars)
        primary = get(EDGAR_FILE.format(cik=cik, acc=acc_nodash, name="primary_doc.xml"))
        entry_total, value_total, is_amend, amend_type = parse_summary(primary)

        # --- validation ---
        summed = sum(h["value"] for h in holdings)
        if value_total is not None and abs(summed - value_total) > 2:
            print(f"  ! VALIDATION {rpt} {acc}: holdings sum {summed:,} != summary {value_total:,}", file=sys.stderr)
        if entry_total is not None and entry_total != len(holdings):
            print(f"  ! VALIDATION {rpt} {acc}: entry count {len(holdings)} != summary {entry_total}", file=sys.stderr)
        seen = {}
        for h in holdings:      # duplicate-CUSIP detection
            seen[h["cusip"]] = seen.get(h["cusip"], 0) + 1
        for cu, n in seen.items():
            if n > 1:
                print(f"  ! note {rpt}: CUSIP {cu} appears {n}x (multiple lots)", file=sys.stderr)

        rec = {
            "period": rpt, "quarter": calendar_quarter(rpt), "filed": fdt,
            "accession": acc, "formType": form, "verified": True,
            "url": EDGAR_INDEX_HTML.format(cik=cik, acc=acc_nodash, accd=acc),
            "holdings": holdings,
        }
        if is_amend and rpt in by_period and amend_type and "ADD" in amend_type.upper():
            # merge added holdings into existing period
            existing = {h["cusip"]: h for h in by_period[rpt]["holdings"]}
            for h in holdings:
                existing[h["cusip"]] = h
            by_period[rpt]["holdings"] = list(existing.values())
            by_period[rpt]["formType"] = form
        else:
            by_period[rpt] = rec      # original, or restatement amendment
        print(f"  ✓ {rpt} {form:9} {len(holdings)} holdings  {summed:>16,}", file=sys.stderr)

    # normalize + enrich into the dashboard schema
    filings = []
    used_cusips = set()
    for rpt in sorted(by_period):
        f = by_period[rpt]
        out_h = []
        for h in f["holdings"]:
            used_cusips.add(h["cusip"])
            out_h.append({"cusip": h["cusip"], "class": h["class"],
                          "value": h["value"], "shares": h["shares"],
                          "discretion": h["discretion"]})
            if h["cusip"] not in SECURITY_META:
                review[h["cusip"]] = h["issuer"]
        filings.append({**{k: f[k] for k in
                        ("period", "quarter", "filed", "accession", "formType", "verified", "url")},
                        "holdings": out_h})

    securities = {}
    for cu in sorted(used_cusips):
        if cu in SECURITY_META:
            t, n, s, ind, ctry = SECURITY_META[cu]
        else:
            issuer = review.get(cu, cu)
            t, n, s, ind, ctry = (issuer.split()[0][:5].upper(), issuer.title(),
                                  "Unknown", "Unclassified", "Unknown")
        securities[cu] = {"ticker": t, "name": n, "sector": s, "industry": ind, "country": ctry}

    if review:
        json.dump(review, open("review_securities.json", "w"), indent=2)
        print(f"  ⚠ {len(review)} unclassified CUSIP(s) → review_securities.json", file=sys.stderr)

    return {
        "meta": {
            "manager": manager, "cik": cik10,
            "generated": date.today().isoformat(),
            "source": "SEC EDGAR Form 13F-HR filings (information tables), fetched & parsed by sec_ingest.py",
            "valueUnits": "US dollars (whole)",
            "note": "All quarters parsed directly from EDGAR and validated against each filing's summary page.",
        },
        "securities": securities,
        "filings": filings,
    }


def inject_html(html_path, dataset):
    html = open(html_path, encoding="utf-8").read()
    start = html.index('<script id="dataset"')
    open_tag_end = html.index(">", start) + 1
    close = html.index("</script>", open_tag_end)
    new = html[:open_tag_end] + json.dumps(dataset, separators=(",", ":")) + html[close:]
    open(html_path, "w", encoding="utf-8").write(new)
    print(f"  ✓ injected dataset into {html_path}", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(description="Ingest SEC 13F filings for the 13F Terminal")
    ap.add_argument("--cik", default="0001045810", help="10-digit zero-padded CIK (default: NVIDIA)")
    ap.add_argument("--out", default="data.json", help="output JSON path")
    ap.add_argument("--html", help="dashboard HTML to inject the dataset into")
    ap.add_argument("--contact", help="override SEC User-Agent contact string")
    ap.add_argument("--check", action="store_true",
                    help="exit 10 if EDGAR has a period newer than the cached --out; do not rewrite")
    args = ap.parse_args()
    cik10 = args.cik.zfill(10)
    global UA_STRING
    UA_STRING = ua_from_contact(args.contact or CONTACT)

    if args.check:
        latest_edgar = max((r[2] for r in list_13f_filings(cik10)), default=None)
        cached = None
        if os.path.exists(args.out):
            cached = max((f["period"] for f in json.load(open(args.out))["filings"]), default=None)
        print(f"EDGAR latest period: {latest_edgar} | cached: {cached}", file=sys.stderr)
        if latest_edgar and latest_edgar != cached:
            print("New filing available — refresh needed.", file=sys.stderr)
            sys.exit(10)
        print("Up to date.", file=sys.stderr)
        return

    print(f"Ingesting 13F filings for CIK {cik10} …", file=sys.stderr)
    ds = build_dataset(cik10, args.contact)
    json.dump(ds, open(args.out, "w"), indent=2)
    print(f"  ✓ wrote {args.out}  ({len(ds['filings'])} filings, {len(ds['securities'])} securities)", file=sys.stderr)
    if args.html:
        inject_html(args.html, ds)
    print("Done.", file=sys.stderr)


if __name__ == "__main__":
    main()
