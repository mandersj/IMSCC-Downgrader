#!/usr/bin/env python3
import zipfile, xml.etree.ElementTree as ET, io, sys

Z = "cs-temp-dev-export-cc11.imscc"  # adjust path if needed
z = zipfile.ZipFile(Z)
man = z.read("imsmanifest.xml")
tree = ET.parse(io.BytesIO(man))
root = tree.getroot()

ns = {"imscp": "http://www.imsglobal.org/xsd/imscp_v1p1"}
# fallback: match any namespace
def findall_resources():
    return root.findall(".//{*}resource")

namelist = set(z.namelist())

issues = {"NO_HREF": [], "MISSING": [], "DIR": [], "WHITESPACE_ONLY": [], "NOFILE_CHILD": [], "FILE_MISMATCH": []}

for res in findall_resources():
    rid = res.get("identifier") or "(unknown)"
    rtype = res.get("type") or ""
    href = (res.get("href") or "").strip()

    # webcontent-ish?
    is_page_like = href.lower().endswith((".html",".htm")) or "webcontent" in rtype.lower() or "learning-application-resource" in rtype.lower()

    # 1) no href at all
    if not href:
        issues["NO_HREF"].append((rid, rtype, href))
        continue

    # 2) directory or missing
    if href.endswith("/"):
        issues["DIR"].append((rid, href))
    if href not in namelist:
        # try common normalizations (POSIX was already used; still check)
        alt = href.lstrip("./")
        if alt not in namelist:
            issues["MISSING"].append((rid, href))
            continue

    # 3) whitespace-only HTML
    if href.lower().endswith((".html",".htm")):
        try:
            data = z.read(href)
        except KeyError:
            continue
        if not data.strip():
            issues["WHITESPACE_ONLY"].append((rid, href))

    # 4) file children sanity
    files = [f.get("href","").strip() for f in res.findall("{*}file")]
    if not files:
        issues["NOFILE_CHILD"].append((rid, href))
    elif href not in files:
        issues["FILE_MISMATCH"].append((rid, href, files[:3]))  # show a few

# Summary
def show(name, arr, limit=15):
    print(f"{name}: {len(arr)}")
    for row in arr[:limit]:
        print("  ", row)
    if len(arr) > limit:
        print(f"  ... (+{len(arr)-limit} more)")
for k in ["NO_HREF","MISSING","DIR","WHITESPACE_ONLY","NOFILE_CHILD","FILE_MISMATCH"]:
    show(k, issues[k])
