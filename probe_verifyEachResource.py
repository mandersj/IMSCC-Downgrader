#!/usr/bin/env python3

import zipfile, xml.etree.ElementTree as ET, io, urllib.parse

Z = "cs-temp-dev-export-cc11.imscc"
z = zipfile.ZipFile(Z)
root = ET.parse(io.BytesIO(z.read("imsmanifest.xml"))).getroot()

def zget(path:str):
    try:
        return z.read(path)
    except KeyError:
        return None

empty_hits=[]
moodle_misses=[]
ok=0

for r in root.findall(".//{*}resource"):
    rtype=(r.get("type") or "").lower()
    if "webcontent" not in rtype:
        continue

    # Moodle prefers the FIRST <file href> if present; else resource@href
    fchild = r.find("{*}file")
    href = (fchild.get("href") if fchild is not None else r.get("href") or "").strip()
    if not href:
        continue

    # Moodle typically urldecode()s once before lookup
    decoded = urllib.parse.unquote(href)

    # Try decoded first (Moodle behavior), then raw
    data = zget(decoded)
    tried = [decoded]
    if data is None:
        data = zget(href)
        tried.append(href)

    if data is None:
        moodle_misses.append((r.get("identifier"), href, tried))
        continue

    # If it loads but is empty/whitespace -> DOMDocument::loadHTML('') will throw
    if not data.strip():
        empty_hits.append((r.get("identifier"), tried[0], len(data)))
    else:
        ok+=1

print("OK_WEBCONTENT:", ok)
print("MOODLE_PATH_MISS:", len(moodle_misses))
for rid, href, tried in moodle_misses[:40]:
    print("  -", rid, "href=", href, "tried=", tried)

print("EMPTY_OR_WHITESPACE:", len(empty_hits))
for rid, path, size in empty_hits[:40]:
    print("  -", rid, "path=", path, "size=", size)
