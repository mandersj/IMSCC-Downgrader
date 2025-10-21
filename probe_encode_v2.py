#!/usr/bin/env python3

import zipfile, xml.etree.ElementTree as ET, io, urllib.parse
Z="cs-temp-dev-export-cc11.imscc"
z=zipfile.ZipFile(Z)
root=ET.parse(io.BytesIO(z.read("imsmanifest.xml"))).getroot()
bad=[]
for r in root.findall(".//{*}resource"):
    href=(r.get("href") or "").strip()
    if not href: continue
    if urllib.parse.quote(urllib.parse.unquote(href), safe="/-_.~")!=href:
        bad.append((r.get("identifier"), href))
print("MALFORMED_ENCODING:", len(bad))
for b in bad[:40]: print("  -", b)