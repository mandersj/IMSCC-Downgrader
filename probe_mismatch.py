#!/usr/bin/env python3

import sys, zipfile, io, os, urllib.parse, xml.etree.ElementTree as ET
Z = sys.argv[1] if len(sys.argv)>1 else input("Path to downgraded .imscc: ").strip()
with zipfile.ZipFile(Z) as z:
  root = ET.parse(io.BytesIO(z.read("imsmanifest.xml"))).getroot()
  bad=[]
  for r in root.findall(".//{*}resource"):
    href=(r.get("href") or "").strip()
    if not href: continue
    canon = urllib.parse.quote(urllib.parse.unquote(href), safe="/-_.~()")
    if href!=canon:
      bad.append((r.get("identifier"), href, canon))
  print("HREFS_NEED_CANONICAL:", len(bad))
  for x in bad: print("  -", x)