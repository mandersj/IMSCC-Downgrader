#!/usr/bin/env python3

import zipfile, xml.etree.ElementTree as ET, io
Z = "cs-temp-dev-export-cc11.imscc"  # adjust path if needed
z = zipfile.ZipFile(Z)
tree = ET.parse(io.BytesIO(z.read("imsmanifest.xml")))
root = tree.getroot()

# grab all resource ids
res_ids = {r.get("identifier") for r in root.findall(".//{*}resource") if r.get("identifier")}

dangling = []
for itm in root.findall(".//{*}item"):
    ref = itm.get("identifierref")
    if ref and ref not in res_ids:
        title = itm.find("{*}title")
        title_text = (title.text.strip() if title is not None and title.text else "")
        dangling.append((ref, title_text))

print(f"DANGLING_ITEMS: {len(dangling)}")
for ref, title in dangling[:50]:
    print(f"  - identifierref={ref!r}  title={title!r}")
if len(dangling) > 50:
    print(f"  ... (+{len(dangling)-50} more)")
