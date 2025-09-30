#!/usr/bin/env python3
"""
imscc_12_to_11_downgrader.py

Purpose
-------
Given a Canvas-exported IMS Common Cartridge (IMSCC) package that
uses the v1.2 profile (or embeds v1.2 schema URIs), create a best‑effort
v1.1‑compatible cartridge suitable for Moodle import.

What it does (safe, "easy" downgrades)
--------------------------------------
1) Unzips the .imscc/.zip into a temp folder.
2) Edits **imsmanifest.xml** to:
   • Flip any CC version metadata ≥ 1.2.0 -> 1.1.0
   • Replace CC v1.2 schema/profile URIs with v1.1 equivalents.
   • Remove **Curriculum Standards** metadata (1.2+ namespace) cleanly.
   • Change webcontent `intendedUse="assignment"` to `unspecified`.
   • **Remove LTI resources entirely** (policy) and prune references.
   • Remove CC "assignment" extension resources; prune references.
   • Downshift certain **resource@type** strings (e.g., imsdt/imswl v1p2 -> v1p1).
   • Remove dangling `<dependency>` references to deleted resources.
3) Rewrites discussion/weblink descriptor XMLs that reference v1p2
   schemas (imsdt_v1p2, imswl_v1p2) to their v1p1 counterparts.
4) **QTI audit:** Detects QTI 2.x and Canvas‑flavored QTI 1.2; reports
   potential import risk (no rewrites).
5) Repackages everything as a new **.imscc** file.
6) Prints a concise change report.

What it does *not* do (on purpose)
----------------------------------
• It does not rewrite QTI assessment internals. If a quiz uses 1.2‑only
  logic (e.g., fill‑in‑the‑blank OR), Moodle may still drop/alter items.
• It does not promise a lossless round‑trip.

Usage
-----
$ python3 imscc_12_to_11_downgrader.py
  (interactive prompts for input file/dir and output dir)

Or:
$ python3 imscc_12_to_11_downgrader.py --input /path/to/file.imscc --output /dest/dir

Tested with Python 3.11+ and designed for 3.13.
No third‑party deps; macOS‑friendly.

Notes
-----
• If you pass a directory for --input, the first *.imscc file in it will be used.
• Always validate the output by importing into a Moodle sandbox and/or using
  a CC validator if you have access.
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import zipfile
import xml.etree.ElementTree as ET

# ---------------------------------------------------------------------------
# XML / CC helpers
# ---------------------------------------------------------------------------

def localname(tag: str) -> str:
    if tag.startswith("{"):
        return tag.split("}", 1)[1]
    return tag

XSI = '{http://www.w3.org/2001/XMLSchema-instance}'

# CC schema/profile replacements (textual)
# Order matters: handle v1p3 → v1p1 first, then v1p2 → v1p1, then descriptor schema tags.
V12_TO_V11_SCHEMA_REPLACEMENTS = [
    # --- Default CC namespace downshift (manifest root) ---
    ("http://www.imsglobal.org/xsd/imsccv1p3/imscp_v1p1",
     "http://www.imsglobal.org/xsd/imsccv1p1/imscp_v1p1"),

    # --- LOM namespaces (manifest/resource) ---
    ("http://ltsc.ieee.org/xsd/imsccv1p3/LOM/manifest",
     "http://ltsc.ieee.org/xsd/imsccv1p1/LOM/manifest"),
    ("http://ltsc.ieee.org/xsd/imsccv1p3/LOM/resource",
     "http://ltsc.ieee.org/xsd/imsccv1p1/LOM/resource"),

    # --- CC profile schemaLocations (ccv1p3 -> ccv1p1) ---
    ("http://www.imsglobal.org/profile/cc/ccv1p3/ccv1p3_imscp_v1p2_v1p0.xsd",
     "http://www.imsglobal.org/profile/cc/ccv1p1/ccv1p1_imscp_v1p1_v1p0.xsd"),
    ("http://www.imsglobal.org/profile/cc/ccv1p3/LOM/ccv1p3_lommanifest_v1p0.xsd",
     "http://www.imsglobal.org/profile/cc/ccv1p1/LOM/ccv1p1_lommanifest_v1p0.xsd"),
    ("http://www.imsglobal.org/profile/cc/ccv1p3/LOM/ccv1p3_lomresource_v1p0.xsd",
     "http://www.imsglobal.org/profile/cc/ccv1p1/LOM/ccv1p1_lomresource_v1p0.xsd"),

    # --- Generic v1p3 → v1p1 fallbacks (keep after specific swaps) ---
    ("/xsd/imsccv1p3/", "/xsd/imsccv1p1/"),
    ("/profile/cc/ccv1p3/", "/profile/cc/ccv1p1/"),

    # --- Existing v1p2 → v1p1 swaps (still needed for mixed carts) ---
    ("/xsd/imsccv1p2/", "/xsd/imsccv1p1/"),
    ("/profile/cc/ccv1p2/", "/profile/cc/ccv1p1/"),

    # Descriptor schemas for discussions & weblinks (manifest and XML files)
    ("imsdt_v1p2", "imsdt_v1p1"),
    ("imswl_v1p2", "imswl_v1p1"),

    # --- Remove CP extension (no 1.1 equivalent) ---
    ("http://www.imsglobal.org/xsd/imsccv1p3/imscp_extensionv1p2", ""),
    ("http://www.imsglobal.org/profile/cc/ccv1p3/ccv1p3_cpextensionv1p2_v1p0.xsd", ""),
]

# Patterns to remove CP extension (imscp_extensionv1p2) artifacts outright
# - Drop xmlns:cpx / xmlns:ns3 declarations
CP_EXTENSION_XMLNS_DROP_REGEX = r'\s+xmlns:(?:cpx|ns3)="[^"]+"'
# - Drop <cpx:variant>...</cpx:variant> or alias <ns3:variant>...</ns3:variant>
CP_EXTENSION_VARIANT_DROP_REGEX = r'<(?:cpx|ns3):variant\b[^>]*>.*?</(?:cpx|ns3):variant>'

# CC 1.2+ Curriculum Standards namespace hint
CSM_HINT = 'imscsmd'

# Assignment extension identifiers sometimes seen in later CC versions
ASSIGNMENT_TYPE_PREFIXES = (
    'assignment_xmlv',                 # e.g., assignment_xmlv1p0
    'associatedcontent/imscc_xmlv1p1/assignment',  # seen in some carts
)

ASSIGNMENT_HREF_HINTS = (
    '/assignment.xml',                 # folder/assignment.xml
    'assignment.xml',                  # general fallback
)

def remove_assignment_extension_resources(tree: ET.ElementTree, log: ChangeLog,
                                          res_id_to_titles: Dict[str, Set[str]]) -> Set[str]:
    root = tree.getroot()
    resources_parent = root.find('.//{*}resources')
    if resources_parent is None:
        return set()

    to_remove: List[ET.Element] = []
    for res in list(resources_parent.findall('{*}resource')):
        rtype = (res.get('type') or '').lower()
        href = (res.get('href') or '').lower()

        type_hit = any(rtype.startswith(pfx) for pfx in ASSIGNMENT_TYPE_PREFIXES)
        href_hit = any(h in href for h in ASSIGNMENT_HREF_HINTS)

        if type_hit or href_hit:
            to_remove.append(res)

    removed_ids: Set[str] = set()
    for res in to_remove:
        rid = res.get('identifier') or 'UNKNOWN'
        resources_parent.remove(res)
        removed_ids.add(rid)
        titles = sorted(res_id_to_titles.get(rid, []))
        log.removed_assignment_resources.append((rid, "; ".join(titles) if titles else None))

    return removed_ids

# Allowed v1.1 intendedUse values
V11_INTENDED_USE_VALUES = {"lessonplan", "syllabus", "unspecified"}

# QTI heuristics --------------------------------------------------------------

QTI2_NAMESPACE_HINTS = (
    'imsqti_v2p',  # e.g., imsqti_v2p1, imsqti_v2p2
    'imsqti_v2',
)
QTI2_ROOTS = {'assessmentItem', 'assessmentTest'}
QTI12_ROOT = 'questestinterop'
VENDOR_HINTS = ('canvas', 'instructure')


class ChangeLog:
    def __init__(self) -> None:
        self.intended_use_changes: List[Tuple[str, str, str]] = []
        self.removed_cs_blocks: int = 0
        self.removed_assignment_resources: List[Tuple[str, Optional[str]]] = []
        self.removed_lti_resources: List[Tuple[str, Optional[str]]] = []
        self.descriptor_schema_patches: List[Path] = []
        self.resource_type_downshifts: List[Tuple[str, str, str]] = []
        self.version_bumped: Optional[str] = None
        self.namespaces_downgraded: bool = False
        self.manifest_path: Optional[Path] = None
        # (resource_id, titles, classification, notes)
        self.qti_audit: List[Tuple[str, Optional[str], str, Optional[str]]] = []

        # NEW: QTI T/F normalization counters
        self.tf_fixed: int = 0
        self.tf_mc_fallback: int = 0
        self.tf_skipped: int = 0

    def print_summary(self) -> None:
        print("\n=== IMSCC v1.2 -> v1.1 Downgrade Summary ===")
        if self.version_bumped:
            print(f"- Manifest schemaversion set: {self.version_bumped}")
        print(f"- Curriculum Standards metadata blocks removed: {self.removed_cs_blocks}")

        if self.intended_use_changes:
            print("- intendedUse reassigned (assignment -> unspecified) for resources:")
            for rid, old, new in self.intended_use_changes:
                print(f"    • {rid}: {old} -> {new}")

        if self.resource_type_downshifts:
            print("- resource@type downshifted:")
            for rid, old, new in self.resource_type_downshifts:
                print(f"    • {rid}: {old} -> {new}")

        if self.removed_assignment_resources:
            print("- Assignment extension resources removed (item titles shown when resolved):")
            for rid, title in self.removed_assignment_resources:
                label = f"{rid}"
                if title:
                    label += f"  [item: {title}]"
                print(f"    • {label}")

        if self.removed_lti_resources:
            print("- LTI resources removed (by policy):")
            for rid, title in self.removed_lti_resources:
                label = f"{rid}"
                if title:
                    label += f"  [item: {title}]"
                print(f"    • {label}")

        if self.descriptor_schema_patches:
            print("- Descriptor XMLs patched from v1p2 -> v1p1 schemas:")
            for p in self.descriptor_schema_patches:
                print(f"    • {p}")

        if self.namespaces_downgraded:
            print("- Manifest schemaLocation / ns URIs updated from v1p2 -> v1p1")

        if self.qti_audit:
            print("- QTI audit (no rewrites performed):")
            for rid, titles, cls, note in self.qti_audit:
                t = f" [items: {titles}]" if titles else ""
                n = f" — {note}" if note else ""
                print(f"    • {rid}{t}: {cls}{n}")
                
        # NEW: T/F normalization summary
        if (self.tf_fixed or self.tf_mc_fallback or self.tf_skipped):
            print("- QTI True/False normalization:")
            if self.tf_fixed:
                print(f"    • normalized to 'true'/'false': {self.tf_fixed}")
            if self.tf_mc_fallback:
                print(f"    • converted to 2-option multiple choice: {self.tf_mc_fallback}")
            if self.tf_skipped:
                print(f"    • skipped (ambiguous/malformed): {self.tf_skipped}")

        print("===============================================\n")

# Core helpers ----------------------------------------------------------------

def find_first_imscc(path: Path) -> Optional[Path]:
    if path.is_file() and path.suffix.lower() in {'.imscc', '.zip'}:
        return path
    if path.is_dir():
        for p in sorted(path.glob('*.imscc')) + sorted(path.glob('*.zip')):
            return p
    return None


def unzip_to_temp(src_zip: Path) -> Path:
    tmpdir = Path(tempfile.mkdtemp(prefix='cc12to11_'))
    with zipfile.ZipFile(src_zip, 'r') as z:
        z.extractall(tmpdir)
    return tmpdir

def assert_no_v13_v12_uris(manifest_text: str) -> None:
    bad = re.findall(r'imsccv1p3|imscp_extensionv1p2|ccv1p3', manifest_text)
    if bad:
        raise ValueError(f"Leftover v1p3/v1p2 markers found: {set(bad)}")

def rezip_folder(src_folder: Path, dest_zip: Path) -> None:
    dest_zip = dest_zip.with_suffix('.imscc')
    with zipfile.ZipFile(dest_zip, 'w', compression=zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src_folder):
            for fname in files:
                fpath = Path(root) / fname
                arcname = str(fpath.relative_to(src_folder))
                z.write(fpath, arcname)


def load_manifest(manifest_path: Path) -> ET.ElementTree:
    return ET.parse(manifest_path)


def save_manifest(tree: ET.ElementTree, manifest_path: Path) -> None:
    tree.write(manifest_path, encoding='utf-8', xml_declaration=True)


def element_iter(el: ET.Element):
    yield el
    for child in list(el):
        yield from element_iter(child)


def iter_with_parent(el: ET.Element):
    for child in list(el):
        yield el, child
        yield from iter_with_parent(child)
        
def drop_cp_variants(tree: ET.ElementTree) -> int:
    """Remove any <variant> elements (CP extension) in the manifest."""
    root = tree.getroot()
    removed = 0
    for parent in list(element_iter(root)):
        for child in list(parent):
            if localname(child.tag) == 'variant':
                parent.remove(child)
                removed += 1
    return removed
        
def manifest_cc_version(tree: ET.ElementTree) -> Optional[str]:
    for sv in tree.getroot().findall('.//{*}schemaversion'):
        if sv.text:
            return sv.text.strip()
    return None

# ------------------ Manifest edits & pruning --------------------------------

def prune_curriculum_standards(tree: ET.ElementTree, log: ChangeLog) -> None:
    """Remove Curriculum Standards metadata based on namespace (safer than localname)."""
    root = tree.getroot()
    removed = 0
    for parent in list(element_iter(root)):
        for child in list(parent):
            tag = child.tag
            if tag.startswith('{'):
                ns = tag.split('}', 1)[0].strip('{')
                if CSM_HINT in ns:
                    parent.remove(child)
                    removed += 1
    if removed:
        log.removed_cs_blocks += removed


def build_res_id_to_item_titles(tree: ET.ElementTree) -> Dict[str, Set[str]]:
    root = tree.getroot()
    mapping: Dict[str, Set[str]] = {}
    for org in root.findall('.//{*}organization'):
        for item in org.findall('.//{*}item'):
            rid = item.get('identifierref')
            title_el = item.find('{*}title')
            title = title_el.text.strip() if title_el is not None and title_el.text else None
            if rid and title:
                mapping.setdefault(rid, set()).add(title)
    return mapping


def patch_intended_use(tree: ET.ElementTree, log: ChangeLog) -> None:
    root = tree.getroot()
    for res in root.findall('.//{*}resource'):
        for attr in ('intendedUse', 'intendeduse'):
            iu = res.get(attr)
            if iu and iu.lower() == 'assignment':
                res.set(attr, 'unspecified')
                log.intended_use_changes.append((res.get('identifier', 'UNKNOWN'), iu, 'unspecified'))


def downshift_resource_types(tree: ET.ElementTree, log: ChangeLog) -> None:
    """Re-map known v1p2 resource types to v1p1 where such types exist.
    NOTE: Do NOT touch QTI 'xmlv1p2' (that’s QTI 1.2, not CC 1.2).
    """
    replacements = {
        'imsdt_xmlv1p2': 'imsdt_xmlv1p1',  # discussion topic
        'imswl_xmlv1p2': 'imswl_xmlv1p1',  # weblink
    }
    root = tree.getroot()
    for res in root.findall('.//{*}resource'):
        rid = res.get('identifier', 'UNKNOWN')
        rtype = res.get('type', '')
        new_type = rtype
        for old, new in replacements.items():
            if old in new_type:
                new_type = new_type.replace(old, new)
        if new_type != rtype:
            res.set('type', new_type)
            log.resource_type_downshifts.append((rid, rtype, new_type))


def remove_items_referring_to(tree: ET.ElementTree, removed_ids: Set[str]) -> None:
    root_el = tree.getroot()
    for parent, item in iter_with_parent(root_el):
        if localname(item.tag) == 'item' and item.get('identifierref') in removed_ids:
            parent.remove(item)


def remove_dependencies_referring_to(tree: ET.ElementTree, removed_ids: Set[str]) -> None:
    root = tree.getroot()
    for res in root.findall('.//{*}resource'):
        for dep in list(res.findall('{*}dependency')):
            if dep.get('identifierref') in removed_ids:
                res.remove(dep)


# ----------------------------- LTI removal ----------------------------------

def is_lti_descriptor(xml_path: Path) -> bool:
    try:
        txt = xml_path.read_text(encoding='utf-8', errors='ignore')
        root = ET.fromstring(txt)
    except Exception:
        return False
    lname = localname(root.tag)
    ns = ''
    if root.tag.startswith('{'):
        ns = root.tag.split('}')[0].strip('{')
    # IMS Basic LTI link root element & namespace patterns
    if lname == 'cartridge_basiclti_link':
        return True
    if 'imslticc' in ns or 'imsbasiclti' in ns:
        return True
    return False


def remove_lti_resources(tree: ET.ElementTree, base_folder: Path, log: ChangeLog, res_id_to_titles: Dict[str, Set[str]]) -> Set[str]:
    root = tree.getroot()
    resources_parent = root.find('.//{*}resources')
    if resources_parent is None:
        return set()

    removed_ids: Set[str] = set()

    for res in list(resources_parent.findall('{*}resource')):
        rid = res.get('identifier') or 'UNKNOWN'
        rtype = (res.get('type') or '').lower()
        href = res.get('href')

        # Quick type hints
        type_is_lti = ('imsbasiclti' in rtype) or ('basiclti' in rtype) or ('lti_link' in rtype)

        looks_lti = False
        targets: List[Path] = []
        if href:
            targets.append(base_folder / href)
        for f in res.findall('{*}file'):
            fh = f.get('href')
            if fh:
                targets.append(base_folder / fh)
        for p in targets:
            if p.suffix.lower() == '.xml' and p.exists():
                if is_lti_descriptor(p):
                    looks_lti = True
                    break
        if type_is_lti or looks_lti:
            resources_parent.remove(res)
            removed_ids.add(rid)
            titles = sorted(res_id_to_titles.get(rid, []))
            log.removed_lti_resources.append((rid, "; ".join(titles) if titles else None))

    return removed_ids


# ---------------------- Namespaces / schemalocation -------------------------

def bump_manifest_version_and_namespaces(manifest_path: Path, tree: ET.ElementTree, log: ChangeLog) -> None:
    root = tree.getroot()

    # 1) schemaversion: downshift anything >= 1.2 to 1.1.0 (coarse but safe for our purpose)
    for sv in root.findall('.//{*}schemaversion'):
        txt = (sv.text or '').strip()
        if txt and (txt[0].isdigit() or txt.startswith('1.')):
            if not txt.startswith('1.1'):
                sv.text = '1.1.0'
                log.version_bumped = f'{txt} -> 1.1.0'

    # 2) Cleanly edit xsi:schemaLocation:
    #    - drop any pair containing Curriculum Standards (imscsmd)
    #    - drop any pair referencing CP extension (imscp_extensionv1p2 / cpextensionv1p2)
    sl = root.get(XSI + 'schemaLocation')
    if sl:
        toks = sl.split()
        pairs = list(zip(toks[0::2], toks[1::2]))
        BAD_NS_SUBSTR = ('imscsmd', 'imscp_extensionv1p2')
        BAD_LOC_SUBSTR = ('cpextensionv1p2', 'imscp_extensionv1p2')
        # (We don't reuse `pairs` once we normalize, but keeping the filter documents intent.)
        pairs = [
            (ns, loc) for (ns, loc) in pairs
            if not any(b in ns for b in BAD_NS_SUBSTR)
            and not any(b in loc for b in BAD_LOC_SUBSTR)
        ]

    # Normalize schemaLocation to the canonical CC 1.1 trio (order matters for some importers)
    want = [
        (IMSP_V11,
         "http://www.imsglobal.org/profile/cc/ccv1p1/ccv1p1_imscp_v1p1_v1p0.xsd"),
        (LOM_MANIFEST_NS,
         "http://www.imsglobal.org/profile/cc/ccv1p1/LOM/ccv1p1_lommanifest_v1p0.xsd"),
        (LOM_RESOURCE_NS,
         "http://www.imsglobal.org/profile/cc/ccv1p1/LOM/ccv1p1_lomresource_v1p0.xsd"),
    ]
    root.set(XSI + 'schemaLocation', ' '.join(x for p in want for x in p))

    # 3) Write the tree (structural changes)
    save_manifest(tree, manifest_path)

    # 4) Textual cleanup: downshift remaining URIs and remove extra namespaces/elements
    text = manifest_path.read_text(encoding='utf-8', errors='ignore')
    for pat, repl in V12_TO_V11_SCHEMA_REPLACEMENTS:
        text = text.replace(pat, repl)

    # Final sweep for any profile ccv1p3 path segments we didn't enumerate
    text = re.sub(r'(/profile/cc/)ccv1p3(/)', r'\1ccv1p1\2', text)

    # Drop stray xmlns decls for curriculum standards ns prefixes
    text = re.sub(r'\s+xmlns:(imscsmd|csmd)="[^"]+"', '', text)
    # Drop CP extension (namespace + elements)
    text = re.sub(CP_EXTENSION_XMLNS_DROP_REGEX, '', text)
    text = re.sub(CP_EXTENSION_VARIANT_DROP_REGEX, '', text, flags=re.DOTALL)

    manifest_path.write_text(text, encoding='utf-8')
    log.namespaces_downgraded = True

def patch_descriptor_xmls(root_folder: Path, log: ChangeLog) -> None:
    for xmlpath in root_folder.rglob('*.xml'):
        if xmlpath.name == 'imsmanifest.xml':
            continue
        try:
            txt = xmlpath.read_text(encoding='utf-8', errors='ignore')
        except Exception:
            continue
        original = txt
        for pat, repl in V12_TO_V11_SCHEMA_REPLACEMENTS:
            txt = txt.replace(pat, repl)
        if txt != original:
            xmlpath.write_text(txt, encoding='utf-8')
            log.descriptor_schema_patches.append(xmlpath)

# ----------------------------- QTI AUDIT ------------------------------------

def classify_qti_xml(xml_path: Path) -> Tuple[str, Optional[str]]:
    """Return (classification, note) without throwing.
    classification ∈ { 'QTI 2.x', 'QTI 1.2', 'Not QTI/Unknown' }
    note may mention vendor extensions if detected.
    """
    try:
        txt = xml_path.read_text(encoding='utf-8', errors='ignore')
    except Exception:
        return 'Not QTI/Unknown', None

    lowered = txt.lower()
    vendor = any(h in lowered for h in VENDOR_HINTS)

    try:
        root = ET.fromstring(txt)
        lname = localname(root.tag)
        ns = ''
        if root.tag.startswith('{'):
            ns = root.tag.split('}')[0].strip('{')
    except Exception:
        if any(h in lowered for h in QTI2_NAMESPACE_HINTS):
            return 'QTI 2.x', 'Detected by namespace text (parse failed)'
        return 'Not QTI/Unknown', None

    if lname in QTI2_ROOTS or any(h in ns for h in QTI2_NAMESPACE_HINTS):
        note = 'Vendor-specific extensions present' if vendor else None
        return 'QTI 2.x', note

    if lname == QTI12_ROOT:
        note = 'Canvas/Instructure extensions detected in QTI 1.2' if vendor else None
        return 'QTI 1.2', note

    return 'Not QTI/Unknown', None


def audit_qti_resources(root_folder: Path, tree: ET.ElementTree, log: ChangeLog, res_id_to_titles: Dict[str, Set[str]]) -> None:
    root = tree.getroot()
    for res in root.findall('.//{*}resource'):
        rtype = (res.get('type') or '').lower()
        rid = res.get('identifier') or 'UNKNOWN'

        is_qtiish = 'imsqti' in rtype or 'qti' in rtype
        target_paths: List[Path] = []

        href = res.get('href')
        if href:
            target_paths.append(root_folder / href)
        for f in res.findall('{*}file'):
            fh = f.get('href')
            if fh:
                target_paths.append(root_folder / fh)

        seen: Set[Path] = set()
        xml_targets = []
        for p in target_paths:
            if p.suffix.lower() == '.xml':
                rp = p.resolve()
                if rp not in seen:
                    seen.add(rp)
                    xml_targets.append(rp)

        classification: Optional[str] = None
        note: Optional[str] = None

        if is_qtiish or xml_targets:
            for xmlp in xml_targets[:5]:  # cap to first few files
                c, n = classify_qti_xml(xmlp)
                if c != 'Not QTI/Unknown':
                    classification = c
                    note = n
                    break
            if not classification and is_qtiish:
                classification = 'Not QTI/Unknown'

        if classification:
            titles = "; ".join(sorted(res_id_to_titles.get(rid, []))) or None
            log.qti_audit.append((rid, titles, classification, note))

def _qmd_value(root: ET.Element, label: str) -> Optional[str]:
    """Read a Canvas qtimetadata field by label."""
    for f in root.findall('.//{*}qtimetadatafield'):
        l = f.find('{*}fieldlabel')
        v = f.find('{*}fieldentry')
        if l is not None and v is not None and (l.text or '').strip() == label:
            return (v.text or '').strip()
    return None


def _resp_lid_and_labels(item: ET.Element) -> Tuple[Optional[ET.Element], List[ET.Element]]:
    """Return (response_lid, list(response_label)) for a QTI 1.2 item."""
    rl = item.find('.//{*}response_lid')
    if rl is None:
        return None, []
    labels = rl.findall('.//{*}response_label')
    return rl, labels


def _correct_ident_from_resprocessing(item: ET.Element) -> Optional[str]:
    """Find the ident marked correct via varequal (first hit)."""
    for rc in item.findall('.//{*}respcondition'):
        condvar = rc.find('{*}conditionvar')
        if condvar is None:
            continue
        for ve in condvar.findall('{*}varequal'):
            ident = (ve.text or '').strip()
            if ident:
                return ident
    return None


def _rename_ident_everywhere(item: ET.Element, old_ident: str, new_ident: str) -> None:
    """Rename a choice ident and adjust all <varequal> references in the item."""
    # 1) response_label ident
    for lab in item.findall('.//{*}response_label'):
        if lab.get('ident') == old_ident:
            lab.set('ident', new_ident)
    # 2) varequal references
    for ve in item.findall('.//{*}varequal'):
        if (ve.text or '').strip() == old_ident:
            ve.text = new_ident


def _convert_tf_to_mc(item: ET.Element) -> bool:
    """
    Flip Canvas metadata to multiple_choice_question and strip TF-specific 'other' branches.
    Keep the varequal to the correct ident so Moodle will treat as single-answer MC.
    """
    changed = False
    # Change Canvas metadata question_type
    for f in item.findall('.//{*}qtimetadatafield'):
        lbl = f.find('{*}fieldlabel'); ent = f.find('{*}fieldentry')
        if lbl is not None and ent is not None and (lbl.text or '').strip() == 'question_type':
            if (ent.text or '').strip() == 'true_false_question':
                ent.text = 'multiple_choice_question'
                changed = True

    # Remove <other/> branches often present in TF exports
    for rc in item.findall('.//{*}respcondition'):
        condvar = rc.find('{*}conditionvar')
        if condvar is None:
            continue
        for other in list(condvar.findall('{*}other')):
            condvar.remove(other)
            changed = True
    return changed


def fix_qti_true_false_in_place(xml_path: Path, fallback_tf_to_mc: bool, log: ChangeLog) -> bool:
    """
    For QTI 1.2 files, normalize any True/False items so the two choice idents are literally
    'true' and 'false' (what Moodle's CC11 importer expects). If normalization is ambiguous
    and fallback is enabled, convert the item to a 2-option multiple choice.
    """
    try:
        txt = xml_path.read_text(encoding='utf-8', errors='ignore')
        root = ET.fromstring(txt)
    except Exception:
        return False

    if localname(root.tag) != 'questestinterop':
        return False  # not QTI 1.2

    changed_any = False

    # Iterate over items
    for item in root.findall('.//{*}item'):
        rl, labels = _resp_lid_and_labels(item)
        qtype = (_qmd_value(item, 'question_type') or '').strip().lower()

        # Treat as TF if metadata says so OR structure is 2 choices (common Canvas TF)
        is_tf_like = (qtype == 'true_false_question') or (rl is not None and len(labels) == 2)
        if not is_tf_like:
            continue

        rl, labels = _resp_lid_and_labels(item)
        if rl is None or len(labels) != 2:
            # malformed TF; try converting if allowed
            if fallback_tf_to_mc and _convert_tf_to_mc(item):
                log.tf_mc_fallback += 1
                changed_any = True
            else:
                log.tf_skipped += 1
            continue

        # Current idents
        id1 = labels[0].get('ident') or ''
        id2 = labels[1].get('ident') or ''
        if not id1 or not id2:
            if fallback_tf_to_mc and _convert_tf_to_mc(item):
                log.tf_mc_fallback += 1
                changed_any = True
            else:
                log.tf_skipped += 1
            continue

        # Determine which ident is correct
        correct = _correct_ident_from_resprocessing(item)

        # If already true/false, nothing to do
        idset = {id1.lower(), id2.lower()}
        if idset == {'true', 'false'}:
            # still clean up <other/> if it exists
            changed = False
            for rc in item.findall('.//{*}respcondition'):
                condvar = rc.find('{*}conditionvar')
                if condvar is None:
                    continue
                for other in list(condvar.findall('{*}other')):
                    condvar.remove(other); changed = True
            if changed:
                changed_any = True
            continue

        # Try to map the two idents to 'true'/'false'
        if correct == id1:
            true_ident, false_ident = id1, id2
        elif correct == id2:
            true_ident, false_ident = id2, id1
        else:
            # No clear correct answer found
            if fallback_tf_to_mc and _convert_tf_to_mc(item):
                log.tf_mc_fallback += 1
                changed_any = True
            else:
                log.tf_skipped += 1
            continue

        # Rename both idents and all references
        if true_ident.lower() != 'true':
            _rename_ident_everywhere(item, true_ident, 'true')
        if false_ident.lower() != 'false':
            _rename_ident_everywhere(item, false_ident, 'false')

        # Remove <other/> branches
        for rc in item.findall('.//{*}respcondition'):
            condvar = rc.find('{*}conditionvar')
            if condvar is None:
                continue
            for other in list(condvar.findall('{*}other')):
                condvar.remove(other)

        log.tf_fixed += 1
        changed_any = True

    if changed_any:
        # Write back
        new_xml = ET.tostring(root, encoding='utf-8', xml_declaration=True)
        xml_path.write_bytes(new_xml)
    return changed_any


def normalize_true_false_qti(root_folder: Path, fallback_tf_to_mc: bool, log: ChangeLog) -> None:
    """
    Walk every .xml under the package; for QTI 1.2 files try to fix T/F items in place.
    """
    for xmlp in root_folder.rglob('*.xml'):
        # Skip manifest and non-files
        if xmlp.name.lower() == 'imsmanifest.xml':
            continue
        # Best-effort: try; if it's not QTI 1.2 the helper is a no-op
        fix_qti_true_false_in_place(xmlp, fallback_tf_to_mc, log)

# ------------------------------- Orchestration -------------------------------
IMSP_V11 = "http://www.imsglobal.org/xsd/imsccv1p1/imscp_v1p1"
LOM_MANIFEST_NS = "http://ltsc.ieee.org/xsd/imsccv1p1/LOM/manifest"
LOM_RESOURCE_NS = "http://ltsc.ieee.org/xsd/imsccv1p1/LOM/resource"

def normalize_default_ns(tree: ET.ElementTree) -> None:
    """
    Make IMSCP v1.1 the default namespace, but keep the LOM and LOM/resource
    subtrees in their proper namespaces.
    """
    root = tree.getroot()

    # Pretty serialization
    ET.register_namespace('', IMSP_V11)
    ET.register_namespace('lom', LOM_MANIFEST_NS)
    ET.register_namespace('lomr', LOM_RESOURCE_NS)

    def ln(tag: str) -> str:
        return tag.split('}', 1)[1] if tag.startswith('{') else tag

    # 1) Force the entire <lom> subtree to the LOM namespace
    def retag_lom_subtree(el: ET.Element):
        el.tag = f'{{{LOM_MANIFEST_NS}}}{ln(el.tag)}'
        for c in list(el):
            retag_lom_subtree(c)

    for lom_el in root.findall('.//{*}lom'):
        retag_lom_subtree(lom_el)

    # 2) Put IMSCP-ish stuff into the default IMSCP namespace, but DO NOT touch LOM
    IMSCP_HINTS = ('imscp_v1p1', 'imsccv1p1', 'imsccv1p2', 'imsccv1p3', 'imscp_v1p2', 'imscp_v1p3')

    def retag_imscp(el: ET.Element):
        tag = el.tag
        ns = ''
        local = tag
        if tag.startswith('{'):
            ns, local = tag[1:].split('}', 1)

        # Skip anything already in LOM or LOM-resource
        if ns in (LOM_MANIFEST_NS, LOM_RESOURCE_NS):
            for c in list(el):
                retag_imscp(c)
            return

        # IMSCP-ish or un-namespaced → move to IMSCP v1.1 default
        if (ns == '') or any(h in ns for h in IMSCP_HINTS):
            el.tag = f'{{{IMSP_V11}}}{local}'

        for c in list(el):
            retag_imscp(c)

    retag_imscp(root)


def process_cartridge(input_zip: Path, output_dir: Path, tf_to_mc_fallback: bool = False) -> Path:
    input_zip = input_zip.resolve()
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    log = ChangeLog()

    # 1) Unzip to temp
    tmp = unzip_to_temp(input_zip)
    manifest_path = tmp / 'imsmanifest.xml'
    if not manifest_path.exists():
        found = list(tmp.rglob('imsmanifest.xml'))
        if not found:
            raise FileNotFoundError('imsmanifest.xml not found in package')
        manifest_path = found[0]
    log.manifest_path = manifest_path

    # 2) Parse manifest
    tree = load_manifest(manifest_path)

    # 2b) Fast-exit if already CC 1.0/1.1
    ver = manifest_cc_version(tree)
    if ver and (ver.startswith('1.0') or ver.startswith('1.1')):
        print(f"Detected Common Cartridge {ver}. No downgrade needed.")
        output_dir.mkdir(parents=True, exist_ok=True)
        dest = output_dir / Path(input_zip.name)
        if str(input_zip.resolve()) != str(dest.resolve()):
            shutil.copy2(input_zip, dest)
        print(f"Output written to: {dest}")
        return dest


    # 3) Remove Curriculum Standards metadata (1.2+ only)
    prune_curriculum_standards(tree, log)

    # 4) Collect item titles per resource
    res_id_to_titles = build_res_id_to_item_titles(tree)

    # 5) Fix intendedUse
    patch_intended_use(tree, log)

    # 6) Remove assignment extension resources (if any)
    removed_assign = remove_assignment_extension_resources(tree, log, res_id_to_titles)

    # 7) Remove LTI resources entirely (policy)
    removed_lti = remove_lti_resources(tree, tmp, log, res_id_to_titles)

    removed_any = set().union(removed_assign, removed_lti)

    if removed_any:
        # 8) Remove items and dependencies referring to deleted resources
        remove_items_referring_to(tree, removed_any)
        remove_dependencies_referring_to(tree, removed_any)

    # 9) Downshift resource@type strings where v1p1 alternatives exist
    downshift_resource_types(tree, log)

    # 9b) Remove CP extension <variant> elements that some export tools leave behind
    drop_cp_variants(tree)

    # 10) Save the structurally edited manifest now
    save_manifest(tree, manifest_path)

    # 11) Bump version & URIs
    bump_manifest_version_and_namespaces(manifest_path, tree, log)

    # 11b) Make IMSCP the default namespace and drop prefixes
    normalize_default_ns(tree)
    save_manifest(tree, manifest_path)

    # 12) Patch descriptor XMLs to reference v1p1 schemas instead of v1p2
    patch_descriptor_xmls(tmp, log)

    # 12b) Normalize QTI T/F idents to 'true'/'false' (with optional MC fallback)
    normalize_true_false_qti(tmp, tf_to_mc_fallback, log)

    # 13A) QTI audit (no changes, just reporting)
    audit_qti_resources(tmp, tree, log, res_id_to_titles)

    # 13B) Sanity check: ensure no v1p3/v1p2 URIs remain
    manifest_text = (tmp / 'imsmanifest.xml').read_text(encoding='utf-8', errors='ignore')
    assert_no_v13_v12_uris(manifest_text)

    # 14) Write output zip (.imscc)
    out_name = input_zip.stem + '-cc11.imscc'
    out_path = (output_dir / out_name).with_suffix('.imscc')
    rezip_folder(tmp, out_path)

    # 15) Print summary
    log.print_summary()

    # Cleanup temp directory
    shutil.rmtree(tmp, ignore_errors=True)

    print(f"Output written to: {out_path}")
    return out_path


# CLI -------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Downgrade IMSCC 1.2 packages to 1.1-compatible for Moodle (with QTI audit).")
    parser.add_argument('--input', '-i', type=str, help="Path to .imscc/.zip file OR a directory containing one")
    parser.add_argument('--output', '-o', type=str, help="Destination directory for downgraded .imscc")
    parser.add_argument('--tf-to-mc-fallback', action='store_true',
    help="If a True/False item can't be normalized, convert it to a 2-option Multiple Choice.")
    args = parser.parse_args()

    if args.input:
        input_path = Path(args.input)
    else:
        raw = input("Enter path to .imscc file OR a directory containing it: ").strip()
        input_path = Path(raw)

    imscc = find_first_imscc(input_path)
    if not imscc or not imscc.exists():
        print("Error: Could not find a .imscc/.zip at that location.", file=sys.stderr)
        sys.exit(2)

    if args.output:
        outdir = Path(args.output)
    else:
        raw = input("Enter destination directory for the downgraded .imscc: ").strip()
        outdir = Path(raw)

    try:
        process_cartridge(imscc, outdir, tf_to_mc_fallback=args.tf_to_mc_fallback)
    except Exception as e:
        print(f"FAILED: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
