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
import urllib.parse

def canonicalize_href(h: str) -> str:
    """
    Return a URL-encoded href suitable for IMS CC (encode spaces and reserved chars).
    IMPORTANT: Do NOT mark parentheses safe; keep safe to "/-_.~".
    """
    return urllib.parse.quote(urllib.parse.unquote(h), safe="/-_.~")

def ensure_canonical_file(base_folder: Path, relpath: str) -> str:
    """
    Ensure the encoded (canonical) path exists on disk.
    If only a decoded path exists, copy it to the encoded path.
    If both exist, keep the encoded one (do not duplicate).
    Return the encoded relative path (POSIX-style).
    """
    enc = canonicalize_href(relpath)
    dec = urllib.parse.unquote(enc)

    enc_abs = base_folder / enc
    dec_abs = base_folder / dec

    # If only decoded exists → copy to encoded
    if dec_abs.exists() and not enc_abs.exists():
        enc_abs.parent.mkdir(parents=True, exist_ok=True)
        if os.path.abspath(dec_abs) != os.path.abspath(enc_abs):
            shutil.copy2(dec_abs, enc_abs)

    # If neither exists, just return the encoded relpath; caller will decide to stub if needed
    # If both exist, prefer the encoded. (Optionally you can remove the decoded twin here.)

    # Normalize to forward slashes for manifest
    return "/".join(Path(enc).parts)


def detect_canvas_qti_artifacts(manifest_path: Path, extracted_dir: Path):
    """
    Returns (should_force_qti: bool, reasons: list[str], qti_files: list[Path])
    Heuristics:
      - Any <file href="non_cc_assessments/*.xml.qti">
      - Any CC 1.1 assessment with a <dependency> that points at a .xml.qti
      - Presence of assessment_qti.xml files (we process them regardless)
    """
    reasons, qti_files = [], []

    if not manifest_path.exists():
        return False, ["no imsmanifest.xml found"], []

    try:
        root = ET.parse(manifest_path).getroot()
    except Exception:
        return False, ["imsmanifest.xml not parseable"], []

    # 1) non_cc_assessments payloads
    for f in root.findall('.//{*}file'):
        href = f.get('href') or ''
        if href.startswith('non_cc_assessments/') and href.endswith('.xml.qti'):
            reasons.append(f'non_cc_assessments/{Path(href).name}')
            qti_files.append(extracted_dir / href)

    # 2) dependency chain into non_cc_assessments
    for dep in root.findall('.//{*}dependency'):
        ref = dep.get('identifierref')
        if not ref:
            continue
        r = root.find(f".//{{*}}resource[@identifier='{ref}']")
        if r is not None:
            href = r.get('href') or ''
            if href.startswith('non_cc_assessments/') and href.endswith('.xml.qti'):
                reasons.append(f'dependency->{href}')
                qti_files.append(extracted_dir / href)

    # 3) standard assessment_qti.xml
    for f in root.findall('.//{*}file'):
        href = f.get('href') or ''
        if href.endswith('/assessment_qti.xml'):
            qti_files.append(extracted_dir / href)

    # De-dupe + filter existing
    uniq, seen = [], set()
    for p in qti_files:
        if p not in seen and p.exists():
            seen.add(p); uniq.append(p)

    should_force = len(reasons) > 0 or len(uniq) > 0
    return should_force, reasons, uniq
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
        
def load_manifest(manifest_path: Path) -> ET.ElementTree:
    return ET.parse(manifest_path)

def save_manifest(tree: ET.ElementTree, manifest_path: Path) -> None:
    tree.write(manifest_path, encoding='utf-8', xml_declaration=True)

def manifest_cc_version(tree: ET.ElementTree) -> Optional[str]:
    for sv in tree.getroot().findall('.//{*}schemaversion'):
        if sv.text:
            return sv.text.strip()
    return None

# ------------------ Manifest edits & pruning --------------------------------

def prune_curriculum_standards(tree: ET.ElementTree, log: "ChangeLog") -> None:
    """Remove Curriculum Standards metadata by namespace (safer than tag match)."""
    root = tree.getroot()
    removed = 0
    for parent in list(root.iter()):
        for child in list(parent):
            tag = child.tag
            if tag.startswith('{'):
                ns = tag.split('}', 1)[0].strip('{')
                if CSM_HINT in ns:  # e.g., 'imscsmd'
                    parent.remove(child)
                    removed += 1
    if removed:
        log.removed_cs_blocks += removed


def build_res_id_to_item_titles(tree: ET.ElementTree) -> Dict[str, Set[str]]:
    """Map <resource identifier> -> set of <item><title> strings from organizations."""
    root = tree.getroot()
    m: Dict[str, Set[str]] = {}
    for org in root.findall('.//{*}organization'):
        for item in org.findall('.//{*}item'):
            rid = item.get('identifierref')
            title_el = item.find('{*}title')
            if not rid or title_el is None or not title_el.text:
                continue
            m.setdefault(rid, set()).add(title_el.text.strip())
    return m


def patch_intended_use(tree: ET.ElementTree, log: "ChangeLog") -> None:
    """Canvas sometimes sets intendedUse='assignment' which 1.1 doesn't allow."""
    root = tree.getroot()
    for res in root.findall('.//{*}resource'):
        for attr in ('intendedUse', 'intendeduse'):
            iu = res.get(attr)
            if iu and iu.lower() == 'assignment':
                res.set(attr, 'unspecified')
                log.intended_use_changes.append((res.get('identifier', 'UNKNOWN'), iu, 'unspecified'))


def downshift_resource_types(tree: ET.ElementTree, log: "ChangeLog") -> None:
    """Re-map known v1p2 resource types to v1p1 equivalents where they exist."""
    repl = {'imsdt_xmlv1p2': 'imsdt_xmlv1p1', 'imswl_xmlv1p2': 'imswl_xmlv1p1'}
    root = tree.getroot()
    for res in root.findall('.//{*}resource'):
        rid = res.get('identifier', 'UNKNOWN')
        rtype = res.get('type') or ''
        new_type = rtype
        for old, new in repl.items():
            if old in new_type:
                new_type = new_type.replace(old, new)
        if new_type != rtype:
            res.set('type', new_type)
            log.resource_type_downshifts.append((rid, rtype, new_type))


def remove_items_referring_to(tree: ET.ElementTree, removed_ids: Set[str]) -> None:
    """Drop <item> nodes whose identifierref points at a resource we removed."""
    root = tree.getroot()
    for parent in list(root.iter()):
        for child in list(parent):
            if child.tag.endswith('item') and (child.get('identifierref') in removed_ids):
                parent.remove(child)


def remove_dependencies_referring_to(tree: ET.ElementTree, removed_ids: Set[str]) -> None:
    """Prune <dependency identifierref=...> that point at removed resources."""
    root = tree.getroot()
    for res in root.findall('.//{*}resource'):
        for dep in list(res.findall('{*}dependency')):
            if dep.get('identifierref') in removed_ids:
                res.remove(dep)

# ----------------------------- LTI removal ----------------------------------

def is_lti_descriptor(xml_path: Path) -> bool:
    """Very lightweight detection of IMS Basic LTI link descriptors."""
    try:
        txt = xml_path.read_text(encoding='utf-8', errors='ignore')
        root = ET.fromstring(txt)
    except Exception:
        return False
    lname = root.tag.split('}', 1)[-1] if root.tag.startswith('{') else root.tag
    ns = root.tag.split('}', 1)[0].strip('{') if root.tag.startswith('{') else ''
    if lname == 'cartridge_basiclti_link':
        return True
    if 'imslticc' in ns or 'imsbasiclti' in ns:
        return True
    return False


def remove_lti_resources(tree: ET.ElementTree, base_folder: Path, log: "ChangeLog",
                         res_id_to_titles: Dict[str, Set[str]]) -> Set[str]:
    """Remove LTI resources outright (Moodle CC 1.1 import policy)."""
    root = tree.getroot()
    resources_parent = root.find('.//{*}resources')
    if resources_parent is None:
        return set()

    removed_ids: Set[str] = set()
    for res in list(resources_parent.findall('{*}resource')):
        rid = res.get('identifier') or 'UNKNOWN'
        rtype = (res.get('type') or '').lower()
        href = res.get('href')

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
            if p.suffix.lower() == '.xml' and p.exists() and is_lti_descriptor(p):
                looks_lti = True
                break

        if type_is_lti or looks_lti:
            resources_parent.remove(res)
            removed_ids.add(rid)
            titles = sorted(res_id_to_titles.get(rid, []))
            log.removed_lti_resources.append((rid, "; ".join(titles) if titles else None))

    return removed_ids


def drop_cp_variants(tree: ET.ElementTree) -> int:
    """Remove any <variant> elements (CP extension) in the manifest."""
    root = tree.getroot()
    removed = 0
    for parent in list(root.iter()):
        for child in list(parent):
            if child.tag.split('}', 1)[-1] == 'variant':
                parent.remove(child)
                removed += 1
    return removed


def log_tf_validation_issues(root_folder: Path) -> None:
    """
    Scan all QTI 1.2 XMLs and print precise diagnostics for T/F items
    that would break Moodle import. Non-fatal; purely informational.
    """
    for xmlp in root_folder.rglob('*.xml'):
        if xmlp.name.lower() == 'imsmanifest.xml':
            continue
        try:
            txt = xmlp.read_text(encoding='utf-8', errors='ignore')
            root = ET.fromstring(txt)
        except Exception:
            continue
        if localname(root.tag) != 'questestinterop':
            continue
        for item in root.findall('.//{*}item'):
            qid = item.get('ident') or 'UNKNOWN_ITEM_IDENT'
            title = (item.find('.//{*}mattext').text or '').strip() if item.find('.//{*}mattext') is not None else ''
            qtype = (_qmd_value(item, 'question_type') or '').strip().lower()
            rl = item.find('.//{*}response_lid')
            labels = rl.findall('{*}response_label') if rl is not None else []
            is_tf = (qtype == 'true_false_question')
            # Heuristic TF look-alike
            if not is_tf and rl is not None and len(labels) >= 2:
                tset = { (labels[0].find('.//{*}mattext').text or '').strip().lower() if labels[0].find('.//{*}mattext') is not None else '',
                         (labels[1].find('.//{*}mattext').text or '').strip().lower() if labels[1].find('.//{*}mattext') is not None else ''}
                if tset & {'true','false'}:
                    is_tf = True
            if not is_tf:
                continue
            problems = []
            if rl is None:
                problems.append("no <response_lid>")
            elif len(labels) != 2:
                problems.append(f"{len(labels)} response_label(s)")
            else:
                ids = [(labels[0].get('ident') or '').lower(), (labels[1].get('ident') or '').lower()]
                if set(ids) != {'true','false'}:
                    problems.append(f"id mismatch: {ids} (expected ['true','false'])")
            for ve in item.findall('.//{*}respcondition/{*}conditionvar/{*}varequal'):
                if (ve.text or '').strip().lower() not in {'true','false'}:
                    problems.append(f"bad varequal '{(ve.text or '').strip()}'")
            if problems:
                print(f"[TF-VALIDATION] {xmlp} :: item ident={qid!r} title={title!r} -> " + "; ".join(problems))
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

def bump_manifest_version_and_namespaces(manifest_path: Path, tree: ET.ElementTree, log: "ChangeLog") -> None:
    root = tree.getroot()

    # 1) schemaversion: downshift anything >= 1.2 to 1.1.0
    for sv in root.findall('.//{*}schemaversion'):
        txt = (sv.text or '').strip()
        if txt and (txt[0].isdigit() or txt.startswith('1.')):
            if not txt.startswith('1.1'):
                sv.text = '1.1.0'
                log.version_bumped = f'{txt} -> 1.1.0'

    # 2) Clean up xsi:schemaLocation: drop imscsmd (Curriculum Standards) and CP extension pairs
    sl = root.get(XSI + 'schemaLocation')
    if sl:
        toks = sl.split()
        pairs = list(zip(toks[0::2], toks[1::2]))
        BAD_NS_SUBSTR = ('imscsmd', 'imscp_extensionv1p2')
        BAD_LOC_SUBSTR = ('cpextensionv1p2', 'imscp_extensionv1p2')
        pairs = [
            (ns, loc) for (ns, loc) in pairs
            if not any(b in ns for b in BAD_NS_SUBSTR)
            and not any(b in loc for b in BAD_LOC_SUBSTR)
        ]

    # 3) Set canonical CC 1.1 schemaLocation (order matters for Moodle)
    want = [
        (IMSP_V11,
         "http://www.imsglobal.org/profile/cc/ccv1p1/ccv1p1_imscp_v1p1_v1p0.xsd"),
        (LOM_MANIFEST_NS,
         "http://www.imsglobal.org/profile/cc/ccv1p1/LOM/ccv1p1_lommanifest_v1p0.xsd"),
        (LOM_RESOURCE_NS,
         "http://www.imsglobal.org/profile/cc/ccv1p1/LOM/ccv1p1_lomresource_v1p0.xsd"),
    ]
    root.set(XSI + 'schemaLocation', ' '.join(x for p in want for x in p))

    # 4) Save structural changes
    save_manifest(tree, manifest_path)

    # 5) Text sweeps: downshift v1p3/v1p2 URIs, drop extra xmlns + <variant/> blocks
    text = manifest_path.read_text(encoding='utf-8', errors='ignore')
    for pat, repl in V12_TO_V11_SCHEMA_REPLACEMENTS:
        text = text.replace(pat, repl)

    # Generic v1p3 → v1p1 fallback for any missed profile paths
    text = re.sub(r'(/profile/cc/)ccv1p3(/)', r'\1ccv1p1\2', text)

    # Drop stray Curriculum Standards xmlns and CP extension artifacts
    text = re.sub(r'\s+xmlns:(imscsmd|csmd)="[^"]+"', '', text)
    text = re.sub(CP_EXTENSION_XMLNS_DROP_REGEX, '', text)
    text = re.sub(CP_EXTENSION_VARIANT_DROP_REGEX, '', text, flags=re.DOTALL)

    manifest_path.write_text(text, encoding='utf-8')
    log.namespaces_downgraded = True

def patch_descriptor_xmls(root_folder: Path, log: "ChangeLog") -> None:
    """
    Rewrite discussion/weblink descriptor XMLs that reference v1p2 schemas
    (imsdt_v1p2, imswl_v1p2) to v1p1 equivalents.
    """
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
            try:
                xmlpath.write_text(txt, encoding='utf-8')
                log.descriptor_schema_patches.append(xmlpath)
            except Exception:
                # Non-fatal; leave file as-is
                pass

def dedupe_keep_decoded(root: Path) -> int:
    removed = 0
    for p in list(root.rglob("*")):
        if not p.is_file():
            continue
        rel = "/".join(p.relative_to(root).parts)
        enc = urllib.parse.quote(urllib.parse.unquote(rel), safe="/-_.~")
        dec = urllib.parse.unquote(enc)
        enc_abs = root / enc
        dec_abs = root / dec
        if enc_abs != dec_abs and enc_abs.exists() and dec_abs.exists():
            try:
                enc_abs.unlink()
                removed += 1
            except Exception:
                pass
    return removed


def rezip_folder(src_folder: Path, dest_zip: Path) -> None:
    dest_zip = dest_zip.with_suffix('.imscc')
    with zipfile.ZipFile(dest_zip, 'w', compression=zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src_folder):
            for fname in files:
                fpath = Path(root) / fname
                arcname = str(fpath.relative_to(src_folder))
                z.write(fpath, arcname)

def _qmd_value(root: ET.Element, label: str) -> Optional[str]:
    for f in root.findall('.//{*}qtimetadatafield'):
        l = f.find('{*}fieldlabel'); v = f.find('{*}fieldentry')
        if l is not None and v is not None and (l.text or '').strip() == label:
            return (v.text or '').strip()
    return None


def fix_qti_true_false_in_place(xml_path: Path, fallback_tf_to_mc: bool, log: "ChangeLog") -> bool:
    """Normalize Canvas-flavored QTI 1.2 True/False items to Moodle-friendly form."""
    try:
        txt = xml_path.read_text(encoding='utf-8', errors='ignore')
        root = ET.fromstring(txt)
    except Exception:
        return False
    if (root.tag.split('}', 1)[-1] if root.tag.startswith('{') else root.tag) != 'questestinterop':
        return False

    def mattext_lower(el: ET.Element) -> str:
        mt = el.find('.//{*}mattext')
        return (mt.text or '').strip().lower() if (mt is not None and mt.text) else ''

    changed_any = False

    def drop_other(item: ET.Element):
        nonlocal changed_any
        for rc in item.findall('.//{*}respcondition'):
            cv = rc.find('{*}conditionvar')
            if cv is None:
                continue
            for o in list(cv.findall('{*}other')):
                cv.remove(o); changed_any = True

    for item in root.findall('.//{*}item'):
        qtype = (_qmd_value(item, 'question_type') or '').strip().lower()
        rl = item.find('.//{*}response_lid')
        labels = rl.findall('{*}response_label') if rl is not None else []
        looks_tf = rl is not None and len(labels) >= 2 and ({mattext_lower(labels[0]), mattext_lower(labels[1])} & {'true','false'})
        is_tf = (qtype == 'true_false_question') or looks_tf
        if not is_tf:
            continue

        if rl is None or len(labels) != 2:
            if fallback_tf_to_mc:
                # change metadata only; Moodle will treat as 2-option MC
                for f in item.findall('.//{*}qtimetadatafield'):
                    lbl = f.find('{*}fieldlabel'); ent = f.find('{*}fieldentry')
                    if lbl is not None and ent is not None and (lbl.text or '').strip() == 'question_type':
                        ent.text = 'multiple_choice_question'
                drop_other(item)
                log.tf_mc_fallback += 1
                changed_any = True
                continue
            # collapse to two (prefer true/false text)
            if rl is not None and len(labels) > 2:
                true_like = [l for l in labels if mattext_lower(l) == 'true']
                false_like = [l for l in labels if mattext_lower(l) == 'false']
                keep = [true_like[0], false_like[0]] if (true_like and false_like) else labels[:2]
                for l in list(labels):
                    if l not in keep:
                        rl.remove(l); changed_any = True
                labels = keep
            else:
                log.tf_skipped += 1
                continue

        # now exactly two
        l1, l2 = labels[0], labels[1]
        id1, id2 = (l1.get('ident') or ''), (l2.get('ident') or '')
        t1, t2 = mattext_lower(l1), mattext_lower(l2)

        correct_ident = None
        for ve in item.findall('.//{*}respcondition/{*}conditionvar/{*}varequal'):
            val = (ve.text or '').strip()
            if val:
                correct_ident = val; break

        mapping: Dict[str,str] = {}
        if correct_ident in {id1, id2}:
            mapping[correct_ident] = 'true'
            mapping[id1 if correct_ident == id2 else id2] = 'false'
        elif {t1, t2} == {'true','false'}:
            mapping[id1] = t1; mapping[id2] = t2
        else:
            mapping[id1] = 'true'; mapping[id2] = 'false'

        if l1.get('ident') != mapping[id1]:
            l1.set('ident', mapping[id1]); changed_any = True
        if l2.get('ident') != mapping[id2]:
            l2.set('ident', mapping[id2]); changed_any = True

        for ve in item.findall('.//{*}respcondition/{*}conditionvar/{*}varequal'):
            val = (ve.text or '').strip()
            if val in mapping:
                if ve.text != mapping[val]:
                    ve.text = mapping[val]; changed_any = True
            elif val.lower() in {'true','false'}:
                if ve.text != val.lower():
                    ve.text = val.lower(); changed_any = True
            else:
                ve.text = 'false'; changed_any = True

        # ensure only two labels and canonical idents
        for extra in list(rl.findall('{*}response_label'))[2:]:
            rl.remove(extra); changed_any = True

        if { (l1.get('ident') or '').lower(), (l2.get('ident') or '').lower() } != {'true','false'}:
            l1.set('ident', 'true'); l2.set('ident', 'false'); changed_any = True
            for ve in item.findall('.//{*}respcondition/{*}conditionvar/{*}varequal'):
                ve.text = 'true' if (ve.text or '').strip().lower() == 'true' else 'false'

        drop_other(item)
        log.tf_fixed += 1

    if changed_any:
        xml_path.write_bytes(ET.tostring(root, encoding='utf-8', xml_declaration=True))
    return changed_any


def _has_bad_tf_idents(xml_path: Path) -> bool:
    try:
        txt = xml_path.read_text(encoding='utf-8', errors='ignore')
        root = ET.fromstring(txt)
    except Exception:
        return False
    if (root.tag.split('}', 1)[-1] if root.tag.startswith('{') else root.tag) != 'questestinterop':
        return False
    for item in root.findall('.//{*}item'):
        qtype = (_qmd_value(item, 'question_type') or '').strip().lower()
        rl = item.find('.//{*}response_lid')
        labels = rl.findall('{*}response_label') if rl is not None else []
        is_tf = (qtype == 'true_false_question')
        if not is_tf and rl is not None and len(labels) == 2:
            texts = { (labels[0].find('.//{*}mattext').text or '').strip().lower() if labels[0].find('.//{*}mattext') is not None else '',
                      (labels[1].find('.//{*}mattext').text or '').strip().lower() if labels[1].find('.//{*}mattext') is not None else '' }
            is_tf = (texts == {'true','false'})
        if not is_tf:
            continue
        if rl is None or len(labels) != 2:
            return True
        idset = {(labels[0].get('ident') or '').lower(), (labels[1].get('ident') or '').lower()}
        if idset != {'true','false'}:
            return True
        for ve in item.findall('.//{*}respcondition/{*}conditionvar/{*}varequal'):
            if (ve.text or '').strip().lower() not in {'true','false'}:
                return True
    return False


def assert_no_bad_tf_idents(root_folder: Path) -> None:
    offenders: List[str] = []
    for xmlp in root_folder.rglob('*.xml'):
        if xmlp.name.lower() == 'imsmanifest.xml':
            continue
        if _has_bad_tf_idents(xmlp):
            offenders.append(str(xmlp))
    if offenders:
        raise ValueError(
            "Unfixed TF items remain (idents must be 'true'/'false' and varequal must reference them) in:\n"
            + "\n".join(offenders)
        )
        
def classify_qti_xml(xml_path: Path) -> Tuple[str, Optional[str]]:
    """Return (classification, note): 'QTI 2.x' | 'QTI 1.2' | 'Not QTI/Unknown'."""
    try:
        txt = xml_path.read_text(encoding='utf-8', errors='ignore')
    except Exception:
        return 'Not QTI/Unknown', None
    lowered = txt.lower()
    vendor = any(h in lowered for h in ('canvas', 'instructure'))
    try:
        root = ET.fromstring(txt)
        lname = root.tag.split('}', 1)[-1] if root.tag.startswith('{') else root.tag
        ns = root.tag.split('}', 1)[0].strip('{') if root.tag.startswith('{') else ''
    except Exception:
        if ('imsqti_v2p' in lowered) or ('imsqti_v2' in lowered):
            return 'QTI 2.x', 'Detected by namespace text (parse failed)'
        return 'Not QTI/Unknown', None
    if lname in {'assessmentItem','assessmentTest'} or ('imsqti_v2' in ns) or ('imsqti_v2p' in ns):
        return ('QTI 2.x', 'Vendor-specific extensions present' if vendor else None)
    if lname == 'questestinterop':
        return ('QTI 1.2', 'Canvas extensions detected' if vendor else None)
    return 'Not QTI/Unknown', None


def audit_qti_resources(root_folder: Path, tree: ET.ElementTree, log: "ChangeLog",
                        res_id_to_titles: Dict[str, Set[str]]) -> None:
    root = tree.getroot()
    for res in root.findall('.//{*}resource'):
        rtype = (res.get('type') or '').lower()
        rid = res.get('identifier') or 'UNKNOWN'
        is_qtiish = 'imsqti' in rtype or 'qti' in rtype

        targets: List[Path] = []
        href = res.get('href')
        if href:
            targets.append(root_folder / href)
        for f in res.findall('{*}file'):
            fh = f.get('href')
            if fh:
                targets.append(root_folder / fh)

        seen: Set[Path] = set()
        xml_targets: List[Path] = []
        for p in targets:
            if p.suffix.lower() == '.xml':
                rp = p.resolve()
                if rp not in seen:
                    seen.add(rp); xml_targets.append(rp)

        classification: Optional[str] = None
        note: Optional[str] = None
        if is_qtiish or xml_targets:
            for xmlp in xml_targets[:5]:
                c, n = classify_qti_xml(xmlp)
                if c != 'Not QTI/Unknown':
                    classification, note = c, n
                    break
            if not classification and is_qtiish:
                classification = 'Not QTI/Unknown'

        if classification:
            titles = "; ".join(sorted(res_id_to_titles.get(rid, []))) or None
            log.qti_audit.append((rid, titles, classification, note))

def fix_qti_resource_hrefs(tree: ET.ElementTree, base_folder: Path) -> int:
    """
    For each QTI assessment resource, if @href is missing/null but a
    <file href=".../assessment_qti.xml"> exists, set href to that file.
    Returns number of resources patched.
    """
    root = tree.getroot()
    patched = 0
    for res in root.findall('.//{*}resource'):
        rtype = (res.get('type') or '').lower()
        if 'imsqti_xml' not in rtype:           # only QTI assessment resources
            continue
        href = res.get('href')
        if href:                                 # already good
            continue
        # find a candidate assessment_qti.xml file entry
        files = res.findall('{*}file')
        cand = None
        for f in files:
            fh = (f.get('href') or '')
            if fh.endswith('/assessment_qti.xml'):
                cand = fh
                break
        if cand:
            res.set('href', cand)
            patched += 1
    return patched

def audit_qti_assessment_items(root_folder: Path, tree: ET.ElementTree) -> None:
    """
    Print a warning if any assessment_qti.xml has zero <item> nodes.
    Non-fatal; helps pinpoint upstream bad exports.
    """
    root = tree.getroot()
    for res in root.findall('.//{*}resource'):
        rtype = (res.get('type') or '').lower()
        if 'imsqti_xml' not in rtype:
            continue
        href = res.get('href') or ''
        if not href:
            continue
        xmlp = (root_folder / href)
        if not xmlp.exists():
            print(f"[QTI-WARN] missing assessment file: {xmlp}")
            continue
        try:
            txt = xmlp.read_text(encoding='utf-8', errors='ignore')
            r = ET.fromstring(txt)
            n_items = len(r.findall('.//{*}item'))
            if n_items == 0:
                rid = res.get('identifier') or 'UNKNOWN_RESOURCE_ID'
                print(f"[QTI-WARN] assessment has 0 items: resource={rid} file={href}")
        except Exception as e:
            print(f"[QTI-WARN] parse failed for {href}: {e}")

def _qti_item_count(xml_path: Path) -> int:
    try:
        txt = xml_path.read_text(encoding='utf-8', errors='ignore')
        root = ET.fromstring(txt)
    except Exception:
        return 0
    return len(root.findall('.//{*}item'))

# ----------------------------- Exclude kinds --------------------------------

def _res_targets(base_folder: Path, res: ET.Element) -> List[Path]:
    t: List[Path] = []
    href = res.get('href')
    if href:
        t.append(base_folder / href)
    for f in res.findall('{*}file'):
        fh = f.get('href')
        if fh:
            t.append(base_folder / fh)
    return t

def is_qti_resource(res: ET.Element, base_folder: Path) -> bool:
    rtype = (res.get('type') or '').lower()
    if 'imsqti' in rtype:
        return True
    for p in _res_targets(base_folder, res):
        s = p.suffix.lower()
        if p.name.endswith('assessment_qti.xml') or p.name.endswith('.xml.qti'):
            return True
        if s == '.xml':
            try:
                txt = p.read_text(encoding='utf-8', errors='ignore').lower()
                if 'questestinterop' in txt or 'imsqti_v2' in txt:
                    return True
            except Exception:
                pass
    return False

def is_weblink_resource(res: ET.Element, base_folder: Path) -> bool:
    rtype = (res.get('type') or '').lower()
    if 'imswl' in rtype:  # weblink descriptor
        return True
    for p in _res_targets(base_folder, res):
        if p.suffix.lower() == '.xml':
            try:
                txt = p.read_text(encoding='utf-8', errors='ignore').lower()
                if 'imswl_v1p' in txt:
                    return True
            except Exception:
                pass
    return False

def is_discussion_resource(res: ET.Element, base_folder: Path) -> bool:
    rtype = (res.get('type') or '').lower()
    if 'imsdt' in rtype:  # discussion topic descriptor
        return True
    for p in _res_targets(base_folder, res):
        if p.suffix.lower() == '.xml':
            try:
                txt = p.read_text(encoding='utf-8', errors='ignore').lower()
                if 'imsdt_v1p' in txt:
                    return True
            except Exception:
                pass
    return False

def is_webcontent_page(res: ET.Element, base_folder: Path) -> bool:
    rtype = (res.get('type') or '').lower()
    if not (
        'webcontent' in rtype
        or 'learning-application-resource' in rtype
        or rtype == 'imscc_xmlv1p1/learning-application-resource'
        or rtype == 'associatedcontent/imscc_xmlv1p1/learning-application-resource'
        or rtype == 'associatedcontent/imscc_xmlv1p1/webcontent'
    ):
        return False
    href = res.get('href') or ''
    return href.lower().endswith(('.html', '.htm'))

def is_file_resource(res: ET.Element, base_folder: Path) -> bool:
    # very loose: non-QTI, non-weblink/discussion, non-page webcontent
    if is_qti_resource(res, base_folder): return False
    if is_weblink_resource(res, base_folder): return False
    if is_discussion_resource(res, base_folder): return False
    if is_webcontent_page(res, base_folder): return False
    return True

def drop_resources_by_kind(tree: ET.ElementTree, base_folder: Path, kinds: Set[str]) -> Dict[str, int]:
    """
    kinds: subset of {'qti','webcontent','discussion','weblink','file','page'}
    Returns counts dict.
    """
    root = tree.getroot()
    resources_parent = root.find('.//{*}resources')
    counts = {'qti':0, 'webcontent':0, 'discussion':0, 'weblink':0, 'file':0, 'page':0}
    if resources_parent is None or not kinds:
        return counts

    # collect ids to remove
    to_remove_ids: Set[str] = set()
    for res in list(resources_parent.findall('{*}resource')):
        rid = res.get('identifier') or 'UNKNOWN'
        hit = None
        if 'qti' in kinds and is_qti_resource(res, base_folder):
            hit = 'qti'
        elif 'discussion' in kinds and is_discussion_resource(res, base_folder):
            hit = 'discussion'
        elif 'weblink' in kinds and is_weblink_resource(res, base_folder):
            hit = 'weblink'
        elif 'page' in kinds and is_webcontent_page(res, base_folder):
            hit = 'page'
        elif 'webcontent' in kinds:
            rtype = (res.get('type') or '').lower()
            if ('webcontent' in rtype) or ('learning-application-resource' in rtype) \
               or rtype in {
                   'imscc_xmlv1p1/learning-application-resource',
                   'associatedcontent/imscc_xmlv1p1/learning-application-resource',
                   'associatedcontent/imscc_xmlv1p1/webcontent'
               }:
                hit = 'webcontent'
        elif 'file' in kinds and is_file_resource(res, base_folder):
            hit = 'file'

        if hit:
            to_remove_ids.add(rid)
            counts[hit] = counts.get(hit, 0) + 1

    if not to_remove_ids:
        return counts

    # remove resources
    for res in list(resources_parent.findall('{*}resource')):
        if (res.get('identifier') or '') in to_remove_ids:
            resources_parent.remove(res)

    # prune <item> references in organizations
    for org in root.findall('.//{*}organization'):
        for parent in list(org.iter()):
            for child in list(parent):
                if child.tag.endswith('item') and (child.get('identifierref') in to_remove_ids):
                    parent.remove(child)

    # prune <dependency> refs
    for res in root.findall('.//{*}resource'):
        for dep in list(res.findall('{*}dependency')):
            if dep.get('identifierref') in to_remove_ids:
                res.remove(dep)

    return counts


def remove_empty_qti_assessments(tree: ET.ElementTree, base_folder: Path) -> int:
    """
    Remove QTI resources whose resolved assessment_qti.xml contains zero <item>.
    Also prunes <item> nodes in <organizations> and <dependency> refs to those resources.
    Returns count removed.
    """
    root = tree.getroot()
    resources_parent = root.find('.//{*}resources')
    if resources_parent is None:
        return 0

    # 1) Find empty QTI resources
    empty_ids = set()
    for res in list(resources_parent.findall('{*}resource')):
        rtype = (res.get('type') or '').lower()
        if 'imsqti' not in rtype:  # only QTI assessments
            continue
        href = res.get('href') or ''
        if not href:
            continue
        xmlp = base_folder / href
        if not xmlp.exists():
            # Missing file is equivalent to empty for import purposes
            empty_ids.add(res.get('identifier') or 'UNKNOWN')
            continue
        if _qti_item_count(xmlp) == 0:
            empty_ids.add(res.get('identifier') or 'UNKNOWN')

    if not empty_ids:
        return 0

    # 2) Remove those resources
    for res in list(resources_parent.findall('{*}resource')):
        if (res.get('identifier') or '') in empty_ids:
            resources_parent.remove(res)

    # 3) Prune org <item> entries that reference them
    for org in root.findall('.//{*}organization'):
        for it in list(org.findall('.//{*}item')):
            if (it.get('identifierref') or '') in empty_ids:
                parent = it.getparent() if hasattr(it, 'getparent') else None  # ElementTree lacks getparent
                # fallback: manual search
                if parent is None:
                    # brute-force: walk children to find and remove
                    for p in list(org.iter()):
                        for c in list(p):
                            if c is it:
                                p.remove(c)
                                break
                else:
                    parent.remove(it)

    # 4) Prune <dependency> links to removed resources
    for res in root.findall('.//{*}resource'):
        for dep in list(res.findall('{*}dependency')):
            if (dep.get('identifierref') or '') in empty_ids:
                res.remove(dep)

    return len(empty_ids)      

def sanitize_html_like_resources(tree: ET.ElementTree, base_folder: Path) -> Tuple[int, int]:
    """
    Make every page-like or webcontent resource safely loadable:
      • Prefer resource/@href as the entry; if missing, use first <file>.
      • Canonicalize hrefs (encode space and parentheses).
      • If href points to a directory or a missing file, create a small HTML stub and point to it.
      • Do NOT make HTML sidecars for non-HTML files; direct links are fine.
      • If no href can be found, remove the resource.
    Returns (removed_count, patched_count).
    """
    print("[DEBUG] entered sanitize_html_like_resources")
    root = tree.getroot()
    resources_parent = root.find('.//{*}resources')
    if resources_parent is None:
        return (0, 0)

    removed = patched = 0

    def _first_href(res: ET.Element) -> str:
        href = (res.get('href') or '').strip()
        if href:
            return href
        f = res.find('{*}file')
        if f is not None:
            h2 = (f.get('href') or '').strip()
            if h2:
                return h2
        return ''

    def _point_res_to(res: ET.Element, rel_href: str):
        res.set('href', rel_href)
        file_el = res.find('{*}file')
        if file_el is None:
            file_el = ET.SubElement(res, '{http://www.imsglobal.org/xsd/imscp_v1p1}file')
        file_el.set('href', rel_href)

    def _write_stub(path: Path, title: str):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "<!doctype html><meta charset='utf-8'>"
            f"<title>{title}</title>"
            "<p>Placeholder created during CC 1.1 normalization.</p>",
            encoding="utf-8"
        )

    for res in list(root.findall('.//{*}resource')):
        # We’ll treat anything with type containing 'webcontent' or 'learning-application-resource'
        # as a candidate; plus anything whose href/first-file looks html-ish.
        rtype = (res.get('type') or '').lower()
        href_raw = _first_href(res)
        rid = res.get('identifier') or '(unknown)'

        if not href_raw:
            resources_parent.remove(res)
            removed += 1
            print(f"[MANIFEST] removed page-like resource with no href (id={rid})")
            continue

        # Canonicalize (encode space and parentheses etc.)
        href_enc = canonicalize_href(href_raw)

        # Ensure the file lives at the encoded path (rename decoded -> encoded if needed)
        href_enc = ensure_canonical_file(base_folder, href_enc)

        target = (base_folder / href_enc)

        # Determine if we should require HTML content
        looks_html = href_enc.lower().endswith(('.html', '.htm'))
        is_page_like = looks_html or ('webcontent' in rtype) or ('learning-application-resource' in rtype)

        if is_page_like:
            # Directory or missing target -> create stub HTML and point to it
            if (not target.exists()) or target.is_dir():
                stub_rel = Path(href_enc).with_suffix('.html')
                stub_path = (base_folder / stub_rel)
                _write_stub(stub_path, f"Resource {rid}")
                _point_res_to(res, str(stub_rel))
                patched += 1
                print(f"[MANIFEST] html fix: wrote stub for {rid} -> {stub_rel}")
                continue

            # If it exists and is html-ish, just point manifest at canonical value
            _point_res_to(res, href_enc)
            if href_enc != href_raw:
                patched += 1
                print(f"[MANIFEST] canonicalized href for {rid}: {href_raw} -> {href_enc}")
            continue

        # Non-HTML webcontent (pdf/png/etc.): leave as-is but canonicalize manifest
        _point_res_to(res, href_enc)
        if href_enc != href_raw:
            patched += 1
            print(f"[MANIFEST] canonicalized non-HTML href for {rid}: {href_raw} -> {href_enc}")

    print(f"[DEBUG] exiting sanitize_html_like_resources removed={removed}, patched={patched}")
    return (removed, patched)


def audit_html_resources(tree: ET.ElementTree, base_folder: Path) -> None:
    """
    Report any resource whose primary href points to .html/.htm but is missing/empty.
    Independent of resource@type.
    """
    root = tree.getroot()
    offenders = []

    def first_href(res: ET.Element) -> Optional[str]:
        href = res.get('href')
        if href:
            return href
        f = res.find('{*}file')
        if f is not None and f.get('href'):
            return f.get('href')
        return None

    for res in root.findall('.//{*}resource'):
        rid  = res.get('identifier') or '?'
        href = first_href(res)
        if not href:
            # other checks will remove; but note as 'missing href'
            offenders.append((rid, '(no href)', 'missing-href'))
            continue
        if not href.lower().endswith(('.html', '.htm')):
            continue

        p = (base_folder / href)
        if not p.exists() or not p.is_file():
            offenders.append((rid, href, 'missing'))
            continue
        try:
            data = p.read_text(encoding='utf-8', errors='ignore')
        except Exception:
            data = ''
        if len(data.strip()) == 0:
            offenders.append((rid, href, 'empty'))

    if offenders:
        print("[AUDIT] HTML-like resource files that are missing/empty:")
        for rid, href, why in offenders[:100]:
            print(f"  - {rid}: {href} ({why})")



def process_cartridge(input_zip: Path, output_dir: Path, tf_to_mc_fallback: bool = False, exclude_kinds: Optional[Set[str]] = None) -> Path:
    # normalize paths (prevents write errors on rezip)
    input_zip = input_zip.resolve()
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # normalize exclude set
    exclude_kinds = exclude_kinds or set()

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

    # 2b) If already CC 1.0/1.1, only pass-through if there are NO Canvas QTI artifacts
    ver = manifest_cc_version(tree)
    force_qti, reasons, _ = detect_canvas_qti_artifacts(manifest_path, tmp)
    
    if ver and (ver.startswith('1.0') or ver.startswith('1.1')) and not force_qti:
        print(f"Detected Common Cartridge {ver}. No downgrade needed (no QTI normalization required).")
        output_dir.mkdir(parents=True, exist_ok=True)
        dest = output_dir / input_zip.name
        # avoid copying a file onto itself
        if input_zip.resolve() != dest.resolve():
            shutil.copy2(str(input_zip), str(dest))
        print(f"Output written to: {dest}")
        return dest

    if ver and (ver.startswith('1.0') or ver.startswith('1.1')) and force_qti:
        print(f"Detected Common Cartridge {ver} wrapper with Canvas QTI artifacts → proceeding with QTI normalization.")

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
    
    # 7b) Optional: drop entire activity kinds instead of converting
    if exclude_kinds:
        dropped = drop_resources_by_kind(tree, tmp, exclude_kinds)
        any_dropped = sum(dropped.values())
        if any_dropped:
            save_manifest(tree, manifest_path)
            print(f"[EXCLUDE] dropped resources -> " +
                  ", ".join(f"{k}:{v}" for k,v in dropped.items() if v))
    
    # 8) Remove items and dependencies referring to deleted resources
    if removed_any:
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

    # 11c) Ensure QTI assessment resources have href -> assessment_qti.xml
    patched = fix_qti_resource_hrefs(tree, tmp)
    if patched:
        save_manifest(tree, manifest_path)
        print(f"[MANIFEST] patched QTI resource hrefs: {patched}")

    # audit (optional)
    audit_qti_assessment_items(tmp, tree)

    # 11d) Remove assessments with 0 <item>
    removed_empty = remove_empty_qti_assessments(tree, tmp)
    if removed_empty:
        save_manifest(tree, manifest_path)
        print(f"[MANIFEST] removed empty QTI assessments: {removed_empty}")

    # 11e) sanitize HTML-like resources (prevents DOMDocument->loadHTML on empty)
    wc_removed, wc_patched = sanitize_html_like_resources(tree, tmp)
    if wc_removed or wc_patched:
        save_manifest(tree, manifest_path)
        print(f"[MANIFEST] html-like sanitized: removed={wc_removed}, patched={wc_patched}")
    audit_html_resources(tree, tmp)


    # 12) Patch descriptor XMLs to reference v1p1 schemas instead of v1p2
    patch_descriptor_xmls(tmp, log)

    # 12b) Normalize QTI T/F idents to 'true'/'false' (with optional MC fallback)
    normalize_true_false_qti(tmp, tf_to_mc_fallback, log)

    # 12c) Log detailed TF validation issues then abort if anything is still off
    log_tf_validation_issues(tmp)
    assert_no_bad_tf_idents(tmp)

    # 13A) QTI audit (no changes, just reporting)
    audit_qti_resources(tmp, tree, log, res_id_to_titles)

    # 13B) Sanity check: ensure no v1p3/v1p2 URIs remain
    manifest_text = (tmp / 'imsmanifest.xml').read_text(encoding='utf-8', errors='ignore')
    assert_no_v13_v12_uris(manifest_text)
    
    # 13C) FINAL AUDIT (before rezip)
    dirty = False
    final_missing = []

    resources_root = tree.getroot().find('.//{*}resources')

    for res in list(tree.getroot().findall('.//{*}resource')):
        href = (res.get('href') or '').strip()
        rid = res.get('identifier') or '(unknown)'

        if not href:
            # Remove completely href-less resources
            if resources_root is not None:
                resources_root.remove(res)
                dirty = True
                print(f"[FINAL AUDIT] Removed resource lacking href (id={rid})")
            continue

        tgt = (tmp / href)

        # Flag anything that is HTML-like OR any directory target
        is_htmlish = href.lower().endswith(('.html', '.htm'))
        is_dir_or_missing = (not tgt.exists()) or tgt.is_dir()

        if (is_htmlish and is_dir_or_missing) or tgt.is_dir():
            final_missing.append((rid, href))

    if final_missing:
        print(f"[FINAL AUDIT] Missing/invalid targets: {len(final_missing)}")
        for rid, href in final_missing:
            print(f"  - {rid}: {href}")

        for rid, href in final_missing:
            res = tree.find(f".//{{*}}resource[@identifier='{rid}']")
            if res is None:
                continue

            # Build a POSIX placeholder href (forward slashes)
            raw_rel = Path(href).with_suffix('.html')
            placeholder_rel = "/".join(raw_rel.parts)  # ensure POSIX separators
            placeholder_path = tmp / placeholder_rel

            placeholder_path.parent.mkdir(parents=True, exist_ok=True)
            placeholder_path.write_text(
                f"<html><body><h1>Resource {rid}</h1>"
                f"<p>Auto-generated placeholder for missing or invalid target: "
                f"<code>{href}</code>.</p></body></html>",
                encoding='utf-8'
            )

            # Point resource to placeholder and REPLACE file children to avoid stale refs
            res.set('href', placeholder_rel)
            for f in list(res.findall('{*}file')):
                res.remove(f)
            ET.SubElement(res, '{http://www.imsglobal.org/xsd/imscp_v1p1}file').set('href', placeholder_rel)

            print(f"[FINAL AUDIT] Patched {rid} -> {placeholder_rel}")
            dirty = True

    if dirty:
        save_manifest(tree, manifest_path)
    
    # 13D) Prune <item> nodes whose identifierref points to a missing <resource>
    def prune_items_with_missing_resources(tree: ET.ElementTree) -> tuple[int, int]:
        root = tree.getroot()
        # Set of valid resource identifiers
        valid_res_ids = {
            r.get("identifier") for r in root.findall(".//{*}resource")
            if r.get("identifier")
        }

        removed_items = 0
        removed_empty_items = 0  # containers with no children and no identifierref

        def prune_item_children(parent_el):
            nonlocal removed_items, removed_empty_items
            # Iterate over a static list of children because we'll mutate the parent
            for child in list(parent_el):
                tag = child.tag.rsplit('}', 1)[-1]  # localname
                if tag == "item":
                    # Recurse before deciding to keep/remove this child
                    prune_item_children(child)

                    ref = (child.get("identifierref") or "").strip()
                    has_children = any(grand.tag.rsplit('}',1)[-1] == "item" for grand in child)

                    # Remove if it references a missing resource
                    if ref and ref not in valid_res_ids:
                        parent_el.remove(child)
                        removed_items += 1
                        continue

                    # Remove empty containers: no identifierref and no child <item>
                    if (not ref) and (not has_children):
                        parent_el.remove(child)
                        removed_empty_items += 1
                        continue
                # else: leave non-<item> nodes alone

        # Prune under each organization
        for org in root.findall(".//{*}organization"):
            prune_item_children(org)

        return removed_items, removed_empty_items

    # --- run pruner & log ---
    pruned_refs, pruned_empty = prune_items_with_missing_resources(tree)
    if pruned_refs or pruned_empty:
        print(f"[ORG] removed items referencing missing resources: {pruned_refs}")
        print(f"[ORG] removed empty item containers: {pruned_empty}")
        save_manifest(tree, manifest_path)

    # 13E) Normalize hrefs to URL-encoded paths and ensure files exist
    def ensure_parent_dir(path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)

    def normalize_resource_hrefs(tree: ET.ElementTree, tmp: Path) -> tuple[int, int]:
        """
        Canonicalize *all* resource hrefs to URL-encoded form and ensure the encoded file exists.
        Align the single <file href> to match. This hits images, PDFs, zips, etc.
        """
        changed_href = 0
        created_files = 0
        root = tree.getroot()

        for res in root.findall(".//{*}resource"):
            href = (res.get("href") or "").strip()
            if not href:
                continue

            # Canonical encoded target
            enc = urllib.parse.quote(urllib.parse.unquote(href), safe="/-_.~")
            if enc != href:
                res.set("href", enc)
                changed_href += 1

            dst = tmp / enc
            if not dst.exists():
                # Try plausible source variants
                candidates = [
                    tmp / urllib.parse.unquote(href),  # decoded of original
                    tmp / href,                        # original as-is
                    tmp / urllib.parse.unquote(enc),   # decoded of encoded
                ]
                for src in candidates:
                    try:
                        if src.exists() and src.is_file():
                            dst.parent.mkdir(parents=True, exist_ok=True)
                            if os.path.abspath(src) != os.path.abspath(dst):
                                shutil.copy2(str(src), str(dst))
                            created_files += 1
                            break
                    except Exception:
                        # fallthrough to try next candidate
                        pass
                else:
                    # As a last resort, create a tiny placeholder (rare)
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    dst.write_text(
                        f"<html><body><p>Auto-generated placeholder for missing target: "
                        f"<code>{enc}</code></p></body></html>",
                        encoding="utf-8",
                    )
                    created_files += 1

            # Align a single <file href> child to the encoded href
            f = res.find("{*}file")
            if f is None:
                f = ET.SubElement(res, "{http://www.imsglobal.org/xsd/imscp_v1p1}file")
            f.set("href", enc)

            # Remove any extra <file> elements that don't match the encoded href
            for extra in list(res.findall("{*}file")):
                if extra is not f and (extra.get("href") or "").strip() != enc:
                    res.remove(extra)

        return changed_href, created_files

    def enforce_decoded_payload(tree: ET.ElementTree, tmp: Path) -> tuple[int,int,int]:
        """
        Ensure every referenced resource exists on disk at its *decoded* path.
        - If only encoded exists -> move to decoded.
        - If both exist -> delete encoded, keep decoded.
        - If neither exists -> create a small placeholder at decoded.
        Returns (moved_enc_to_dec, removed_encoded_dupes, created_placeholders).
        """
        moved = removed = created = 0
        root = tree.getroot()

        # Helper to normalize any href-like string
        def enc_and_dec(rel: str) -> tuple[Path, Path, str, str]:
            rel = (rel or "").strip()
            enc = urllib.parse.quote(urllib.parse.unquote(rel), safe="/-_.~")
            dec = urllib.parse.unquote(enc)
            return (tmp / enc, tmp / dec, enc, dec)

        # Walk both <resource href> and <file href> to cover everything we reference
        hrefs: set[str] = set()
        for res in root.findall(".//{*}resource"):
            h = (res.get("href") or "").strip()
            if h:
                hrefs.add(h)
            for f in res.findall("{*}file"):
                fh = (f.get("href") or "").strip()
                if fh:
                    hrefs.add(fh)

        for h in sorted(hrefs):
            enc_abs, dec_abs, enc, dec = enc_and_dec(h)

            # Case 1: encoded exists, decoded missing -> move (rename) encoded -> decoded
            if enc_abs.exists() and not dec_abs.exists():
                dec_abs.parent.mkdir(parents=True, exist_ok=True)
                # Use move if same filesystem; copy+unlink otherwise (Path.rename throws across FS)
                try:
                    enc_abs.rename(dec_abs)
                except Exception:
                    shutil.copy2(str(enc_abs), str(dec_abs))
                    try:
                        enc_abs.unlink()
                    except Exception:
                        pass
                moved += 1
                continue

            # Case 2: both exist -> delete the encoded duplicate
            if enc_abs.exists() and dec_abs.exists():
                try:
                    enc_abs.unlink()
                    removed += 1
                except Exception:
                    pass
                continue

            # Case 3: neither exists -> create placeholder at decoded
            if not enc_abs.exists() and not dec_abs.exists():
                dec_abs.parent.mkdir(parents=True, exist_ok=True)
                dec_abs.write_text(
                    f"<html><body><p>Auto-generated placeholder for missing asset: "
                    f"<code>{dec}</code></p></body></html>",
                    encoding="utf-8",
                )
                created += 1

        return moved, removed, created

    href_updates, copies = normalize_resource_hrefs(tree, tmp)
    moved, removed, created = enforce_decoded_payload(tree, tmp)
    if href_updates or copies or moved or removed or created:
        print(f"[PAYLOAD] moved enc→dec: {moved}, removed enc dupes: {removed}, created decoded placeholders: {created}")
        save_manifest(tree, manifest_path)
    
    # 14a) Just before rezip
    _ = dedupe_keep_decoded(tmp)
    
    # 14b) Write output zip (.imscc)
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
    parser.add_argument('--exclude', type=str, default='', help="Comma-separated kinds to drop instead of converting. " "Supported kinds: qti, webcontent, discussion, weblink, file, page")
    args = parser.parse_args()
    exclude_kinds = {k.strip().lower() for k in (args.exclude.split(',') if args.exclude else []) if k.strip()}

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
        process_cartridge(imscc, outdir, tf_to_mc_fallback=args.tf_to_mc_fallback, exclude_kinds=exclude_kinds)

    except Exception as e:
        print(f"FAILED: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()


def iter_all_qti_files(extracted_dir: Path):
    # Standard CC assessments
    for p in extracted_dir.rglob('*/assessment_qti.xml'):
        yield p
    # Canvas sidecar QTI
    for p in extracted_dir.rglob('non_cc_assessments/*.xml.qti'):
        yield p

