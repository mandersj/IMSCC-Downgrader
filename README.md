# IMSCC Downgrader (v1.4.5)

A Python utility that attempts to **downgrade IMS Common Cartridge 1.2 exports (Canvas)** to **1.1 format (Moodle)**.  
The project explored the boundary between strict IMS standards, real-world LMS quirks, and automated content repair — but ultimately, it didn’t achieve full compatibility.

🚧 Status

Release: v1.4.5
Outcome: partial success – consistent downgrades for most courses, but failed full automation due to encoding edge cases.
Current direction: manual review and selective exclusion are recommended before Moodle import.

---

## 🎯 Purpose

Canvas exports (`.imscc`) often use the **IMSCC 1.2** schema, while Moodle’s importer only supports **1.1**.  
This script’s goal was to:

- Rewrite manifest and namespace schema references from v1.2 → v1.1  
- Normalize or remove Canvas-specific extensions (notably QTI 1.2+ quiz packages)  
- Sanitize file references with URL-encoding issues  
- Maintain integrity so Moodle could import without manual editing  

---

## ⚙️ Features Attempted

- **Manifest normalization:** rewrites schemaLocation, namespace URIs, and XML structure for 1.1 compliance  
- **QTI normalization:** detects and removes invalid or empty Canvas QTI assessments  
- **HTML sanitation:** canonicalizes `src` and `href` attributes in embedded content  
- **Duplicate file cleanup:** resolves `%20` vs space mismatches and removes redundant files  
- **Optional downgrade skip:** detects already-compatible 1.0/1.1 cartridges  
- **Post-run probes:** standalone checkers (`probe_*.py`) to inspect encoding, orphaned resources, or mismatch issues  

---

## 🔎 Helper Scripts (`probe_*.py`)

| File | Purpose |
| :------------------------------------ | :------------------------------------------------------------ |
| **probe_badresources.py** | Identifies broken or missing resource references in the manifest |
| **probe_danglingID.py** | Checks for `<item>` elements pointing to non-existent resources |
| **probe_encode_v2.py** | Flags malformed or double-encoded file names (`%2520`, `%20%20`, etc.) |
| **probe_mismatch.py** | Compares manifest hrefs vs actual filenames for canonicalization errors |
| **probe_verifyEachResource.py** | Walks through all `<resource>` elements to confirm file existence and type validity |
---
### 🧰 v1.4.5 Usage Example

python3 DowngradeIMSCC.py \
  --input course.imscc \
  --output downgraded/ \
  --exclude-href web_resources/Uploaded%20Media/EItAcdPP-kk%281%29.jpg

Adds an optional flag to remove specific resources from the manifest:
--exclude-href <path or suffix>

---
# 🧩 Why It Failed

Despite dozens of successful conversions, **Moodle’s importer applies a single URL decode**, while Canvas exports inconsistently encode filenames. This led to one unsolvable mismatch: files with parentheses like `EItAcdPP-kk(1).jpg` vs encoded `%281%29`.

Moodle clarified:
> Moodle decodes once. The manifest can use `%28`/`%29`, but the actual file on disk must use literal `(` `)`.  
> Do **not** include both versions or double-encode.

After multiple attempts to reconcile Canvas’ double-encoding, we chose a pragmatic approach: **exclude the problematic file** instead of overengineering a brittle fix.

---
# 🧪 Lessons Learned

- LMS vendors often deviate from IMS specs — imports succeed based on implementation details, not schema compliance.
- Canvas exports can contain both encoded and decoded file paths. Moodle does not handle both.
- Over-encoding filenames (%28%29, %2528%2529) breaks Moodle’s single-decode rule.
- Full cross-LMS automation requires knowledge of internal import logic, which is effectively a black box.
