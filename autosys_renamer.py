#!/usr/bin/env python3
"""
autosys_renamer.py — Autosys Job Renaming Tool
===============================================
Converts legacy Autosys job names to a strict 64-character positional
naming standard.

New Name Format (character positions):
  AAAAA_IIIPSС_SSSSSS_TTNNNNN_<suffix>
  1-5   7-12   14-19  21-26   28-64
  App   Inst+  Subj   ETL+ID  Suffix
        Env+
        Sched+
        Type

  Position breakdown:
    1-5   App code         (5 chars, uppercase alphanumeric)
    6     Separator        _
    7-9   Instance code    (3 chars, from config)
    10    Environment      (1 char, always P from config)
    11    Schedule         (1 char, from schedule_codes.csv)
    12    AS Job Type      (1 char, F/B/C from as_job_types.csv)
    13    Separator        _
    14-19 Subject Area     (6 chars, from subject_areas.csv)
    20    Separator        _
    21-22 ETL Type         (1-2 chars; 1-char shifts Unique ID left by 1)
    23-26 Unique ID        (4 digits, sequential per subject+category)
    27    Separator        _
    28-64 Suffix           (up to 37 chars, from remaining name tokens)

Usage:
  python autosys_renamer.py
  python autosys_renamer.py --config config.yaml --input input/jobs_export.csv
  python autosys_renamer.py --output output/renamed_jobs.csv
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd
import yaml

# ── Constants ─────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent

ALLOWED_SCHEDULES = set("RHDWMQYAO")
ALLOWED_AS_TYPES = {"F", "B", "C"}
ALLOWED_ETL_TYPES = {
    "A", "B", "C", "E", "EN", "F", "I", "L",
    "MB", "M", "N", "P", "R", "S", "T", "V",
}
# Tokens at the end of old names that encode the job type
OLD_TYPE_SUFFIXES = {"BX", "CM", "CMD", "FW"}

MAX_SUFFIX_LEN = 37
MAX_NAME_LEN = 64

# Tokens that encode the schedule — never treated as subject candidates
SCHEDULE_TOKENS = {"D", "M", "A", "OD", "W", "H", "Q", "Y", "O", "R"}

# Noise tokens — environmental, processing, or structural markers to skip
NOISE_TOKENS = {
    "PROD", "DEV", "UAT", "LOAD", "EXT", "INT", "SRC",
    "BX", "BOX", "CM", "CMD", "FW", "PRLL", "HIST",
    "CONF", "SAN", "RAW", "DW", "DATA", "PROCESS",
}


# ── Config Loading ─────────────────────────────────────────────────────────────

def load_config(path: Path) -> dict:
    """Load and validate config.yaml. Raises on missing keys or bad values."""
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    required = ["instance_code", "env_code"]
    missing = [k for k in required if k not in cfg]
    if missing:
        raise ValueError(f"config.yaml missing required keys: {missing}")

    if len(str(cfg["instance_code"]).strip()) != 3:
        raise ValueError(
            f"instance_code must be exactly 3 characters, got: {cfg['instance_code']!r}"
        )
    if len(str(cfg["env_code"]).strip()) != 1:
        raise ValueError(
            f"env_code must be exactly 1 character, got: {cfg['env_code']!r}"
        )
    return cfg


# ── Mapping Loading ────────────────────────────────────────────────────────────

def load_mappings(config: dict) -> dict:
    """
    Load all five mapping CSVs and return consolidated lookup structures.

    Returns dict with keys:
      app        — {old_prefix_upper: new_app_code}
      schedule   — {old_schedule_upper: new_schedule_char}
      etl        — {old_suffix_upper: new_etl_code}
      as_type    — {old_job_type_upper: new_as_type_char}
      subj_exact  — {name_upper: entry_dict}
      subj_prefix — [entry_dict, ...] sorted by len(pattern) desc
      subj_regex  — [entry_dict, ...]
    """
    maps_dir = BASE_DIR / "mappings"

    def read_csv(filename: str) -> pd.DataFrame:
        p = maps_dir / filename
        if not p.exists():
            raise FileNotFoundError(f"Mapping file not found: {p}")
        return pd.read_csv(p, dtype=str).fillna("")

    app_df  = read_csv("app_codes.csv")
    sched_df = read_csv("schedule_codes.csv")
    subj_df  = read_csv("subject_areas.csv")
    etl_df   = read_csv("etl_job_types.csv")
    as_df    = read_csv("as_job_types.csv")

    # Simple key→value dicts
    app_map = {
        r["old_prefix"].strip().upper(): r["new_app_code"].strip().upper()
        for _, r in app_df.iterrows()
    }
    sched_map = {
        r["old_schedule"].strip().upper(): r["new_schedule"].strip().upper()
        for _, r in sched_df.iterrows()
    }
    etl_map = {
        r["old_suffix"].strip().upper(): r["new_etl_code"].strip().upper()
        for _, r in etl_df.iterrows()
    }
    as_type_map = {
        r["old_job_type"].strip().upper(): r["new_as_type"].strip().upper()
        for _, r in as_df.iterrows()
    }

    # Subject area — three lookup tiers
    subj_exact: dict = {}
    subj_prefix: list = []
    subj_regex: list = []

    for _, row in subj_df.iterrows():
        match_type = row["match_type"].strip().lower()
        entry = {
            "pattern":      row["pattern"].strip(),
            "subject_code": row["subject_code"].strip().upper(),
            "category":     row["category"].strip().lower(),
        }
        if match_type == "exact":
            subj_exact[entry["pattern"].upper()] = entry
        elif match_type == "prefix":
            subj_prefix.append(entry)
        elif match_type == "regex":
            subj_regex.append(entry)

    # Longest prefix wins
    subj_prefix.sort(key=lambda e: len(e["pattern"]), reverse=True)

    return {
        "app":         app_map,
        "schedule":    sched_map,
        "etl":         etl_map,
        "as_type":     as_type_map,
        "subj_exact":  subj_exact,
        "subj_prefix": subj_prefix,
        "subj_regex":  subj_regex,
    }


# ── Old-Name Parser ────────────────────────────────────────────────────────────

def parse_old_name(name: str) -> dict:
    """
    Tokenise a legacy job name into its components.

    Returns dict:
      app_prefix     — first token (e.g. "ORCAP", "P-RDS")
      schedule_token — schedule designator token (e.g. "D", "OD")
      type_suffix    — trailing type token if present (BX/CM/CMD/FW), else ""
      suffix_body    — middle tokens joined with "_" (used as new name suffix)
      is_zzz         — True when the name uses the CARAT ZZZ-placeholder format

    ZZZ-format example: CARAT_ZZZDAC_ORZZZZ_CH815_SNOW_RCSA_CONT_INST_XRE
      token[0] = "CARAT"  → app_prefix
      token[1] = "ZZZDAC" → is_zzz=True; schedule = token[1][3] = 'D'
      remaining tokens form suffix_body
    """
    tokens = name.strip().split("_")
    result = {
        "app_prefix":     "",
        "schedule_token": "",
        "type_suffix":    "",
        "suffix_body":    "",
        "is_zzz":         False,
    }

    if not tokens or not tokens[0]:
        return result

    # ── Strip leading P_ / PR_ / P- placeholders ──
    t0 = tokens[0].upper()
    if t0 in {"P", "PR"}:
        # e.g. "P_FCTDS_D_..." or "PR_FCTDS_D_..." → drop the placeholder token
        tokens = tokens[1:]
        if not tokens:
            return result
    elif t0.startswith("P-"):
        # e.g. "P-FCTDS_D_..." → strip the two-char prefix
        tokens[0] = tokens[0][2:]
        if not tokens[0]:
            tokens = tokens[1:]
            if not tokens:
                return result

    result["app_prefix"] = tokens[0].upper()

    if len(tokens) < 2:
        return result

    token1 = tokens[1].upper()

    # ── ZZZ-placeholder format ──
    if token1.startswith("ZZZ"):
        result["is_zzz"] = True
        # Schedule char is at position 3 of the ZZZ token (e.g. ZZZDAC → 'D')
        result["schedule_token"] = token1[3] if len(token1) > 3 else ""
        last = tokens[-1].upper()
        if last in OLD_TYPE_SUFFIXES and len(tokens) > 2:
            result["type_suffix"] = last
            result["suffix_body"] = "_".join(tokens[2:-1])
        else:
            result["suffix_body"] = "_".join(tokens[2:])
        return result

    # ── Normal format ──
    result["schedule_token"] = token1
    last = tokens[-1].upper()
    if last in OLD_TYPE_SUFFIXES and len(tokens) > 2:
        result["type_suffix"] = last
        result["suffix_body"] = "_".join(tokens[2:-1])
    else:
        result["suffix_body"] = "_".join(tokens[2:])

    return result


# ── Subject-Area Lookup ────────────────────────────────────────────────────────

def _pattern_candidates(pattern: str) -> set:
    """Extract subject-area token candidates from a CSV pattern string."""
    parts = pattern.strip().split("_")
    found = []
    for tok in parts[1:]:   # skip token[0] = app prefix
        clean = re.sub(r"[^A-Z0-9]", "", tok.upper())
        if not clean:
            continue
        if clean in SCHEDULE_TOKENS or clean in NOISE_TOKENS:
            continue
        if len(clean) < 3:
            continue
        found.append(clean)
        if len(found) == 2:
            break
    return set(found)


def lookup_subject_area(
    job_name: str, maps: dict, app_code: str = "", freq_map: dict = None
) -> tuple:
    """
    Find subject area for a job name using four-tier matching:
      1. Exact match
      2. Longest-prefix match
      3. Regex match
      4. Dynamic derivation from name tokens (fallback)

    Returns (subject_code, category, candidates, is_auto_derived).
    candidates is non-empty for Tier-1/2 (pattern tokens) and Tier-4 (auto-
    derived), so the caller can strip them from the suffix to avoid repetition.
    is_auto_derived is True only for Tier-4 (caller uses it for NEEDS_REVIEW).
    """
    upper = job_name.upper()

    if upper in maps["subj_exact"]:
        e = maps["subj_exact"][upper]
        return e["subject_code"], e["category"], _pattern_candidates(e["pattern"]), False

    for entry in maps["subj_prefix"]:
        if upper.startswith(entry["pattern"].upper()):
            return entry["subject_code"], entry["category"], _pattern_candidates(entry["pattern"]), False

    for entry in maps["subj_regex"]:
        if re.search(entry["pattern"], job_name, re.IGNORECASE):
            return entry["subject_code"], entry["category"], set(), False

    # Tier 4 — dynamic derivation from name tokens; skip resolved app_code
    extra = {re.sub(r"[^A-Z0-9]", "", app_code.upper())} if app_code else set()
    subject_code, category, candidates = derive_subject_from_name(
        job_name, extra_skip=extra, freq_map=freq_map
    )
    return subject_code, category, candidates, True


def derive_subject_from_name(
    job_name: str,
    extra_skip: set = None,
    freq_map: dict = None,
) -> tuple:
    """
    Dynamically derive a subject code from job name tokens.

    Algorithm:
      - Tokenise on '_', upper-case and strip non-alphanumeric per token
      - Skip token[0] (app prefix — always)
      - Walk remaining tokens; skip if in SCHEDULE_TOKENS, NOISE_TOKENS,
        extra_skip, or fewer than 3 alphanumeric characters
      - Collect first 2 meaningful candidates
      - If freq_map supplied and (cand[0], cand[1]) appears in ≥3 jobs:
          subject_code = (cand[0] + cand[1])[:6]  — use both tokens
        Else:
          subject_code = cand[0][:6]               — use first token only
      - Right-pad to exactly 6 chars with 'X'
      - category = "raw" if literal "RAW" token present, else "curated"

    Returns (subject_code: str, category: str, strip_candidates: set).
    strip_candidates contains only tokens actually used in building subject_code
    (primary always; secondary only if fully consumed within 6-char budget).
    """
    raw_tokens = job_name.strip().split("_")
    _toks = {t.upper() for t in raw_tokens}
    if _toks & {"RAW", "SAN", "SANITIZED"}:
        category = "raw"
    elif _toks & {"CONF", "CONFORMED"}:
        category = "conformed"
    elif _toks & {"CURATED", "FFP"}:
        category = "curated"
    elif _toks & {"KAFKA", "API", "STREAMING"}:
        category = "streaming"
    elif "NDM" in _toks:
        category = "ndm"
    else:
        category = "other"

    skip = extra_skip or set()
    candidates: list = []
    for tok in raw_tokens[1:]:
        clean = re.sub(r"[^A-Z0-9]", "", tok.upper())
        if not clean:
            continue
        if clean in SCHEDULE_TOKENS or clean in NOISE_TOKENS or clean in skip:
            continue
        if len(clean) < 3:
            continue
        candidates.append(clean)
        if len(candidates) == 2:
            break

    # Frequency-based decision: use 2-token pair only if it appears in ≥3 jobs
    freq_map = freq_map or {}
    use_pair = (
        len(candidates) == 2
        and freq_map.get((candidates[0], candidates[1]), 0) >= 3
        and len(candidates[0]) + len(candidates[1]) <= 6   # both tokens must fit whole
    )

    if use_pair:
        combined = candidates[0] + candidates[1]           # no truncation — guaranteed ≤6
        active_candidates = candidates[:2]
    elif candidates:
        combined = candidates[0][:6]                       # single token, pad if shorter
        active_candidates = candidates[:1]
    else:
        combined = ""
        active_candidates = []

    subject_code = combined.ljust(6, "X") if combined else "UNKNWX"

    # Strip only candidates fully consumed by the 6-char budget
    strip_candidates: set = set()
    chars_used = 0
    for i, cand in enumerate(active_candidates):
        if i == 0:
            strip_candidates.add(cand)
            chars_used += min(len(cand), 6)
        elif chars_used + len(cand) <= 6:
            strip_candidates.add(cand)
            chars_used += len(cand)
        else:
            break

    return subject_code, category, strip_candidates


def build_token_pair_freq(df: pd.DataFrame) -> dict:
    """
    Pre-scan all job names to count frequency of (token_a, token_b) pairs.

    For each job name: skip token[0] (app prefix) and walk remaining tokens
    using the same SCHEDULE_TOKENS / NOISE_TOKENS / len<3 skip rules.
    Collect the first two meaningful candidates; if two exist, increment
    the counter for that pair.

    Returns {(token_a, token_b): count} dict.
    """
    from collections import Counter
    freq: Counter = Counter()
    for _, row in df.iterrows():
        job_name = str(row.get("job_name", "")).strip()
        if not job_name:
            continue
        raw_tokens = job_name.strip().split("_")
        cands: list = []
        for tok in raw_tokens[1:]:
            clean = re.sub(r"[^A-Z0-9]", "", tok.upper())
            if not clean:
                continue
            if clean in SCHEDULE_TOKENS or clean in NOISE_TOKENS:
                continue
            if len(clean) < 3:
                continue
            cands.append(clean)
            if len(cands) == 2:
                break
        if len(cands) == 2 and len(cands[0]) + len(cands[1]) <= 6:
            freq[(cands[0], cands[1])] += 1
    return dict(freq)


# ── ID Counter ─────────────────────────────────────────────────────────────────

_CATEGORY_ID_START = {
    "raw":       1000,
    "conformed": 2000,
    "curated":   3000,
    "streaming": 4000,
    "ndm":       5000,
    "other":     6000,
}


class IDCounter:
    """
    Sequential unique-ID generator per (subject_code, category) pair.

    Category → ID range start:
      raw        → 1000  (RAW, SAN, SANITIZED)
      conformed  → 2000  (CONF, CONFORMED)
      curated    → 3000  (CURATED, FFP)
      streaming  → 4000  (KAFKA, API, STREAMING)
      ndm        → 5000  (NDM)
      other      → 6000  (fallback)
    """

    def __init__(self):
        self._counters: dict = {}

    def next(self, subject_code: str, category: str) -> int:
        cat = category.lower()
        start = _CATEGORY_ID_START.get(cat, _CATEGORY_ID_START["other"])
        key = (subject_code.upper(), cat)
        if key not in self._counters:
            self._counters[key] = start
        val = self._counters[key]
        self._counters[key] += 1
        return val


# ── Field Resolution ───────────────────────────────────────────────────────────

def resolve_fields(
    parsed: dict,
    job_name: str,
    job_type: str,
    maps: dict,
    cfg: dict,
    id_counter: IDCounter,
    application_name: str = "",
    freq_map: dict = None,
) -> dict:
    """
    Map parsed tokens → final field values using config and mapping tables.

    Fallback behaviour per field:
      App code     — tries application_name column before raising EXCEPTION
      Schedule     — NEEDS_REVIEW, defaults to 'D'
      AS Job Type  — NEEDS_REVIEW, defaults to 'C'
      Subject area — auto-derived from name tokens (Tier 4)
      ETL type     — NEEDS_REVIEW, defaults to 'C'

    Returns dict with all name fields plus 'status' and 'notes'.
    """
    notes: list = []
    is_exception = False
    needs_review = False

    # ── App code — taken directly from application_name column ──
    app_code = re.sub(r"[^A-Z0-9]", "", application_name.upper())[:5].ljust(5, "X")
    if not app_code.strip("X"):
        is_exception = True
        app_code = (re.sub(r"[^A-Z0-9]", "", parsed["app_prefix"].upper()) + "XXXXX")[:5]
        notes.append(
            f"application_name '{application_name}' is empty or invalid — "
            "populate application_name in the source data"
        )

    # ── Instance & Env — always from config ──
    instance = str(cfg["instance_code"]).strip().upper()
    env = str(cfg["env_code"]).strip().upper()

    # ── Schedule ──
    sched_token = parsed["schedule_token"].upper()
    schedule = maps["schedule"].get(sched_token, "")
    if not schedule:
        needs_review = True
        notes.append(
            f"Unknown schedule token '{sched_token}' — defaulting to 'D'; "
            "add to mappings/schedule_codes.csv if intentional"
        )
        schedule = "D"

    # ── AS Job Type (from job_type column) ──
    jtype_key = job_type.strip().upper()
    as_job_type = maps["as_type"].get(jtype_key, "")
    if not as_job_type:
        needs_review = True
        notes.append(
            f"Unknown AS job type column value '{jtype_key}' — defaulting to 'C'; "
            "add to mappings/as_job_types.csv"
        )
        as_job_type = "C"

    # ── Subject area ──
    subject_code, category, subj_candidates, subj_auto_derived = lookup_subject_area(
        job_name, maps, app_code, freq_map=freq_map
    )
    if subj_auto_derived:   # True only for Tier-4; CSV-matched tiers return False
        needs_review = True
        notes.append(
            "Subject area auto-derived from job name tokens; "
            "verify in subject_areas.csv"
        )

    # ── Guard: subject_code must not echo app_code ──
    if subject_code[:5] == app_code:
        needs_review = True
        notes.append(
            f"subject_code '{subject_code}' echoes app_code '{app_code}' — "
            "add a subject_areas.csv rule to assign a distinct subject"
        )

    # ── ETL type (from old name's trailing type token) ──
    type_suffix = parsed["type_suffix"].upper()
    etl_type = maps["etl"].get(type_suffix, "")
    if not etl_type:
        needs_review = True
        if type_suffix:
            notes.append(
                f"Unknown ETL suffix token '{type_suffix}' — defaulting to 'C'; "
                "add to mappings/etl_job_types.csv"
            )
        else:
            notes.append(
                "No recognised type suffix (BX/CM/CMD/FW) found in old name — "
                "defaulting ETL type to 'C'"
            )
        etl_type = "C"

    # ── Unique ID ──
    job_id = id_counter.next(subject_code, category)

    # ── Suffix body — remove app_code token if present (already in name header) ──
    suffix_body = parsed["suffix_body"]
    app_clean = re.sub(r"[^A-Z0-9]", "", app_code.upper())
    kept = []
    for part in suffix_body.split("_"):
        if re.sub(r"[^A-Z0-9]", "", part.upper()) == app_clean:
            pass   # already encoded in app segment of new name
        else:
            kept.append(part)
    suffix_body = "_".join(kept)

    # ── Suffix body — remove subject-area tokens to avoid repetition ──
    if subj_candidates:
        remaining = set(subj_candidates)   # each candidate removed at most once
        kept = []
        for part in suffix_body.split("_"):
            clean = re.sub(r"[^A-Z0-9]", "", part.upper())
            if clean in remaining:
                remaining.discard(clean)   # consume one occurrence
            else:
                kept.append(part)
        suffix_body = "_".join(kept)

    # ── Suffix body — remove category-indicator tokens (already encoded in job_id) ──
    CATEGORY_STRIP = {
        "RAW", "SAN", "SANITIZED",
        "CONF", "CONFORMED",
        "CURATED", "FFP",
        "KAFKA", "API", "STREAMING",
        "NDM",
    }
    kept = []
    for part in suffix_body.split("_"):
        if re.sub(r"[^A-Z0-9]", "", part.upper()) not in CATEGORY_STRIP:
            kept.append(part)
    suffix_body = "_".join(kept)

    # ── Suffix body — replace LOAD → LD ──
    suffix_body = re.sub(r'\bLOAD\b', 'LD', suffix_body, flags=re.IGNORECASE)

    # ── Suffix body — remove single-letter tokens ──
    kept = [p for p in suffix_body.split("_") if len(re.sub(r"[^A-Z0-9]", "", p.upper())) > 1]
    suffix_body = "_".join(kept)

    # ── Suffix body — truncate if over limit ──
    if len(suffix_body) > MAX_SUFFIX_LEN:
        notes.append(
            f"Suffix truncated from {len(suffix_body)} to {MAX_SUFFIX_LEN} chars"
        )
        suffix_body = suffix_body[:MAX_SUFFIX_LEN]

    # ── Overall status ──
    if is_exception:
        status = "EXCEPTION"
    elif needs_review:
        status = "NEEDS_REVIEW"
    else:
        status = "SUCCESS"

    return {
        "app_code":    app_code,
        "instance":    instance,
        "env":         env,
        "schedule":    schedule,
        "as_job_type": as_job_type,
        "subject_code": subject_code,
        "etl_type":    etl_type,
        "job_id":      job_id,
        "category":    category,
        "suffix":      suffix_body,
        "status":      status,
        "notes":       "; ".join(notes),
    }


# ── Name Builder ───────────────────────────────────────────────────────────────

def build_new_name(fields: dict) -> str:
    """
    Assemble the positional new name from resolved fields.

    Schema (char positions, 1-based):
      AAAAA _ III P S C _ SSSSSS _ TT NNNN _ suffix
      1-5  6 7-9 10 11 12 13 14-19 20 21-22 23-26 27 28-64

    Where:
      AAAAA  = app_code (5)
      III    = instance (3)
      P      = env (1)
      S      = schedule (1)
      C      = as_job_type (1)
      SSSSSS = subject_code (6)
      TT     = etl_type (1-2; 1-char shifts unique ID one position left)
      NNNN   = job_id zero-padded to 4 digits
      suffix = up to 37 chars
    """
    app    = (fields["app_code"]    + "XXXXX")[:5].upper()
    inst   = (fields["instance"]    + "XXX")[:3].upper()
    env    = (fields["env"]         + "X")[:1].upper()
    sched  = (fields["schedule"]    + "X")[:1].upper()
    astype = (fields["as_job_type"] + "X")[:1].upper()
    subj   = (fields["subject_code"]+ "XXXXXX")[:6].upper()
    etl    = fields["etl_type"].upper()
    uid    = str(fields["job_id"]).zfill(4)
    suffix = fields.get("suffix", "")

    mid_block = f"{inst}{env}{sched}{astype}"   # positions 7-12 (6 chars)
    etl_uid   = f"{etl}{uid}"                   # positions 21-26 (5 or 6 chars)

    if suffix:
        return f"{app}_{mid_block}_{subj}_{etl_uid}_{suffix}"
    return f"{app}_{mid_block}_{subj}_{etl_uid}"


# ── Validator ──────────────────────────────────────────────────────────────────

def validate_new_name(name: str, fields: dict) -> list:
    """
    Validate the assembled new name against all positional and content rules.

    Returns a list of violation strings. An empty list means the name is valid.
    """
    violations = []

    app = fields.get("app_code", "")
    if not re.match(r"^[A-Z0-9]{5}$", app):
        violations.append(
            f"app_code '{app}' must be exactly 5 uppercase alphanumeric chars"
        )

    inst = fields.get("instance", "")
    if not re.match(r"^[A-Z0-9]{3}$", inst):
        violations.append(
            f"instance '{inst}' must be exactly 3 uppercase alphanumeric chars"
        )

    sched = fields.get("schedule", "")
    if sched not in ALLOWED_SCHEDULES:
        violations.append(
            f"schedule '{sched}' not in allowed set {sorted(ALLOWED_SCHEDULES)}"
        )

    astype = fields.get("as_job_type", "")
    if astype not in ALLOWED_AS_TYPES:
        violations.append(f"as_job_type '{astype}' not in {{F, B, C}}")

    subj = fields.get("subject_code", "")
    if not re.match(r"^[A-Z0-9]{6}$", subj):
        violations.append(
            f"subject_code '{subj}' must be exactly 6 uppercase alphanumeric chars"
        )

    etl = fields.get("etl_type", "")
    if etl not in ALLOWED_ETL_TYPES:
        violations.append(
            f"etl_type '{etl}' not in allowed set {sorted(ALLOWED_ETL_TYPES)}"
        )

    uid = fields.get("job_id", 0)
    uid_str = str(uid).zfill(4)
    if not re.match(r"^\d{4}$", uid_str) or not (0 <= int(uid_str) <= 9999):
        violations.append(
            f"job_id '{uid}' must produce a 4-digit string in range 0000-9999"
        )

    if len(name) > MAX_NAME_LEN:
        violations.append(
            f"new name length {len(name)} exceeds {MAX_NAME_LEN} chars"
        )

    if re.search(r"[^A-Z0-9_]", name.upper()):
        violations.append("new name contains characters outside [A-Z0-9_]")

    return violations


# ── Main Processing Loop ───────────────────────────────────────────────────────

def process_all(df: pd.DataFrame, maps: dict, cfg: dict) -> pd.DataFrame:
    """
    Parse → resolve → build → validate for every row in the input DataFrame.
    Returns output DataFrame with old_name, new_name, status, notes, and all
    per-field columns for audit.
    """
    id_counter = IDCounter()
    freq_map = build_token_pair_freq(df)

    rows = []
    for _, row in df.iterrows():
        job_name = str(row.get("job_name", "")).strip()
        job_type = str(row.get("job_type", "")).strip()
        application_name = str(row.get("application_name", "")).strip()

        parsed = parse_old_name(job_name)
        fields = resolve_fields(
            parsed, job_name, job_type, maps, cfg, id_counter,
            application_name, freq_map=freq_map
        )
        new_name = build_new_name(fields)
        violations = validate_new_name(new_name, fields)

        if violations:
            if fields["status"] == "SUCCESS":
                fields["status"] = "NEEDS_REVIEW"
            extra = "; ".join(violations)
            fields["notes"] = (
                fields["notes"] + "; " + extra if fields["notes"] else extra
            )

        rows.append(
            {
                "old_job_name":      job_name,
                "new_job_name":      new_name,
                "status":            fields["status"],
                "notes":             fields["notes"],
                "app_code":          fields["app_code"],
                "instance":          fields["instance"],
                "env":               fields["env"],
                "schedule":          fields["schedule"],
                "as_job_type":       fields["as_job_type"],
                "subject_code":      fields["subject_code"],
                "etl_type":          fields["etl_type"],
                "job_id":            str(fields["job_id"]).zfill(4),
                "suffix":            fields["suffix"],
                "original_job_type": job_type,
            }
        )

    return pd.DataFrame(rows)


# ── Output Writer ──────────────────────────────────────────────────────────────

def write_output(df: pd.DataFrame, path: Path) -> None:
    """Write output DataFrame to CSV, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"\nOutput written -> {path}")


# ── CLI Entry Point ────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert legacy Autosys job names to positional naming standard."
    )
    parser.add_argument(
        "--config",
        default=str(BASE_DIR / "config.yaml"),
        help="Path to config.yaml (default: ./config.yaml)",
    )
    parser.add_argument(
        "--input",
        default=str(BASE_DIR / "input" / "jobs_export.csv"),
        help="Path to input jobs CSV (default: ./input/jobs_export.csv)",
    )
    parser.add_argument(
        "--output",
        default=str(BASE_DIR / "output" / "renamed_jobs.csv"),
        help="Path to output CSV (default: ./output/renamed_jobs.csv)",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    input_path  = Path(args.input)
    output_path = Path(args.output)

    print("=== Autosys Job Renaming Tool ===")

    # Config
    print(f"Loading config:   {config_path}")
    cfg = load_config(config_path)
    print(f"  instance_code:  {cfg['instance_code']}")
    print(f"  env_code:       {cfg['env_code']}")
    print(f"  ID ranges: raw=1000 conformed=2000 curated=3000 streaming=4000 ndm=5000 other=6000")

    # Mappings
    print("\nLoading mappings...")
    maps = load_mappings(cfg)
    print(f"  app codes:     {len(maps['app'])} entries")
    print(f"  schedule codes: {len(maps['schedule'])} entries")
    print(
        f"  subject areas: {len(maps['subj_exact'])} exact + "
        f"{len(maps['subj_prefix'])} prefix + "
        f"{len(maps['subj_regex'])} regex"
    )
    print(f"  etl types:     {len(maps['etl'])} entries")
    print(f"  as job types:  {len(maps['as_type'])} entries")

    # Input
    if not input_path.exists():
        print(f"\nERROR: Input file not found: {input_path}")
        print("       Place your SQL export there with columns: job_name, job_type")
        sys.exit(1)

    print(f"\nLoading input:   {input_path}")
    df = pd.read_csv(input_path, dtype=str).fillna("")
    df.columns = df.columns.str.strip().str.lower()

    required_cols = {"job_name", "job_type"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        print(f"ERROR: Input CSV missing required columns: {missing_cols}")
        print(f"       Found columns: {list(df.columns)}")
        sys.exit(1)

    # Skip any embedded header rows (e.g. row 62 in jobs_export.csv which has
    # a duplicate header line with non-numeric Row sentinel)
    if "row" in df.columns:
        before = len(df)
        df = df[pd.to_numeric(df["row"], errors="coerce").notna()].reset_index(drop=True)
        skipped = before - len(df)
        if skipped:
            print(f"  Skipped {skipped} embedded header row(s)")

    print(f"Processing {len(df):,} jobs...")
    result_df = process_all(df, maps, cfg)

    # Summary table
    print("\n=== Processing Summary ===")
    counts = result_df["status"].value_counts()
    total  = len(result_df)
    for status in ["SUCCESS", "NEEDS_REVIEW", "EXCEPTION"]:
        n   = counts.get(status, 0)
        pct = 100 * n / max(total, 1)
        bar = "#" * int(pct / 2)
        print(f"  {status:<15}: {n:>6,}  ({pct:5.1f}%)  {bar}")
    print(f"  {'TOTAL':<15}: {total:>6,}")

    # Show first few exceptions to guide immediate action
    exceptions = result_df[result_df["status"] == "EXCEPTION"]
    if not exceptions.empty:
        print(f"\n=== First {min(5, len(exceptions))} EXCEPTION rows ===")
        for _, r in exceptions.head(5).iterrows():
            name_trunc = r["old_job_name"][:50]
            print(f"  {name_trunc:<50}  {r['notes']}")

    write_output(result_df, output_path)
    print(f"Rows written: {total:,}")

    print("\nNext steps:")
    print("  1. Open output/renamed_jobs.csv")
    print("  2. Filter status=EXCEPTION  -> fix missing entries in app_codes.csv")
    print("  3. Filter status=NEEDS_REVIEW -> verify schedule / ETL type choices")
    print("  4. Add patterns to subject_areas.csv and re-run until EXCEPTION count = 0")


if __name__ == "__main__":
    main()
