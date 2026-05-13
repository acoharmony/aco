# © 2025 HarmonyCares
# All rights reserved.

"""SVA PDF extraction.

Scrapes signed SVA letter PDFs in ``$BRONZE/sva_raw/`` to assemble a CMS SVA
submission file. Each form is one or more PDF pages containing the
beneficiary's printed name, MBI, and signature date — sometimes as scanned
images, sometimes as native text. Provider name / NPI / TIN are NOT in the
form, so we resolve them by MBI lookup against silver (``sva.parquet``).

Filenames typically encode patient name + signature date (e.g.
``Amy Hula SVA 04 03 26.pdf``), which we use as a fallback when OCR misses.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from pypdf import PdfReader

from ._base import PluginRegistry

# Field regexes
_MBI_RE = re.compile(r"\b[1-9AC-HJ-NP-RT-Y][AC-HJ-NP-RT-Y\d]{10}\b")
_MBI_LOOSE_RE = re.compile(r"[1-9AC-HJ-NP-RT-Y][AC-HJ-NP-RT-Y0-9]{10}")
_DATE_LONG_RE = re.compile(
    r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{1,2}),?\s+(19\d{2}|20\d{2})\b",
    re.IGNORECASE,
)
_DATE_NUMERIC_RE = re.compile(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\b")

# Filename signature-date patterns: "5-11-26", "05.11.2026", "05112026", "5 11 26".
_FN_DATE_PATTERNS = [
    re.compile(r"(\d{1,2})[.\-_ ](\d{1,2})[.\-_ ](\d{2,4})"),
    re.compile(r"(\d{2})\.?(\d{2})(\d{4})"),  # 05.112026 -> 05 11 2026
]

_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


@dataclass
class FormExtraction:
    """One PDF -> one row of extracted SVA fields + provenance flags."""

    pdf_path: Path
    pdf_name: str
    name_filename: str | None = None
    sig_date_filename: date | None = None
    mbi: str | None = None
    signature_date: date | None = None
    birth_date: date | None = None
    first_name: str | None = None
    last_name: str | None = None
    used_ocr: bool = False
    pages_ocrd: int = 0
    ocr_dpi_used: int = 0
    mbi_source: str = "none"            # "ocr" | "demographics_lookup" | "none"
    demo_candidates: int = 0             # how many silver demo rows matched (0/1/>1)
    errors: list[str] = field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        return bool(self.mbi and self.signature_date and self.first_name and self.last_name)


class SvaPdfExtractPlugins(PluginRegistry):
    """Extract beneficiary identity + signature from signed SVA PDFs."""

    # ---- filename parsing -------------------------------------------------

    def parse_filename(self, stem: str) -> tuple[str | None, date | None]:
        """Pull beneficiary name + signature date hints out of a filename.

        Name cleanup is delegated to
        ``acoharmony._expressions._sva_log.clean_filename_to_name`` so the
        Mabel SFTP log analytics and this PDF extractor stay aligned.
        """
        from acoharmony._expressions._sva_log import clean_filename_to_name

        name = clean_filename_to_name(stem + ".pdf") if stem else None
        sig = self._parse_filename_date(stem)
        return name, sig

    def _parse_filename_date(self, s: str) -> date | None:
        for pat in _FN_DATE_PATTERNS:
            for match in pat.finditer(s):
                d = self._normalize_numeric_date(*match.groups())
                if d is not None:
                    return d
        return None

    @staticmethod
    def _normalize_numeric_date(a: str, b: str, c: str) -> date | None:
        try:
            mm, dd = int(a), int(b)
            yy = int(c)
            if yy < 100:
                # 2-digit year heuristic: 0-49 → 20xx (sig date), 50-99 → 19xx (DOB)
                yy += 2000 if yy < 50 else 1900
            if 1 <= mm <= 12 and 1 <= dd <= 31 and 1900 <= yy <= 2100:
                return date(yy, mm, dd)
        except (ValueError, TypeError):
            return None
        return None

    @staticmethod
    def split_full_name(full: str | None) -> tuple[str | None, str | None]:
        """Convention: last token is last name, the rest joined is first name.

        Handles middle names/initials by lumping them with the first name.
        Examples: ``Amy L Hula`` -> (``Amy L``, ``Hula``); ``Adan Garza`` ->
        (``Adan``, ``Garza``).
        """
        if not full:
            return None, None
        toks = [t for t in re.split(r"\s+", full.strip()) if t]
        if len(toks) == 1:
            return toks[0], None
        return " ".join(toks[:-1]), toks[-1]

    # ---- PDF text + OCR ---------------------------------------------------

    def extract_text(self, pdf_path: Path, ocr_dpi: int = 300) -> tuple[str, bool, int]:
        """Return (combined_text, used_ocr, pages_ocrd).

        Prefers native PDF text. Falls back to tesseract OCR when the
        native text is empty or near-empty (scanned form).
        """
        try:
            native = self._extract_native_text(pdf_path)
        except Exception:  # ALLOWED: pypdf occasionally barfs on malformed PDFs
            native = ""

        # If native text contains the patient/MBI cue, use it.
        if "Medicare Number" in native or len(native) > 500:
            return native, False, 0

        # Fall back to OCR (only if pdf2image + pytesseract are importable).
        ocr_text, n = self._ocr_text(pdf_path, dpi=ocr_dpi)
        return (native + "\n" + ocr_text), True, n

    @staticmethod
    def _extract_native_text(pdf_path: Path) -> str:
        reader = PdfReader(str(pdf_path))
        out: list[str] = []
        for page in reader.pages:
            try:
                out.append(page.extract_text() or "")
            except Exception:  # ALLOWED: single-page extraction failure shouldn't kill the file
                continue
        return "\n".join(out)

    @staticmethod
    def _ocr_text(pdf_path: Path, dpi: int = 300) -> tuple[str, int]:
        from pdf2image import convert_from_path
        import pytesseract

        try:
            images = convert_from_path(str(pdf_path), dpi=dpi)
        except Exception as e:
            raise RuntimeError(f"pdf2image failed: {e}") from e
        parts: list[str] = []
        for img in images:
            parts.append(pytesseract.image_to_string(img))
        return "\n".join(parts), len(images)

    # ---- field extractors from OCR text ----------------------------------

    def find_mbi(self, text: str) -> str | None:
        """Find an MBI in raw text. CMS spec: 11 chars, position-1 is
        ``[1-9AC-HJ-NP-RT-Y]``, positions 2-11 are alphanumerics excluding
        S/L/O/I/B/Z.

        Reference: CMS MBI format guide
        https://www.cms.gov/medicare/new-medicare-card/understanding-the-mbi.pdf

        Strategy: prefer matches in the 400-char window after the
        ``Medicare Number`` anchor (the box adjacent to the DOB on the
        patient-info page); only fall back to scanning the full document
        if that fails. The anchored search guards against picking up
        boilerplate words that happen to match the MBI character class
        (e.g. ``CAREHEALTPA``).
        """
        anchor = re.search(r"medicare\s*number", text, re.IGNORECASE)
        if anchor:
            window = text[anchor.end(): anchor.end() + 400]
            m = _MBI_RE.search(window)
            if m and self._looks_like_mbi(m.group(0)):
                return m.group(0)
            squashed = re.sub(r"\s+", "", window)
            m2 = _MBI_LOOSE_RE.search(squashed)
            if m2 and self._looks_like_mbi(m2.group(0)):
                return m2.group(0)
        # Last-resort: full-document scan with the strict regex.
        for cand in _MBI_RE.findall(text):
            if self._looks_like_mbi(cand):
                return cand
        return None

    @staticmethod
    def _looks_like_mbi(candidate: str) -> bool:
        """Reject all-letter strings (English words) and require >=4 digits.

        Real MBIs always contain digits in positions 4, 7, 8, 11 per CMS spec —
        all-alpha candidates that match the character class (like
        ``CAREHEALTPA``) are guaranteed to be OCR noise.
        """
        if len(candidate) != 11:
            return False
        digits = sum(c.isdigit() for c in candidate)
        return digits >= 4

    def find_signature_date(self, text: str) -> date | None:
        """Locate the signature date — the date appearing under ``Today's date``.

        If we can't isolate that, fall back to the most-common form-date in
        the document (form-completion long-form dates like 'May 11, 2026').
        Birth dates are typically numeric (MM/DD/YYYY) so we prefer long form.
        """
        # Prefer date appearing within ~80 chars after a "Today's date" anchor.
        anchor = re.search(r"today.?s?\s+date", text, re.IGNORECASE)
        if anchor:
            window = text[anchor.end(): anchor.end() + 200]
            d = self._first_date(window)
            if d is not None:
                return d
        # Otherwise the first long-form date in the document is usually the signature date.
        for m in _DATE_LONG_RE.finditer(text):
            d = self._parse_long_date(m.group(1), m.group(2), m.group(3))
            if d is not None:
                return d
        return None

    def _first_date(self, text: str) -> date | None:
        long_m = _DATE_LONG_RE.search(text)
        if long_m:
            return self._parse_long_date(long_m.group(1), long_m.group(2), long_m.group(3))
        num_m = _DATE_NUMERIC_RE.search(text)
        if num_m:
            return self._normalize_numeric_date(num_m.group(1), num_m.group(2), num_m.group(3))
        return None

    @staticmethod
    def _parse_long_date(mon: str, day: str, year: str) -> date | None:
        m = _MONTHS.get(mon[:3].lower())
        if not m:
            return None
        try:
            return date(int(year), m, int(day))
        except ValueError:
            return None

    def find_birth_date(self, text: str) -> date | None:
        """Locate the beneficiary's DOB.

        Strategy: anchor on the ``Date of Birth`` header. The DOB sits on
        the next non-empty line either alone (``May 14, 1979``) or as the
        leftmost token before the MBI on the same line. Fall back to the
        ``VOLUNTARY ALIGNMENT FORM for LAST, FIRST MM/DD/YYYY`` header.
        """
        # Anchored on the "Date of Birth" label
        anchor = re.search(r"date\s*of\s*birth", text, re.IGNORECASE)
        if anchor:
            window = text[anchor.end(): anchor.end() + 200]
            d = self._first_date(window)
            if d is not None and d.year < date.today().year - 5:
                # DOB sanity: must be >5 years ago (signature dates are recent).
                return d
        # Form header fallback: "VOLUNTARY ALIGNMENT FORM for X, Y 05/14/1979"
        m = re.search(
            r"VOLUNTARY ALIGNMENT FORM for[^\n]+?(\d{1,2})/(\d{1,2})/(\d{4})",
            text,
        )
        if m:
            return self._normalize_numeric_date(m.group(1), m.group(2), m.group(3))
        return None

    def find_name_in_text(self, text: str) -> tuple[str | None, str | None]:
        """Pull beneficiary name from the form body.

        Some forms have a 'VOLUNTARY ALIGNMENT FORM for LAST, FIRST DOB ...'
        header on every page. Others print 'Patient/Benificiary Name' followed
        by the name on the next line.
        """
        # Header form: "VOLUNTARY ALIGNMENT FORM for GARZA, ADAN 05/14/1979"
        m = re.search(
            r"VOLUNTARY ALIGNMENT FORM for\s+([A-Z][A-Z\-']+),\s+([A-Z][A-Z\-' ]+?)\s+\d{1,2}/\d{1,2}/\d{4}",
            text,
        )
        if m:
            return m.group(2).strip().title(), m.group(1).strip().title()

        # Inline form: line after "Patient/Benificiary Name" or "Patient Name"
        m2 = re.search(
            r"Patient[/Beneficiary]*\s*Name[^\n]*\n[^\n]*\n([^\n]+)",
            text,
            re.IGNORECASE,
        )
        if m2:
            line = m2.group(1).strip()
            # Discard the boilerplate "JOHN L SMITH" placeholder if present.
            if line.upper().replace(" ", "") in {"JOHNLSMITH", "JOHNSMITH"}:
                return None, None
            return self.split_full_name(line)
        return None, None

    # ---- one-PDF orchestrator --------------------------------------------

    def extract_one(
        self,
        pdf_path: Path,
        ocr_dpi: int = 400,
        retry_dpis: tuple[int, ...] = (500, 600),
        demo_df: pl.DataFrame | None = None,
    ) -> FormExtraction:
        """Extract a single SVA PDF into a ``FormExtraction``.

        OCR pipeline:

        1. Try native PDF text first (free, instant).
        2. If we fall through to OCR, run at ``ocr_dpi`` (default 400).
        3. If the MBI still doesn't parse, re-OCR at each ``retry_dpis``
           in turn until one yields a valid MBI or we exhaust the list.
        4. If we still have no MBI but ``demo_df`` is provided AND we have
           a usable ``(last_name, birth_date)`` pair, fall back to a
           demographics lookup and stamp ``mbi_source="demographics_lookup"``.
        """
        fe = FormExtraction(pdf_path=pdf_path, pdf_name=pdf_path.name)
        fn_name, fn_sig = self.parse_filename(pdf_path.stem)
        fe.name_filename = fn_name
        fe.sig_date_filename = fn_sig

        text = ""
        try:
            text, used_ocr, n_pages = self.extract_text(pdf_path, ocr_dpi=ocr_dpi)
            fe.used_ocr = used_ocr
            fe.pages_ocrd = n_pages
            fe.ocr_dpi_used = ocr_dpi if used_ocr else 0
        except Exception as e:  # ALLOWED: extraction errors are recorded, not fatal
            fe.errors.append(f"extract: {e}")

        fe.mbi = self.find_mbi(text)
        if fe.mbi:
            fe.mbi_source = "ocr"

        # DPI retry ladder — only meaningful when we actually OCR'd.
        if not fe.mbi and fe.used_ocr:
            for dpi in retry_dpis:
                try:
                    retry_text, _ = self._ocr_text(pdf_path, dpi=dpi)
                except Exception as e:  # ALLOWED: a retry failure isn't fatal
                    fe.errors.append(f"retry_dpi_{dpi}: {e}")
                    continue
                text = text + "\n" + retry_text
                fe.ocr_dpi_used = dpi
                fe.mbi = self.find_mbi(retry_text) or self.find_mbi(text)
                if fe.mbi:
                    fe.mbi_source = "ocr"
                    break

        fe.signature_date = self.find_signature_date(text) or fn_sig
        fe.birth_date = self.find_birth_date(text)

        first, last = self.find_name_in_text(text)
        if not (first and last):
            first, last = self.split_full_name(fn_name)
        fe.first_name = first
        fe.last_name = last

        # Demographics fallback for MBI.
        if not fe.mbi and demo_df is not None and fe.last_name and fe.birth_date:
            mbi, n = self.lookup_mbi_by_demographics(
                first_name=fe.first_name,
                last_name=fe.last_name,
                birth_date=fe.birth_date,
                demo_df=demo_df,
            )
            fe.demo_candidates = n
            if mbi:
                fe.mbi = mbi
                fe.mbi_source = "demographics_lookup"

        if not fe.mbi:
            fe.errors.append("missing_mbi")
        if not fe.signature_date:
            fe.errors.append("missing_signature_date")
        if not (fe.first_name and fe.last_name):
            fe.errors.append("missing_name")
        return fe

    # ---- demographics fallback -------------------------------------------

    def load_demographics(self, silver_path: Path) -> pl.DataFrame:
        """Load the deduped beneficiary demographics table from silver.

        Returns an empty DataFrame if the parquet is missing.
        """
        p = Path(silver_path) / "int_beneficiary_demographics_deduped.parquet"
        if not p.exists():
            return pl.DataFrame()
        return pl.read_parquet(p).select(
            pl.col("current_bene_mbi_id").alias("mbi"),
            pl.col("bene_fst_name").str.strip_chars().str.to_uppercase().alias("fst_name"),
            pl.col("bene_lst_name").str.strip_chars().str.to_uppercase().alias("lst_name"),
            pl.col("bene_dob").alias("dob"),
        ).filter(
            pl.col("mbi").is_not_null()
            & pl.col("lst_name").is_not_null()
            & pl.col("dob").is_not_null()
        )

    def lookup_mbi_by_demographics(
        self,
        first_name: str | None,
        last_name: str,
        birth_date: date,
        demo_df: pl.DataFrame,
    ) -> tuple[str | None, int]:
        """Look up MBI given (last_name, DOB) (+ first-name tie-breaker).

        Returns ``(mbi_or_None, candidate_count)``. ``mbi_or_None`` is set
        only when the lookup resolves to *exactly one* MBI. Multi-match (>1)
        is reported via ``candidate_count`` so the notebook can flag it.
        """
        if demo_df.is_empty():
            return None, 0
        last_norm = last_name.strip().upper()
        matches = demo_df.filter(
            (pl.col("lst_name") == last_norm) & (pl.col("dob") == birth_date)
        )
        if matches.height == 0:
            return None, 0
        # Tie-breaker: first-name initial or full first name.
        if matches.height > 1 and first_name:
            initial = first_name.strip()[:1].upper()
            narrowed = matches.filter(pl.col("fst_name").str.starts_with(initial))
            if narrowed.height >= 1:
                matches = narrowed
        if matches.height == 1:
            return matches["mbi"][0], 1
        return None, matches.height

    # ---- batch + dataframe assembly --------------------------------------

    def iter_pdfs(self, sva_raw: Path) -> list[Path]:
        return sorted(p for p in Path(sva_raw).glob("*.pdf") if p.is_file())

    def extractions_to_frame(self, extractions: list[FormExtraction]) -> pl.DataFrame:
        rows = [
            {
                "pdf_name": e.pdf_name,
                "mbi": e.mbi,
                "mbi_source": e.mbi_source,
                "first_name": e.first_name,
                "last_name": e.last_name,
                "signature_date": e.signature_date,
                "birth_date": e.birth_date,
                "name_filename": e.name_filename,
                "sig_date_filename": e.sig_date_filename,
                "used_ocr": e.used_ocr,
                "pages_ocrd": e.pages_ocrd,
                "ocr_dpi_used": e.ocr_dpi_used,
                "demo_candidates": e.demo_candidates,
                "complete": e.is_complete,
                "errors": "; ".join(e.errors) if e.errors else None,
            }
            for e in extractions
        ]
        return pl.DataFrame(rows)

    # ---- provider attribution from silver --------------------------------

    def attribute_providers(
        self, df: pl.DataFrame, sva_prior: pl.DataFrame
    ) -> pl.DataFrame:
        """Attach provider name/NPI/TIN by MBI lookup against prior SVAs.

        Each beneficiary's most-recently submitted SVA row is used as the
        source of truth for who they're aligned to.
        """
        if sva_prior.is_empty():
            return df.with_columns(
                pl.lit(None).cast(pl.Utf8).alias("provider_name"),
                pl.lit(None).cast(pl.Utf8).alias("provider_npi"),
                pl.lit(None).cast(pl.Utf8).alias("provider_tin"),
            )
        latest = (
            sva_prior.sort("sva_signature_date", descending=True, nulls_last=True)
            .unique(subset=["bene_mbi"], keep="first")
            .select(
                pl.col("bene_mbi").alias("mbi"),
                pl.col("sva_provider_name").alias("provider_name"),
                pl.col("sva_npi").alias("provider_npi"),
                pl.col("sva_tin").alias("provider_tin"),
            )
        )
        return df.join(latest, on="mbi", how="left")

    # ---- CMS layout (matches Sva dataclass aliases) ----------------------

    def build_cms_layout(self, df: pl.DataFrame, aco_id: str = "D0259") -> pl.DataFrame:
        """Project the extracted rows into the CMS SVA submission column order.

        Column headers and order match the spec validated by
        ``acoharmony._tables.sva.Sva``.
        """
        return df.select(
            pl.lit(aco_id).alias("ACO ID"),
            pl.col("mbi").alias("Beneficiary's MBI"),
            pl.col("first_name").alias("Beneficiary's First Name"),
            pl.col("last_name").alias("Beneficiary's Last Name"),
            pl.lit(None).cast(pl.Utf8).alias("Beneficiary's Street Address"),
            pl.lit(None).cast(pl.Utf8).alias("City"),
            pl.lit(None).cast(pl.Utf8).alias("State"),
            pl.lit(None).cast(pl.Utf8).alias("Zip"),
            pl.col("provider_name").alias(
                "Provider Name/Primary place the Beneficiary receives care (as it appears on the signed SVA letter)"
            ),
            pl.col("provider_name").alias(
                "Name of Individual  Participant Provider associated w/ attestation"
            ),
            pl.col("provider_npi").alias(
                "iNPI for Individual  Participant Provider (column J)"
            ),
            pl.col("provider_tin").alias(
                "TIN for Individual Participant Provider (column J)"
            ),
            pl.col("signature_date").alias("Signature Date on SVA letter"),
            pl.lit(None).cast(pl.Utf8).alias("Response Code (CMS to fill out)"),
        )

    def write_xlsx(self, cms_df: pl.DataFrame, out_path: Path) -> Path:
        """Write the assembled SVA file as xlsx with sheet name SVA_DATA."""
        cms_df.write_excel(workbook=str(out_path), worksheet="SVA_DATA")
        return out_path

    # ---- summary helpers --------------------------------------------------

    def summary(self, df: pl.DataFrame) -> dict[str, Any]:
        total = df.height
        complete = df.filter(pl.col("complete")).height
        with_mbi = df.filter(pl.col("mbi").is_not_null()).height
        mbi_via_ocr = df.filter(pl.col("mbi_source") == "ocr").height
        mbi_via_demo = df.filter(pl.col("mbi_source") == "demographics_lookup").height
        with_sig = df.filter(pl.col("signature_date").is_not_null()).height
        with_dob = df.filter(pl.col("birth_date").is_not_null()).height
        with_name = df.filter(
            pl.col("first_name").is_not_null() & pl.col("last_name").is_not_null()
        ).height
        ocrd = df.filter(pl.col("used_ocr")).height
        ambiguous_demo = df.filter(pl.col("demo_candidates") > 1).height
        return {
            "total": total,
            "complete": complete,
            "with_mbi": with_mbi,
            "mbi_via_ocr": mbi_via_ocr,
            "mbi_via_demographics": mbi_via_demo,
            "with_signature_date": with_sig,
            "with_birth_date": with_dob,
            "with_name": with_name,
            "ocrd": ocrd,
            "native_text": total - ocrd,
            "demo_ambiguous": ambiguous_demo,
        }
