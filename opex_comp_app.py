from __future__ import annotations

import base64
from io import BytesIO
from pathlib import Path
import tempfile
import json
import re
import csv
from difflib import get_close_matches

import pandas as pd
import streamlit as st

from tools.normalize_opex import (
    TARGET_CATEGORIES,
    build_monthly_comp,
    build_reconciliation,
    classify_rows,
    extract_file_rows,
    load_overrides,
    load_rules,
)


DEVELOPMENT_TYPES = ["new construction", "historic"]
ASSET_TYPES = ["family", "senior"]
BASE_DIR = Path(__file__).resolve().parent
APP_HOME = Path.home() / ".opex_comp_app"
METADATA_PATH = APP_HOME / "opex_app_metadata.json"
REGISTRY_PATH = APP_HOME / "property_registry.csv"
RULES_PATH = BASE_DIR / "config" / "expense_mapping_rules.json"
OVERRIDES_PATH = BASE_DIR / "config" / "expense_overrides.csv"
FONT_DIR = BASE_DIR / "assets" / "fonts"


def build_comp_table(
    monthly_df: pd.DataFrame,
    metadata_df: pd.DataFrame,
    selected_months: list[str],
    selected_development_types: list[str],
    selected_asset_types: list[str],
    period_basis: str,
    value_basis: str,
    exclude_below_avg_months: bool,
) -> pd.DataFrame:
    merged = monthly_df.merge(metadata_df, on="property_name", how="left")
    filtered = merged[
        merged["month"].isin(selected_months)
        & merged["development_type"].isin(selected_development_types)
        & merged["asset_type"].isin(selected_asset_types)
    ].copy()

    if filtered.empty:
        return pd.DataFrame()

    value_cols = TARGET_CATEGORIES.copy()
    monthly_totals = filtered.groupby(["property_name", "month"], as_index=False)[value_cols].sum()
    monthly_totals["total_opex"] = monthly_totals[value_cols].sum(axis=1)
    active_months = monthly_totals[monthly_totals["total_opex"].abs() > 0.000001].copy()
    averaging_mode = period_basis in {"monthly average", "annual average"} or value_basis == "per unit per year"

    if averaging_mode:
        eligible_months = active_months.copy()
        if exclude_below_avg_months and not eligible_months.empty:
            avg_totals = eligible_months.groupby("property_name")["total_opex"].mean().to_dict()
            eligible_months = eligible_months[
                eligible_months.apply(
                    lambda r: float(r["total_opex"]) >= float(avg_totals.get(r["property_name"], 0.0)),
                    axis=1,
                )
            ]

        active_month_counts = eligible_months.groupby("property_name")["month"].nunique().to_dict()
        filtered_for_calc = filtered.merge(
            eligible_months[["property_name", "month"]],
            on=["property_name", "month"],
            how="inner",
        )
        grouped = filtered_for_calc.groupby("property_name", as_index=False)[value_cols].sum()
    else:
        grouped = filtered.groupby("property_name", as_index=False)[value_cols].sum()
        active_month_counts = active_months.groupby("property_name")["month"].nunique().to_dict()

    if value_basis == "per unit per year":
        for col in value_cols:
            grouped[col] = grouped.apply(
                lambda r: ((r[col] / active_month_counts.get(r["property_name"], 1.0)) * 12.0)
                if active_month_counts.get(r["property_name"], 0) not in (0, None)
                else 0.0,
                axis=1,
            )
    elif period_basis == "monthly average":
        for col in value_cols:
            grouped[col] = grouped.apply(
                lambda r: (r[col] / active_month_counts.get(r["property_name"], 1.0))
                if active_month_counts.get(r["property_name"], 0) not in (0, None)
                else 0.0,
                axis=1,
            )
    elif period_basis == "annual average":
        for col in value_cols:
            grouped[col] = grouped.apply(
                lambda r: ((r[col] / active_month_counts.get(r["property_name"], 1.0)) * 12.0)
                if active_month_counts.get(r["property_name"], 0) not in (0, None)
                else 0.0,
                axis=1,
            )

    if value_basis in {"per unit", "per unit per year"}:
        units = metadata_df.set_index("property_name")["units"].to_dict()
        for col in value_cols:
            grouped[col] = grouped.apply(
                lambda r: (r[col] / units.get(r["property_name"], 1.0))
                if units.get(r["property_name"], 0) not in (0, None)
                else 0.0,
                axis=1,
            )

    display = grouped.set_index("property_name")[value_cols].T
    display.index.name = "expense_category"
    display.loc["total_opex"] = display.sum(axis=0)
    return display


def to_excel_bytes(comp_df: pd.DataFrame, recon_df: pd.DataFrame, line_df: pd.DataFrame) -> bytes:
    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        if not comp_df.empty:
            comp_df.to_excel(writer, sheet_name="Comp")
        recon_df.to_excel(writer, sheet_name="Reconciliation", index=False)
        line_df.to_excel(writer, sheet_name="Normalized_Lines", index=False)

        wb = writer.book
        accounting_fmt = '_($* #,##0.00_);_($* (#,##0.00);_($* "-"??_);_(@_)'
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            for col in ws.iter_cols(min_row=2, max_row=ws.max_row, min_col=2, max_col=ws.max_column):
                for cell in col:
                    if isinstance(cell.value, (int, float)):
                        cell.number_format = accounting_fmt
    out.seek(0)
    return out.read()


def load_metadata_map() -> dict:
    if not METADATA_PATH.exists():
        return {}
    try:
        return json.loads(METADATA_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_metadata_map(metadata_map: dict) -> None:
    METADATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    METADATA_PATH.write_text(json.dumps(metadata_map, indent=2, sort_keys=True), encoding="utf-8")


def load_overrides_df() -> pd.DataFrame:
    columns = ["match_type", "pattern", "category", "note"]
    if not OVERRIDES_PATH.exists():
        return pd.DataFrame(columns=columns)
    try:
        df = pd.read_csv(OVERRIDES_PATH)
    except Exception:
        return pd.DataFrame(columns=columns)
    for c in columns:
        if c not in df.columns:
            df[c] = ""
    return df[columns].fillna("")


def save_overrides_df(df: pd.DataFrame) -> None:
    OVERRIDES_PATH.parent.mkdir(parents=True, exist_ok=True)
    out = df[["match_type", "pattern", "category", "note"]].fillna("")
    out.to_csv(OVERRIDES_PATH, index=False, quoting=csv.QUOTE_MINIMAL)


def normalize_property_name(name: str) -> str:
    text = re.sub(r"[^a-z0-9]+", " ", (name or "").lower())
    return " ".join(text.split())


def canonical_development_type(value: str) -> str:
    text = (value or "").strip().lower()
    if "historic" in text:
        return "historic"
    return "new construction"


def canonical_asset_type(value: str) -> str:
    text = (value or "").strip().lower()
    if "senior" in text:
        return "senior"
    return "family"


def token_set(text: str) -> set[str]:
    tokens = set(normalize_property_name(text).split())
    stop = {"the", "at", "on", "of", "and", "residence", "senior", "living"}
    return {t for t in tokens if t not in stop}


def load_registry_map() -> dict:
    if not REGISTRY_PATH.exists():
        return {}
    try:
        df = pd.read_csv(REGISTRY_PATH)
    except Exception:
        return {}
    required = {"property_name", "development_type", "asset_type", "units"}
    if not required.issubset(set(df.columns)):
        return {}

    out = {}
    for _, row in df.iterrows():
        payload = {
            "development_type": canonical_development_type(str(row["development_type"])),
            "asset_type": canonical_asset_type(str(row["asset_type"])),
            "units": int(row["units"]) if pd.notna(row["units"]) else 1,
        }
        primary_key = normalize_property_name(str(row["property_name"]))
        if primary_key:
            out[primary_key] = payload
        alias = str(row["alias"]) if "alias" in df.columns and pd.notna(row["alias"]) else ""
        if alias.strip():
            out[normalize_property_name(alias)] = payload
    return out


def save_registry_dataframe(df: pd.DataFrame) -> None:
    REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(REGISTRY_PATH, index=False)


def registry_map_from_dataframe(df: pd.DataFrame) -> tuple[dict, str | None]:
    required = {"property_name", "development_type", "asset_type", "units"}
    if not required.issubset(set(df.columns)):
        missing = sorted(required.difference(set(df.columns)))
        return {}, f"Deal master list missing required columns: {', '.join(missing)}"

    out = {}
    for _, row in df.iterrows():
        payload = {
            "development_type": canonical_development_type(str(row["development_type"])),
            "asset_type": canonical_asset_type(str(row["asset_type"])),
            "units": int(row["units"]) if pd.notna(row["units"]) else 1,
        }
        primary_key = normalize_property_name(str(row["property_name"]))
        if primary_key:
            out[primary_key] = payload
        alias = str(row["alias"]) if "alias" in df.columns and pd.notna(row["alias"]) else ""
        if alias.strip():
            out[normalize_property_name(alias)] = payload
    return out, None


def resolve_metadata_for_property(property_name: str, file_stem: str, learned_map: dict, registry_map: dict) -> dict:
    candidate_keys = list(set(learned_map.keys()).union(set(registry_map.keys())))
    candidates = [normalize_property_name(property_name), normalize_property_name(file_stem)]

    for key in candidates:
        if key in learned_map:
            return learned_map[key]
        if key in registry_map:
            return registry_map[key]

    # fuzzy fallback
    for key in candidates:
        if not key:
            continue
        matches = get_close_matches(key, candidate_keys, n=1, cutoff=0.74)
        if matches:
            m = matches[0]
            if m in learned_map:
                return learned_map[m]
            return registry_map[m]

    # token overlap fallback
    candidate_token_sets = {k: token_set(k) for k in candidate_keys}
    for key in candidates:
        kt = token_set(key)
        if not kt:
            continue
        scored = []
        for k, ts in candidate_token_sets.items():
            if not ts:
                continue
            inter = len(kt.intersection(ts))
            union = len(kt.union(ts))
            score = inter / union if union else 0.0
            scored.append((score, inter, k))
        if scored:
            scored.sort(reverse=True)
            best_score, best_inter, best_key = scored[0]
            if best_score >= 0.4 or best_inter >= 1:
                if best_key in learned_map:
                    return learned_map[best_key]
                return registry_map[best_key]
    return {"development_type": "new construction", "asset_type": "family", "units": 1}


def build_metadata_df(monthly_df: pd.DataFrame, metadata_map: dict, registry_map: dict) -> pd.DataFrame:
    property_names = sorted(monthly_df["property_name"].unique().tolist())
    file_stems = (
        monthly_df.groupby("property_name")["file_name"]
        .first()
        .fillna("")
        .apply(lambda x: Path(str(x)).stem)
        .to_dict()
    )
    rows = []
    for property_name in property_names:
        entry = resolve_metadata_for_property(
            property_name=property_name,
            file_stem=file_stems.get(property_name, ""),
            learned_map=metadata_map,
            registry_map=registry_map,
        )
        rows.append(
            {
                "property_name": property_name,
                "development_type": entry.get("development_type", "new construction"),
                "asset_type": entry.get("asset_type", "family"),
                "units": int(entry.get("units", 1) or 1),
            }
        )
    return pd.DataFrame(rows)


def _font_weight_from_name(name: str) -> int:
    lower = name.lower()
    if "thin" in lower:
        return 100
    if "extralight" in lower or "ultralight" in lower:
        return 200
    if "light" in lower:
        return 300
    if "medium" in lower:
        return 500
    if "semibold" in lower or "demibold" in lower:
        return 600
    if "bold" in lower:
        return 700
    if "extrabold" in lower or "ultrabold" in lower:
        return 800
    if "black" in lower or "heavy" in lower:
        return 900
    return 400


def build_font_css() -> str:
    # Embed local Bicyclette font files when available so Streamlit Cloud can render them.
    candidates = sorted(
        [
            p
            for p in FONT_DIR.glob("*")
            if p.suffix.lower() in {".woff2", ".woff", ".ttf", ".otf"}
            and "bicyclette" in p.stem.lower()
        ]
    )
    parts: list[str] = []
    mime_by_ext = {
        ".woff2": "font/woff2",
        ".woff": "font/woff",
        ".ttf": "font/ttf",
        ".otf": "font/otf",
    }
    format_by_ext = {
        ".woff2": "woff2",
        ".woff": "woff",
        ".ttf": "truetype",
        ".otf": "opentype",
    }

    for font_path in candidates:
        ext = font_path.suffix.lower()
        mime = mime_by_ext.get(ext)
        fmt = format_by_ext.get(ext)
        if not mime or not fmt:
            continue
        try:
            encoded = base64.b64encode(font_path.read_bytes()).decode("ascii")
        except Exception:
            continue
        weight = _font_weight_from_name(font_path.stem)
        parts.append(
            (
                "@font-face{"
                'font-family:"Bicyclette";'
                "font-style:normal;"
                f"font-weight:{weight};"
                f"src:url(data:{mime};base64,{encoded}) format('{fmt}');"
                "font-display:swap;"
                "}"
            )
        )
    return "\n".join(parts)


def main() -> None:
    st.set_page_config(page_title="OpEx Comp Assistant", layout="wide")
    font_face_css = build_font_css()
    style_block = (
        "<style>\n"
        + font_face_css
        + """
        .stApp {
            font-family: "Bicyclette", "Aptos", "Segoe UI", "Helvetica Neue", Arial, sans-serif !important;
        }
        h1, .stMarkdown h1 {
            font-family: "Bicyclette", "Aptos", "Segoe UI", "Helvetica Neue", Arial, sans-serif !important;
            font-weight: 700 !important;
        }
        </style>
        """
    )
    st.markdown(
        style_block,
        unsafe_allow_html=True,
    )
    st.title("OpEx Comp Assistant")
    st.caption("Upload statements, normalize categories, and run comps by month / annual / per-unit.")

    APP_HOME.mkdir(parents=True, exist_ok=True)
    rules = load_rules(RULES_PATH)
    overrides = load_overrides(OVERRIDES_PATH)
    overrides_df = load_overrides_df()

    uploaded_registry_map = load_registry_map()

    uploads = st.file_uploader(
        "Upload financial statements (.xlsx, .xlsm)",
        type=["xlsx", "xlsm"],
        accept_multiple_files=True,
    )

    st.divider()
    with st.expander("Settings: Deal Master List", expanded=False):
        st.caption(
            "Optional; used for auto-filling development type, asset type, and units. "
            "Required columns: property_name, development_type, asset_type, units. Optional: alias."
        )
        if REGISTRY_PATH.exists():
            st.write(f"Current stored deal master list: `{REGISTRY_PATH}`")
        else:
            st.write("No stored deal master list yet.")

        deal_master_upload = st.file_uploader(
            "Upload/replace deal master list (CSV/XLSX)",
            type=["csv", "xlsx"],
            accept_multiple_files=False,
            key="deal_master_upload_bottom",
        )
        if st.button("Save deal master list", key="save_deal_master_list_btn"):
            if deal_master_upload is None:
                st.warning("Choose a deal master list file first.")
            else:
                try:
                    if deal_master_upload.name.lower().endswith(".csv"):
                        master_df = pd.read_csv(deal_master_upload)
                    else:
                        master_df = pd.read_excel(deal_master_upload)
                    _, master_err = registry_map_from_dataframe(master_df)
                    if master_err:
                        st.error(master_err)
                    else:
                        save_registry_dataframe(master_df)
                        st.success(f"Saved deal master list with {len(master_df)} rows.")
                        st.rerun()
                except Exception as exc:
                    st.error(f"Could not parse deal master list: {exc}")

    if not uploads:
        st.info("Upload one or more financial statements to start.")
        return

    extracted_rows = []
    source_meta = []
    with tempfile.TemporaryDirectory() as tmp_dir:
        for up in uploads:
            tmp_path = Path(tmp_dir) / up.name
            tmp_path.write_bytes(up.getbuffer())
            rows, meta = extract_file_rows(tmp_path, None)
            extracted_rows.extend(rows)
            source_meta.append(meta)

    normalized_rows, unmapped_rows = classify_rows(extracted_rows, rules, overrides)
    monthly_rows = build_monthly_comp(normalized_rows)
    recon_rows = build_reconciliation(monthly_rows, source_meta)

    if not monthly_rows:
        st.error("No monthly rows were produced from the uploaded files.")
        return

    monthly_df = pd.DataFrame(monthly_rows)
    normalized_df = pd.DataFrame(normalized_rows)
    recon_df = pd.DataFrame(recon_rows)
    unmapped_df = pd.DataFrame(unmapped_rows)

    property_names = sorted(monthly_df["property_name"].unique().tolist())
    months = sorted(monthly_df["month"].unique().tolist())

    if "metadata_map" not in st.session_state:
        st.session_state["metadata_map"] = load_metadata_map()
    metadata_map = st.session_state["metadata_map"]
    metadata_df = build_metadata_df(monthly_df, metadata_map, uploaded_registry_map)

    st.subheader("Deal Metadata")
    with st.form("metadata_form", clear_on_submit=False):
        st.data_editor(
            metadata_df,
            use_container_width=True,
            num_rows="fixed",
            column_config={
                "property_name": st.column_config.TextColumn("property_name", disabled=True),
                "development_type": st.column_config.SelectboxColumn(
                    "development_type",
                    options=DEVELOPMENT_TYPES,
                    required=True,
                ),
                "asset_type": st.column_config.SelectboxColumn(
                    "asset_type",
                    options=ASSET_TYPES,
                    required=True,
                ),
                "units": st.column_config.NumberColumn("units", min_value=1, step=1, required=True),
            },
            key="deal_metadata_editor",
        )
        saved = st.form_submit_button("Save metadata (press Enter)")

    if saved:
        edited_df = st.session_state.get("deal_metadata_editor", metadata_df)
        if isinstance(edited_df, pd.DataFrame):
            new_map = {}
            for _, row in edited_df.iterrows():
                prop = normalize_property_name(str(row["property_name"]))
                new_map[prop] = {
                    "development_type": str(row["development_type"]),
                    "asset_type": str(row["asset_type"]),
                    "units": max(1, int(row["units"])),
                }
            st.session_state["metadata_map"] = new_map
            save_metadata_map(new_map)
            st.success("Metadata saved.")
            metadata_df = build_metadata_df(monthly_df, new_map, uploaded_registry_map)
    else:
        metadata_df = build_metadata_df(monthly_df, st.session_state["metadata_map"], uploaded_registry_map)

    with st.expander("Filters", expanded=False):
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        with c1:
            selected_months = st.multiselect("Months", options=months, default=months)
        with c2:
            selected_development_types = st.multiselect(
                "Development type",
                options=DEVELOPMENT_TYPES,
                default=DEVELOPMENT_TYPES,
            )
        with c3:
            selected_asset_types = st.multiselect(
                "Asset type",
                options=ASSET_TYPES,
                default=ASSET_TYPES,
            )
        with c4:
            period_basis = st.selectbox(
                "Period basis",
                options=["per month", "annual total", "monthly average", "annual average"],
                index=0,
            )
        with c5:
            value_basis = st.selectbox(
                "Value basis",
                options=["total", "per unit", "per unit per year"],
                index=0,
            )
        with c6:
            exclude_below_avg_months = st.checkbox(
                "Exclude below-average months",
                value=True,
                help="For averaging modes, exclude months where total OpEx is below that property's average over selected months.",
            )

    if period_basis == "per month":
        month_for_view = st.selectbox("Month to display", options=selected_months or months, index=0)
        selected_months_for_comp = [month_for_view]
        st.caption(f"Showing {month_for_view} only.")
    else:
        selected_months_for_comp = selected_months or months
        if value_basis == "per unit per year":
            st.caption(
                f"Showing annualized per-unit average based on {len(selected_months_for_comp)} selected month(s) (avg month x 12 / units)."
            )
        elif period_basis == "annual total":
            st.caption(f"Showing total across {len(selected_months_for_comp)} selected month(s).")
        elif period_basis == "monthly average":
            st.caption(f"Showing average month across {len(selected_months_for_comp)} selected month(s).")
        else:
            st.caption(
                f"Showing annualized average based on {len(selected_months_for_comp)} selected month(s) (avg month x 12)."
            )

    comp_df = build_comp_table(
        monthly_df=monthly_df,
        metadata_df=metadata_df,
        selected_months=selected_months_for_comp,
        selected_development_types=selected_development_types or DEVELOPMENT_TYPES,
        selected_asset_types=selected_asset_types or ASSET_TYPES,
        period_basis=period_basis,
        value_basis=value_basis,
        exclude_below_avg_months=exclude_below_avg_months,
    )

    st.subheader("Comp View")
    if comp_df.empty:
        st.warning("No rows match current filters.")
    else:
        st.dataframe(
            comp_df.style.format("${:,.2f}"),
            use_container_width=True,
        )

    bad_recon = recon_df[recon_df["variance"].abs() > 0.01] if not recon_df.empty else recon_df
    if bad_recon is not None and not bad_recon.empty:
        st.error("Some uploaded files do not reconcile.")
        st.dataframe(bad_recon, use_container_width=True)
    else:
        st.success("All uploaded files reconcile to source total expenses.")

    with st.expander("Show Reconciliation"):
        st.dataframe(recon_df, use_container_width=True)
    with st.expander("Show Individual Normalized Line Items"):
        st.dataframe(normalized_df, use_container_width=True)
    with st.expander("Settings: Mapping Overrides", expanded=False):
        st.caption(
            "Add mapping rules used when line items are unmapped. "
            "Use `code` when possible for highest reliability."
        )
        with st.form("add_override_form", clear_on_submit=True):
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                match_type = st.selectbox("match_type", options=["code", "contains", "regex"], index=0)
            with c2:
                pattern = st.text_input("pattern", value="")
            with c3:
                category = st.selectbox("category", options=TARGET_CATEGORIES, index=3)
            with c4:
                note = st.text_input("note", value="")
            add_override = st.form_submit_button("Add override")

        if add_override:
            pat = pattern.strip()
            if not pat:
                st.warning("Pattern is required.")
            else:
                dup_mask = (
                    overrides_df["match_type"].str.lower().eq(match_type.lower())
                    & overrides_df["pattern"].str.lower().eq(pat.lower())
                    & overrides_df["category"].str.lower().eq(category.lower())
                )
                if dup_mask.any():
                    st.info("That override already exists.")
                else:
                    overrides_df = pd.concat(
                        [
                            overrides_df,
                            pd.DataFrame(
                                [
                                    {
                                        "match_type": match_type,
                                        "pattern": pat,
                                        "category": category,
                                        "note": note.strip(),
                                    }
                                ]
                            ),
                        ],
                        ignore_index=True,
                    )
                    save_overrides_df(overrides_df)
                    st.success("Override saved.")
                    st.rerun()

        if not overrides_df.empty:
            st.dataframe(overrides_df, use_container_width=True, hide_index=True)
        else:
            st.info("No overrides saved yet.")
    if not unmapped_df.empty:
        with st.expander("Show Unmapped Line Items"):
            st.dataframe(unmapped_df, use_container_width=True)
            st.caption("Tip: Copy an `account_code` or description and add it in `Settings: Mapping Overrides`.")

    download_bytes = to_excel_bytes(comp_df, recon_df, normalized_df)
    st.download_button(
        "Download Comp Workbook",
        data=download_bytes,
        file_name="opex_comp_output.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )



if __name__ == "__main__":
    main()
