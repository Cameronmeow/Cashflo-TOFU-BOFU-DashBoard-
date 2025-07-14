# contact_dedup_streamlit.py
import streamlit as st
import pandas as pd
from difflib import SequenceMatcher

# ──────────────────────────────────────────────────────────────
# 🧹 Contact Dedup Tool: cluster by PAN + fuzzy-name, merge rows
# ──────────────────────────────────────────────────────────────

def normalize_text(s: str) -> str | None:
    if pd.isna(s) or not str(s).strip():
        return None
    return " ".join(str(s).strip().lower().split())

def normalize_phone(p: str) -> str | None:
    if pd.isna(p):
        return None
    digits = "".join(filter(str.isdigit, str(p)))
    if len(digits) == 10:
        return f"+91{digits}"
    if len(digits) > 10 and digits.startswith("91"):
        return f"+{digits}"
    return f"+{digits}" if digits else None

def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio() * 100

class UnionFind:
    def __init__(self, n: int):
        self.parent = list(range(n))
    def find(self, i: int) -> int:
        if self.parent[i] != i:
            self.parent[i] = self.find(self.parent[i])
        return self.parent[i]
    def union(self, i: int, j: int) -> None:
        ri, rj = self.find(i), self.find(j)
        if ri != rj:
            self.parent[rj] = ri

def dedupe_contacts_df(df: pd.DataFrame, threshold: float) -> pd.DataFrame:
    # strip "(No value)"
    df.replace("(No value)", pd.NA, inplace=True)
    # build a canonical full name
    df['canonical_name'] = (
        df['Full Name']
        .fillna(df.get('First Name','').fillna('') + ' ' + df.get('Last Name','').fillna(''))
        .apply(normalize_text)
    )
    # pick up phone/email/title columns
    phone_cols = [c for c in df.columns if any(k in c.lower() for k in ('phone','mobile'))]
    email_cols = [c for c in df.columns if 'email' in c.lower()]
    title_cols = [c for c in df.columns if 'title' in c.lower() or 'designation' in c.lower()]

    # aggregate into lists
    df['phones']      = df[phone_cols].apply(lambda row: list({normalize_phone(x) for x in row if pd.notna(x)}), axis=1)
    df['emails']      = df[email_cols].apply(lambda row: list({normalize_text(x)   for x in row if pd.notna(x)}), axis=1)
    df['titles']      = df[title_cols].apply(lambda row: list({normalize_text(x)   for x in row if pd.notna(x)}), axis=1)
    df['contact_ids'] = df.get('Contact ID', pd.Series()).apply(lambda x: [str(int(x))] if pd.notna(x) else [])

    records = []
    for pan, group in df.groupby('PAN Number'):
        idxs  = group.index.tolist()
        names = group.loc[idxs, 'canonical_name'].tolist()
        uf    = UnionFind(len(idxs))

        # fuzzy-union any pair above threshold
        for i in range(len(idxs)):
            if not names[i]:
                continue
            for j in range(i+1, len(idxs)):
                if names[j] and similarity(names[i], names[j]) >= threshold:
                    uf.union(i, j)

        clusters: dict[int, list[int]] = {}
        for k, idx in enumerate(idxs):
            root = uf.find(k)
            clusters.setdefault(root, []).append(idx)

        # merge each cluster
        for membership in clusters.values():
            sub = df.loc[membership]
            def pick(col: str):
                vals = sub[col].dropna()
                return vals.iloc[0] if not vals.empty else None

            merged = {
                'PAN Number':            pan,
                'Company name':          pick('Company name'),
                'Canonical Full Name':   pick('canonical_name'),
                'Aggregated Emails':     list({e for lst in sub['emails']      for e in lst}),
                'Aggregated Phones':     list({p for lst in sub['phones']      for p in lst}),
                'Aggregated Titles':     list({t for lst in sub['titles']      for t in lst}),
                'Street Address':        pick('Street Address'),
                'State':                 pick('State'),
                'Pincode':               pick('Pincode'),
                'Company ID':            pick('Company ID'),
                'Aggregated Contact IDs':list({cid for lst in sub['contact_ids'] for cid in lst}),
            }
            records.append(merged)

    return pd.DataFrame(records)

def render_page() -> None:
    st.subheader("🧹 Contact Dedup Tool")
    input_file = st.file_uploader("📂 Upload contacts CSV (must have a ‘PAN Number’ column)", type=["csv"])
    threshold  = st.slider("🔍 Name similarity threshold", 0, 100, 85)
    if input_file is None:
        st.info("Please upload your CSV to begin.")
        return

    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        st.error(f"Could not read CSV: {e}")
        return
    if 'PAN Number' not in df.columns:
        st.error("CSV has no ‘PAN Number’ column.")
        return

    deduped = dedupe_contacts_df(df, threshold)
    st.success(f"Deduplicated {len(df)} rows into {len(deduped)} unique contacts.")

    csv_bytes = deduped.to_csv(index=False).encode('utf-8')
    st.download_button(
        "⬇️ Download deduped CSV",
        data=csv_bytes,
        file_name="deduped_contacts.csv",
        mime="text/csv"
    )
