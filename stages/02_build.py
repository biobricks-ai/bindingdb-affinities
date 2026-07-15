import pandas as pd
import zipfile
import os
import glob
import pyarrow as pa
import pyarrow.parquet as pq

def process_data():
    zip_path = "download/data.zip"
    extract_dir = "download/extracted"
    os.makedirs(extract_dir, exist_ok=True)
    os.makedirs("brick", exist_ok=True)
    
    print("Extracting zip...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
        
    tsv_files = glob.glob(os.path.join(extract_dir, "*.tsv"))
    if not tsv_files:
        all_files = [os.path.join(dp, f) for dp, dn, filenames in os.walk(extract_dir) for f in filenames]
        tsv_path = max(all_files, key=os.path.getsize)
    else:
        tsv_path = tsv_files[0]
        
    print(f"Processing {tsv_path}...")
    
    parquet_path = "brick/data.parquet"
    writer = None
    chunksize = 100000
    
    rename_map = {
        'Ligand SMILES': 'smiles',
        'Ligand InChI': 'inchi',
        'Ligand InChI Key': 'inchikey',
        'Target Name Assigned by Curator or DataSource': 'target_name',
        'Target Name': 'target_name',
        'Ki (nM)': 'ki_nm',
        'IC50 (nM)': 'ic50_nm',
        'Kd (nM)': 'kd_nm',
        'EC50 (nM)': 'ec50_nm',
        # BindingDB uses these exact kinetic-rate headers; keep legacy keys as fallbacks.
        'kon (M-1-s-1)': 'kon',
        'koff (s-1)': 'koff',
        'kon (1/Ms)': 'kon',
        'koff (1/s)': 'koff',
        'pH': 'ph',
        'Temp (C)': 'temp_c',
        'PMID': 'pubmed_id',
        'PubChem CID': 'pubchem_cid',
    }

    # BindingDB target columns are per-chain-suffixed (e.g. "... Chain 1", "... Chain 2").
    # Chain 1 is the primary target; fall back to later chains where chain 1 is null.
    max_chains = 20
    target_seq_cols = [f'BindingDB Target Chain Sequence {i}' for i in range(1, max_chains + 1)]
    uniprot_cols = [f'UniProt (SwissProt) Primary ID of Target Chain {i}' for i in range(1, max_chains + 1)]

    def coalesce_chains(chunk, cols):
        result = None
        for col in cols:
            if col not in chunk.columns:
                continue
            series = chunk[col]
            if result is None:
                result = series.copy()
            else:
                result = result.fillna(series)
        return result

    # Define explicit schema to avoid chunk mismatch
    fields = [
        pa.field('smiles', pa.string()),
        pa.field('inchi', pa.string()),
        pa.field('inchikey', pa.string()),
        pa.field('target_sequence', pa.string()),
        pa.field('target_name', pa.string()),
        pa.field('ki_nm', pa.float64()),
        pa.field('ic50_nm', pa.float64()),
        pa.field('kd_nm', pa.float64()),
        pa.field('ec50_nm', pa.float64()),
        # Relation/qualifier for each affinity: '=', '<' (upper bound / more potent),
        # or '>' (lower bound / less potent). BindingDB stores many values as censored
        # (e.g. ">100000", "<0.03"); we keep the magnitude in *_nm and the direction here.
        pa.field('ki_relation', pa.string()),
        pa.field('ic50_relation', pa.string()),
        pa.field('kd_relation', pa.string()),
        pa.field('ec50_relation', pa.string()),
        pa.field('kon', pa.float64()),
        pa.field('koff', pa.float64()),
        pa.field('ph', pa.float64()),
        pa.field('temp_c', pa.float64()),
        pa.field('pubmed_id', pa.string()),
        pa.field('pubchem_cid', pa.float64()),
        pa.field('uniprot_id', pa.string()),
        # True when at least one of Ki/Kd/IC50/EC50 has a value (censored or exact).
        pa.field('has_affinity', pa.bool_()),
    ]
    explicit_schema = pa.schema(fields)
    desired_columns = [f.name for f in fields]

    import re
    _qual_re = re.compile(r'^\s*([<>]=?|~)?\s*(-?\d+\.?\d*(?:[eE][-+]?\d+)?)\s*$')

    def parse_affinity(series):
        """Split a BindingDB affinity string column into (magnitude float, relation str).

        Handles censored values like ">100000" and "<0.03" that would otherwise be
        dropped by a plain numeric coercion. Returns (values, relations) Series.
        Relation is '=' for exact values, '<'/'>' for bounds, None when no value.
        """
        s = series.astype(str).str.strip()
        m = s.str.extract(_qual_re)
        rel = m[0].where(m[1].notna())
        rel = rel.fillna('=')
        rel = rel.where(m[1].notna())  # keep None where there was no number
        val = pd.to_numeric(m[1], errors='coerce')
        rel = rel.where(val.notna())
        return val, rel

    try:
        df_iter = pd.read_csv(tsv_path, sep='\t', chunksize=chunksize, on_bad_lines='skip', encoding='utf-8', low_memory=False)
    except UnicodeDecodeError:
        df_iter = pd.read_csv(tsv_path, sep='\t', chunksize=chunksize, on_bad_lines='skip', encoding='latin1', low_memory=False)

    count = 0
    writer = pq.ParquetWriter(parquet_path, explicit_schema)

    for chunk in df_iter:
        chunk.columns = [c.strip() for c in chunk.columns]
        new_df = pd.DataFrame(index=chunk.index)
        
        for src_col, dest_col in rename_map.items():
            if src_col in chunk.columns:
                if dest_col not in new_df.columns:
                     new_df[dest_col] = chunk[src_col]
                else:
                     new_df[dest_col] = new_df[dest_col].fillna(chunk[src_col])
        
        # Chain-suffixed target columns: coalesce across chains (chain 1 = primary).
        seq = coalesce_chains(chunk, target_seq_cols)
        if seq is not None:
            new_df['target_sequence'] = seq
        uni = coalesce_chains(chunk, uniprot_cols)
        if uni is not None:
            new_df['uniprot_id'] = uni

        if 'smiles' not in new_df.columns and 'SMILES' in chunk.columns:
             new_df['smiles'] = chunk['SMILES']
                 
        # Parse affinity columns, preserving censored ("<"/">") values as
        # magnitude + relation instead of dropping them via numeric coercion.
        affinity_map = {'ki_nm': 'ki_relation', 'ic50_nm': 'ic50_relation',
                        'kd_nm': 'kd_relation', 'ec50_nm': 'ec50_relation'}
        for val_col, rel_col in affinity_map.items():
            if val_col in new_df.columns:
                v, r = parse_affinity(new_df[val_col])
                new_df[val_col] = v
                new_df[rel_col] = r
            else:
                new_df[val_col] = None
                new_df[rel_col] = None

        for col in desired_columns:
            if col not in new_df.columns:
                new_df[col] = None

        new_df = new_df[desired_columns]

        # Temperature values carry a unit suffix (e.g. "37.00 C"); pull the leading
        # numeric magnitude so plain numeric coercion doesn't silently null them.
        if 'temp_c' in new_df.columns:
            new_df['temp_c'] = new_df['temp_c'].astype(str).str.extract(
                r'(-?\d+\.?\d*)', expand=False)

        # Coerce types for pandas to avoid Pyarrow conversion errors
        numeric_cols = ['kon', 'koff', 'ph', 'temp_c', 'pubchem_cid']
        for c in numeric_cols:
            new_df[c] = pd.to_numeric(new_df[c], errors='coerce')

        # has_affinity: True when any of the four measures has a value.
        new_df['has_affinity'] = new_df[['ki_nm', 'ic50_nm', 'kd_nm', 'ec50_nm']].notna().any(axis=1)

        string_cols = ['smiles', 'inchi', 'inchikey', 'target_sequence', 'target_name',
                       'pubmed_id', 'uniprot_id', 'ki_relation', 'ic50_relation',
                       'kd_relation', 'ec50_relation']
        for c in string_cols:
            new_df[c] = new_df[c].astype(str).replace({'nan': None, 'None': None})

        # Drop rows without SMILES
        new_df = new_df.dropna(subset=['smiles'])
        
        if new_df.empty:
            continue
            
        # Create table with explicit schema
        try:
            table = pa.Table.from_pandas(new_df, schema=explicit_schema, preserve_index=False)
            writer.write_table(table)
            count += len(new_df)
            if count % 100000 == 0:
                print(f"Processed {count} rows...")
        except Exception as e:
            print(f"Error writing chunk: {e}")
            continue

    writer.close()
    print(f"Done. Total rows: {count}")

if __name__ == "__main__":
    process_data()
