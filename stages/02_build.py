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
        'BindingDB Target Chain Sequence': 'target_sequence',
        'Target Name Assigned by Curator or DataSource': 'target_name',
        'Target Name': 'target_name', 
        'Ki (nM)': 'ki_nm',
        'IC50 (nM)': 'ic50_nm',
        'Kd (nM)': 'kd_nm',
        'EC50 (nM)': 'ec50_nm',
        'kon (1/Ms)': 'kon',
        'koff (1/s)': 'koff',
        'pH': 'ph',
        'Temp (C)': 'temp_c',
        'PMID': 'pubmed_id',
        'PubChem CID': 'pubchem_cid',
        'UniProt (SwissProt) Primary ID of Target Chain': 'uniprot_id'
    }

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
        pa.field('kon', pa.float64()),
        pa.field('koff', pa.float64()),
        pa.field('ph', pa.float64()),
        pa.field('temp_c', pa.float64()),
        pa.field('pubmed_id', pa.string()),
        pa.field('pubchem_cid', pa.float64()),
        pa.field('uniprot_id', pa.string())
    ]
    explicit_schema = pa.schema(fields)
    desired_columns = [f.name for f in fields]

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
        
        if 'smiles' not in new_df.columns and 'SMILES' in chunk.columns:
             new_df['smiles'] = chunk['SMILES']
                 
        for col in desired_columns:
            if col not in new_df.columns:
                new_df[col] = None
        
        new_df = new_df[desired_columns]
        
        # Coerce types for pandas to avoid Pyarrow conversion errors
        numeric_cols = ['ki_nm', 'ic50_nm', 'kd_nm', 'ec50_nm', 'kon', 'koff', 'ph', 'temp_c', 'pubchem_cid']
        for c in numeric_cols:
            new_df[c] = pd.to_numeric(new_df[c], errors='coerce')
            
        string_cols = ['smiles', 'inchi', 'inchikey', 'target_sequence', 'target_name', 'pubmed_id', 'uniprot_id']
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
