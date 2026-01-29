import requests
from bs4 import BeautifulSoup
import os
import shutil
import urllib.parse
import sys
import re

def download_file():
    base_url = "https://www.bindingdb.org/rwd/bind/chemsearch/marvin/Download.jsp"
    
    print(f"Fetching {base_url}...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(base_url, headers=headers, timeout=30)
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to fetch page: {e}")
        raise

    soup = BeautifulSoup(response.content, 'html.parser')
    
    candidates = []
    
    for a in soup.find_all('a', href=True):
        href = a['href']
        
        # Check if it is the download link we want
        if 'tsv.zip' in href:
            # Check for download_file param
            parsed = urllib.parse.urlparse(href)
            qs = urllib.parse.parse_qs(parsed.query)
            if 'download_file' in qs:
                real_path = qs['download_file'][0]
                candidates.append(real_path)
            else:
                candidates.append(href)

    print(f"Candidates found: {candidates}")

    download_link = None
    # Prefer BindingDB_All...tsv.zip
    # Filter out 2D, 3D if possible, though TSV usually doesn't have 2D/3D distinction in name (SDF does)
    # But sometimes they do: BindingDB_All_2D_...tsv.zip? No, usually SDF.
    # The list showed: BindingDB_All_202601_tsv.zip
    
    for c in candidates:
        if 'BindingDB_All' in c and 'tsv.zip' in c and 'Articles' not in c and 'ChEMBL' not in c and 'Patents' not in c:
            download_link = c
            break
            
    if not download_link and candidates:
        download_link = candidates[0]

    if not download_link:
        print("Could not find suitable link.")
        sys.exit(1)

    # Handle relative URLs
    if not download_link.startswith('http'):
        download_link = urllib.parse.urljoin("https://www.bindingdb.org", download_link)
        
    print(f"Downloading from {download_link}...")
    
    local_filename = "download/data.zip"
    os.makedirs("download", exist_ok=True)
    
    headers = {'User-Agent': 'Mozilla/5.0'}
    with requests.get(download_link, headers=headers, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            shutil.copyfileobj(r.raw, f)
            
    print("Download complete.")

if __name__ == "__main__":
    download_file()
