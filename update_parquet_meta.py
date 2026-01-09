#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   update_parquet_meta.py
@Time    :   2026/01/06
@Author  :   XinkaiJi
@Contact :   xinkaiji@hotmail.com
@Version :   1.0
@Software:   Cursor
@Desc    :  
Update Parquet file metadata using a JSON file.
This script is useful when users manually modify the JSON metadata and want to sync
changes back to the corresponding Parquet file's metadata. 
'''

import argparse
import json
import os
import sys

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    sys.exit("Error: pyarrow is required. Please install it.")


def update_parquet_meta(parquet_path: str, json_path: str, output_path: str = None, compression: str = 'zstd'):
    """
    Read a Parquet file and a JSON file, update the Parquet file's 'dataset_meta'
    metadata field with the JSON content, and save the file.
    """
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Parquet file not found: {parquet_path}")
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"JSON file not found: {json_path}")

    # 1. Load the new metadata from JSON
    print(f"Reading metadata from: {json_path}")
    with open(json_path, 'r', encoding='utf-8') as f:
        new_meta_dict = json.load(f)

    # Convert to JSON string and then bytes
    new_meta_json_str = json.dumps(new_meta_dict, ensure_ascii=False)
    
    # 2. Read the existing Parquet file
    print(f"Reading Parquet file: {parquet_path}")
    table = pq.read_table(parquet_path)

    # 3. Prepare updated metadata
    # Get existing metadata (ensure it's not None)
    existing_meta = table.schema.metadata or {}
    
    # Update 'dataset_meta'. We keep other keys (like 'created_by') if they exist.
    # Note: Keys in pyarrow metadata are usually bytes.
    updated_meta = {
        **existing_meta,
        b'dataset_meta': new_meta_json_str.encode('utf-8')
    }

    # Replace schema metadata in the table
    new_table = table.replace_schema_metadata(updated_meta)

    # 4. Write back to Parquet
    if output_path is None:
        output_path = parquet_path
        action = "Overwriting"
    else:
        action = "Writing to"
    
    print(f"{action} Parquet file: {output_path}")
    # Use the same compression default as export_site_field_csv.py (zstd)
    pq.write_table(new_table, output_path, compression=compression)
    print("Done.")


def main():
    parser = argparse.ArgumentParser(
        description="Update Parquet metadata with content from a JSON file."
    )
    parser.add_argument('parquet', help="Path to the source Parquet file")
    parser.add_argument('json', help="Path to the JSON file containing new metadata")
    parser.add_argument('--output', '-o', default=None, help="Output Parquet path. If not provided, overwrites the input Parquet file.")
    parser.add_argument('--compression', default='zstd', help="Compression to use when writing Parquet (default: zstd)")

    args = parser.parse_args()

    try:
        update_parquet_meta(args.parquet, args.json, args.output, args.compression)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

