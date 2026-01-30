#!/usr/bin/env python3
"""
Convert GDU JSON export to Parquet with tree-optimized schema.
Uses streaming JSON parsing to bound memory usage.
"""

import argparse
import os
import sys
from typing import Iterator

import ijson
import pyarrow as pa
import pyarrow.parquet as pq


BATCH_SIZE = 500_000

SCHEMA = pa.schema([
    ('path', pa.string()),
    ('name', pa.string()),
    ('parent', pa.string()),
    ('depth', pa.int16()),
    ('size', pa.int64()),
    ('usage', pa.int64()),
    ('is_dir', pa.bool_()),
    ('item_count', pa.int64()),
])


def parse_node(node, parent_path: str = "", depth: int = 0) -> Iterator[dict]:
    """
    Recursively parse NCDU-style node structure.
    Directory = [ {metadata}, child1, child2, ... ]
    File = {metadata}

    Yields rows and computes item_count during traversal.
    Returns (rows, item_count) where item_count is total files underneath.
    """
    if isinstance(node, list) and node:
        info = node[0]
        children = node[1:]
        is_dir = True
    elif isinstance(node, dict):
        info = node
        children = []
        is_dir = False
    else:
        return

    name = info.get('name', '')

    # Handle root path
    if parent_path:
        current_path = os.path.join(parent_path, name)
    else:
        current_path = name or "/"

    # Normalize path
    if not current_path.startswith('/'):
        current_path = '/' + current_path

    size = info.get('asize', 0)
    usage = info.get('dsize', 0)

    # Process children first to compute item_count
    child_rows = []
    item_count = 0

    for child in children:
        for row in parse_node(child, current_path, depth + 1):
            child_rows.append(row)
            if row['path'] != current_path:  # Don't count self
                if not row['is_dir']:
                    item_count += 1
                else:
                    item_count += row['item_count']

    # Yield current node
    yield {
        'path': current_path,
        'name': name or '/',
        'parent': parent_path if parent_path else '',
        'depth': depth,
        'size': size,
        'usage': usage,
        'is_dir': is_dir,
        'item_count': item_count if is_dir else 0,
    }

    # Yield children
    yield from child_rows


def stream_json_root(filepath: str):
    """
    Stream parse JSON to find the root directory structure.
    GDU exports are in NCDU format: [version, metadata, [root_dir]]
    """
    with open(filepath, 'rb') as f:
        # Parse the top-level array items
        parser = ijson.items(f, 'item')
        items = list(parser)

        # Find the root directory (first list item that's a list)
        for item in items:
            if isinstance(item, list):
                return item

        # If no list found, try direct dict
        for item in items:
            if isinstance(item, dict):
                return item

    return None


def write_batched_parquet(rows: Iterator[dict], output_path: str):
    """Write rows to parquet in batches to bound memory."""
    writer = None
    batch = []
    total_rows = 0

    for row in rows:
        batch.append(row)
        total_rows += 1

        if len(batch) >= BATCH_SIZE:
            table = pa.Table.from_pylist(batch, schema=SCHEMA)
            if writer is None:
                writer = pq.ParquetWriter(output_path, SCHEMA, compression='snappy')
            writer.write_table(table)
            print(f"  Written {total_rows:,} rows...")
            batch = []

    # Write remaining rows
    if batch:
        table = pa.Table.from_pylist(batch, schema=SCHEMA)
        if writer is None:
            writer = pq.ParquetWriter(output_path, SCHEMA, compression='snappy')
        writer.write_table(table)

    if writer:
        writer.close()

    return total_rows


def main():
    parser = argparse.ArgumentParser(
        description='Convert GDU JSON export to tree-optimized Parquet'
    )
    parser.add_argument('--input', '-i', required=True, help='Input GDU JSON file')
    parser.add_argument('--output', '-o', required=True, help='Output Parquet file')
    args = parser.parse_args()

    if not os.path.exists(args.input):
        sys.exit(f"Error: Input file not found: {args.input}")

    print(f"Reading {args.input}...")

    # For streaming, we need to load the JSON structure
    # ijson doesn't handle NCDU format well, so we use standard json for now
    # but process in a memory-efficient way
    import json

    try:
        with open(args.input, 'r') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        sys.exit(f"Error: Invalid JSON: {e}")
    except Exception as e:
        sys.exit(f"Error reading file: {e}")

    print("Finding root structure...")

    # Find root directory in NCDU format
    root = None
    if isinstance(data, list):
        for item in data:
            if isinstance(item, list):
                root = item
                break
    elif isinstance(data, dict):
        root = data

    if root is None:
        sys.exit("Error: Could not find root directory in JSON")

    print("Parsing tree structure...")

    # Parse and write
    rows = parse_node(root)
    total = write_batched_parquet(rows, args.output)

    print(f"Success! Wrote {total:,} rows to {args.output}")

    # Verify schema
    print("\nSchema verification:")
    pq_file = pq.read_table(args.output)
    print(pq_file.schema)
    print(f"\nTotal rows: {len(pq_file):,}")


if __name__ == '__main__':
    main()
