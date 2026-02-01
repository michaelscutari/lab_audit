#!/usr/bin/env python3
"""
Convert GDU JSON export to Parquet with a tree-optimized schema.
Streams JSON and writes parquet in batches to keep memory bounded.
"""

import argparse
import os
import sys
from dataclasses import dataclass
from typing import Iterator, Optional, List

import ijson
import pyarrow as pa
import pyarrow.parquet as pq


BATCH_SIZE = 100_000

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


@dataclass
class DirFrame:
    name: str
    path: str
    parent: str
    depth: int
    size: int
    usage: int
    item_count: int = 0


def _build_path(parent_path: str, name: str) -> str:
    if parent_path:
        current_path = os.path.join(parent_path, name)
    else:
        current_path = name or "/"
    if not current_path.startswith("/"):
        current_path = "/" + current_path
    return current_path


def _coerce_int(value) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except Exception:
        return 0


def _make_dir_frame(info: dict, parent: Optional[DirFrame]) -> DirFrame:
    name = info.get("name", "") or ""
    parent_path = parent.path if parent else ""
    depth = (parent.depth + 1) if parent else 0
    current_path = _build_path(parent_path, name)
    return DirFrame(
        name=name or "/",
        path=current_path,
        parent=parent_path,
        depth=depth,
        size=_coerce_int(info.get("asize", 0)),
        usage=_coerce_int(info.get("dsize", 0)),
    )


def _make_file_row(info: dict, parent: Optional[DirFrame]) -> Optional[dict]:
    name = info.get("name", "")
    if name is None:
        name = ""
    if name == "":
        return None
    parent_path = parent.path if parent else ""
    depth = (parent.depth + 1) if parent else 0
    current_path = _build_path(parent_path, name)
    return {
        "path": current_path,
        "name": name,
        "parent": parent_path,
        "depth": depth,
        "size": _coerce_int(info.get("asize", 0)),
        "usage": _coerce_int(info.get("dsize", 0)),
        "is_dir": False,
        "item_count": 0,
    }


def _dir_frame_to_row(frame: DirFrame) -> dict:
    return {
        "path": frame.path,
        "name": frame.name or "/",
        "parent": frame.parent,
        "depth": frame.depth,
        "size": frame.size,
        "usage": frame.usage,
        "is_dir": True,
        "item_count": frame.item_count,
    }


def stream_nodes(filepath: str) -> Iterator[dict]:
    """
    Stream NCDU-format JSON and yield rows.
    Directories are emitted post-order so item_count can be computed.
    """
    with open(filepath, "rb") as f:
        parser = ijson.basic_parse(f)

        container_stack = []
        dir_stack: List[DirFrame] = []

        for event, value in parser:
            if event == "start_array":
                arr_ctx = {"type": "unknown", "index": 0, "frame": None}
                container_stack.append({"type": "array", "ctx": arr_ctx})
                continue

            if event == "end_array":
                arr_ctx = container_stack.pop()["ctx"]
                if arr_ctx["type"] == "dir" and arr_ctx.get("frame") is not None:
                    frame = arr_ctx["frame"]
                    yield _dir_frame_to_row(frame)
                    if dir_stack and dir_stack[-1] is frame:
                        dir_stack.pop()
                    if dir_stack:
                        dir_stack[-1].item_count += frame.item_count

                if container_stack and container_stack[-1]["type"] == "array":
                    container_stack[-1]["ctx"]["index"] += 1
                continue

            if event == "start_map":
                obj_ctx = {"data": {}, "key": None}
                container_stack.append({"type": "object", "ctx": obj_ctx})
                continue

            if event == "map_key":
                container_stack[-1]["ctx"]["key"] = value
                continue

            if event in ("string", "number", "boolean", "null"):
                if not container_stack:
                    continue
                top = container_stack[-1]
                if top["type"] == "object":
                    key = top["ctx"]["key"]
                    if key == "name":
                        top["ctx"]["data"]["name"] = value
                    elif key in ("asize", "dsize"):
                        top["ctx"]["data"][key] = _coerce_int(value)
                elif top["type"] == "array":
                    arr_ctx = top["ctx"]
                    if arr_ctx["type"] == "unknown" and arr_ctx["index"] == 0:
                        arr_ctx["type"] = "top"
                    arr_ctx["index"] += 1
                continue

            if event == "end_map":
                obj_ctx = container_stack.pop()["ctx"]
                data = obj_ctx["data"]
                parent = container_stack[-1] if container_stack else None

                if parent and parent["type"] == "array":
                    arr_ctx = parent["ctx"]
                    if arr_ctx["type"] == "unknown" and arr_ctx["index"] == 0:
                        arr_ctx["type"] = "dir" if data else "top"

                    if arr_ctx["type"] == "dir":
                        if arr_ctx["index"] == 0 and arr_ctx.get("frame") is None:
                            frame = _make_dir_frame(data, dir_stack[-1] if dir_stack else None)
                            arr_ctx["frame"] = frame
                            dir_stack.append(frame)
                        else:
                            row = _make_file_row(data, dir_stack[-1] if dir_stack else None)
                            if row is not None:
                                yield row
                                if dir_stack:
                                    dir_stack[-1].item_count += 1

                    arr_ctx["index"] += 1
                else:
                    row = _make_file_row(data, None)
                    if row is not None:
                        yield row
                continue


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
    print("Parsing tree structure...")

    rows = stream_nodes(args.input)
    total = write_batched_parquet(rows, args.output)

    print(f"Success! Wrote {total:,} rows to {args.output}")

    # Verify schema without loading all data
    print("\nSchema verification:")
    pq_file = pq.ParquetFile(args.output)
    print(pq_file.schema_arrow)
    print(f"\nTotal rows: {pq_file.metadata.num_rows:,}")


if __name__ == '__main__':
    main()
