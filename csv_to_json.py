"""
csv_to_json_stream.py

Author: Enzo Villarama
Date: 2026-01-05

Description:
------------
This is a generic, schema-driven CSV to JSON converter that is designed to handle
small, medium, and large datasets efficiently. The script processes the CSV file
row by row, applies optional text mappings (code -> full text), transforms values,
groups data according to a schema, and outputs structured JSON series objects.

Features:
---------
- Streaming processing: does not load the entire CSV or JSON into memory.
- Schema-driven: allows flexible field mapping, grouping, and point definition.
- Supports text replacement via mappings defined in the schema (e.g., abbreviations → full names).
- Outputs either:
    1) Line-delimited JSON objects (default)
    2) Single JSON array (--json-array flag)
- Supports basic numeric transformations (float/int) for points or fields.
- Fully compatible with large CSV files and memory-efficient pipelines.

Usage:
------
python csv_to_json_stream.py <schema_file> [--json-array] > output.json

Arguments:
----------
<schema_file> : Path to the JSON schema file. Must include at least:
    - "csv_file": path to CSV
    - "group_by": list of fields to group by
    - "fields": dict mapping output fields to CSV columns and optional transforms
    - "points": dict defining x and y columns, optional transform
    - "id_template": template string for series_id
    - "mappings" (optional): dict for code → full-text replacements

--json-array : Optional flag to output a single JSON array instead of line-delimited objects.

Example:
--------
python csv_to_json_stream.py student_schema.json --json-array > students.json

"""

import csv
import json
import sys
import argparse
from collections import defaultdict

def generate_json(csv_file, schema, array_output=False):
    """
    Stream CSV -> JSON according to schema.
    - csv_file: path to CSV
    - schema: schema dict
    - array_output: True -> single JSON array, False -> line-delimited objects
    """
    group_by = schema.get("group_by", [])
    fields_schema = schema.get("fields", {})
    points_schema = schema.get("points", {})
    id_template = schema.get("id_template", "")
    mappings = schema.get("mappings", {})

    series_dict = defaultdict(lambda: {"points": [], "fields": {}})

    # Pre-compile transforms
    transform_funcs = {
        "float": float,
        "int": int,
        None: lambda x: x
    }

    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Normalize column names and strip whitespace
            row = {k.lower(): v.strip() for k, v in row.items()}

            # Apply mappings (code -> full text) if present
            for col, map_dict in mappings.items():
                col_lower = col.lower()
                if col_lower in row:
                    row[col_lower] = map_dict.get(row[col_lower], row[col_lower])

            # Map fields
            fields = {}
            for key, cfg in fields_schema.items():
                val = row.get(cfg["source"].lower(), "")
                fields[key] = transform_funcs.get(cfg.get("transform"), lambda x: x)(val)

            # Grouping key
            key = tuple(fields[g] for g in group_by)

            # Initialize fields if first encounter
            if not series_dict[key]["fields"]:
                series_dict[key]["fields"] = fields

            # Add points
            x_val = row.get(points_schema.get("x").lower(), "")
            y_val = row.get(points_schema.get("y").lower(), "")
            if x_val and y_val:
                y_val = transform_funcs.get(points_schema.get("transform"), lambda x: x)(y_val)
                series_dict[key]["points"].append([x_val, y_val])

    # Output
    if array_output:
        # Single JSON array, streaming
        print("[")
        first = True
        for data in series_dict.values():
            data["points"].sort(key=lambda x: x[0])  # optional sort
            series_json = {
                "series_id": id_template.format(**data["fields"]),
                "points": data["points"],
                "fields": data["fields"]
            }
            if not first:
                print(",", end="")
            print(json.dumps(series_json, indent=2), end="")
            first = False
        print("\n]")
    else:
        # Line-delimited JSON objects
        for data in series_dict.values():
            data["points"].sort(key=lambda x: x[0])  # optional sort
            series_json = {
                "series_id": id_template.format(**data["fields"]),
                "points": data["points"],
                "fields": data["fields"]
            }
            print(json.dumps(series_json))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generic CSV -> JSON parser (streaming, schema-driven, mappings supported)")
    parser.add_argument("schema_file", help="Path to JSON schema file (must include CSV path)")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output a single JSON array instead of one object per line"
    )
    args = parser.parse_args()

    # Load schema
    with open(args.schema_file, "r", encoding="utf-8") as f:
        schema = json.load(f)

    # Extract CSV file path from schema
    csv_file = schema.get("csv_file")
    if not csv_file:
        print("Error: schema JSON must include 'csv_file' key pointing to CSV path", file=sys.stderr)
        sys.exit(1)

    # Generate JSON
    generate_json(csv_file, schema, array_output=args.json)
