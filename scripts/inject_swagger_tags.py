#!/usr/bin/env python3
"""Inject @tag.name/@tag.description from Go source into swagger spec.

swag v1.x with --parseDependency drops @tag annotations. This script
parses them from the Go source and injects them into the generated
swagger.json and swagger.yaml files.

Usage: inject_swagger_tags.py <main.go> <swagger.json> <swagger.yaml>
"""

import json
import os
import re
import sys


def parse_tags_from_go(go_file: str) -> list[dict]:
    """Extract @tag.name/@tag.description pairs from Go comments."""
    tags: list[dict] = []
    current_name: str | None = None

    with open(go_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            # Match: // @tag.name <name>
            m = re.match(r"^//\s*@tag\.name\s+(.+)$", line)
            if m:
                if current_name is not None:
                    tags.append({"name": current_name})
                current_name = m.group(1).strip()
                continue
            # Match: // @tag.description <desc>
            m = re.match(r"^//\s*@tag\.description\s+(.+)$", line)
            if m and current_name is not None:
                tags.append({"name": current_name, "description": m.group(1).strip()})
                current_name = None
                continue

    # Flush any name without a description
    if current_name is not None:
        tags.append({"name": current_name})

    return tags


def inject_json(path: str, tags: list[dict]) -> None:
    """Inject tags into swagger.json."""
    with open(path, encoding="utf-8") as f:
        spec = json.load(f)

    spec["tags"] = tags

    # Ensure tags appear near the top of the spec
    ordered = {}
    for key in ("schemes", "swagger", "info", "basePath", "tags"):
        if key in spec:
            ordered[key] = spec.pop(key)
    ordered.update(spec)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(ordered, f, indent=4)
        f.write("\n")


def inject_yaml(path: str, tags: list[dict]) -> None:
    """Inject tags into swagger.yaml using string manipulation (no PyYAML dep)."""
    with open(path, encoding="utf-8") as f:
        content = f.read()

    # Build YAML tags block
    lines = ["tags:"]
    for tag in tags:
        lines.append(f"  - description: {tag.get('description', '')}")
        lines.append(f"    name: {tag['name']}")
    tags_block = "\n".join(lines)

    # Insert after basePath line
    if "basePath:" in content:
        content = content.replace("basePath: /", f"basePath: /\n{tags_block}", 1)
    elif "paths:" in content:
        content = content.replace("paths:", f"{tags_block}\npaths:", 1)

    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def main() -> None:
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <main.go> <swagger.json> <swagger.yaml>")
        sys.exit(1)

    go_file, json_file, yaml_file = sys.argv[1], sys.argv[2], sys.argv[3]

    # Validate all paths resolve within the project directory.
    project_root = os.path.realpath(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    for path in (go_file, json_file, yaml_file):
        resolved = os.path.realpath(path)
        if not resolved.startswith(project_root):
            print(
                f"Error: path '{path}' resolves outside project directory ({project_root})"
            )
            sys.exit(1)

    tags = parse_tags_from_go(go_file)
    if not tags:
        print("Warning: no @tag.name annotations found in", go_file)
        return

    inject_json(json_file, tags)
    inject_yaml(yaml_file, tags)
    print(f"Injected {len(tags)} tag definitions into swagger spec")


if __name__ == "__main__":
    main()
