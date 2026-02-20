#!/usr/bin/env bash
set -uo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: $(basename "$0") <output_format> [search_root]"
  echo "Example: $(basename "$0") svg ."
  exit 1
fi

output_format="${1#.}"
search_root="${2:-.}"

if [[ ! -d "$search_root" ]]; then
  echo "Error: search_root does not exist: \"$search_root\"" >&2
  exit 1
fi

if ! command -v mmdc >/dev/null 2>&1; then
  echo "Error: mmdc not found in PATH. Install Mermaid CLI first." >&2
  exit 1
fi

found=0
failed=0

while IFS= read -r -d '' input_file; do
  found=$((found + 1))
  output_file="${input_file%.*}.${output_format}"

  echo "Rendering \"$input_file\" -> \"$output_file\""
  if ! mmdc -i "$input_file" -o "$output_file"; then
    echo "Error: failed to render \"$input_file\"" >&2
    failed=$((failed + 1))
  fi
done < <(find "$search_root" -type f -name '*.mmd' -print0)

if [[ "$found" -eq 0 ]]; then
  echo "No .mmd files found under \"$search_root\"."
  exit 0
fi

if [[ "$failed" -ne 0 ]]; then
  echo "Completed with $failed failure(s) out of $found file(s)." >&2
  exit 2
fi

echo "Completed successfully: $found file(s) rendered to .$output_format."
exit 0
