#!/usr/bin/env bash
set -uo pipefail

if [[ $# -gt 1 ]]; then
  echo "Usage: $(basename "$0") [search_root]"
  echo "Example: $(basename "$0") ."
  exit 1
fi

search_root="${1:-.}"

if [[ ! -d "$search_root" ]]; then
  echo "Error: search_root does not exist: \"$search_root\"" >&2
  exit 1
fi

if ! command -v pandoc >/dev/null 2>&1; then
  echo "Error: pandoc not found in PATH. Install pandoc first." >&2
  exit 1
fi

is_marp_file() {
  local file_path="$1"
  head -n 20 "$file_path" | grep -Eq '^marp:[[:space:]]*true'
}

rewrite_svg_references_for_pdf() {
  perl -pe '
    # Local SVG assets: prefer sibling PDF exports to avoid svg.sty/rsvg-convert dependency.
    s/\(((?!https?:\/\/)[^)]+)\.svg\)/($1.pdf)/g;
    # Remote SVG assets (e.g. shields): try PNG variant first.
    s/\((https?:\/\/[^)]+)\.svg\)/($1.png)/g;
  '
}

found=0
failed=0

while IFS= read -r -d '' input_file; do
  found=$((found + 1))
  output_file="${input_file%.md}.pdf"
  input_dir="$(dirname "$input_file")"

  echo "Rendering \"$input_file\" -> \"$output_file\""

  if is_marp_file "$input_file"; then
    if ! sed -E 's/!\[width:([0-9]+)\]\(([^)]+)\)/![](\2){ width=\1px }/g' "$input_file" \
      | rewrite_svg_references_for_pdf \
      | pandoc --resource-path="$input_dir:." -f markdown -o "$output_file"; then
      echo "Error: failed to render \"$input_file\"" >&2
      failed=$((failed + 1))
    fi
    continue
  fi

  if ! rewrite_svg_references_for_pdf < "$input_file" \
    | pandoc --resource-path="$input_dir:." -f markdown -o "$output_file"; then
    echo "Error: failed to render \"$input_file\"" >&2
    failed=$((failed + 1))
  fi
done < <(
  find "$search_root" \
    -type d \( -name '.git' -o -name 'node_modules' -o -name '.venv' -o -name 'venv' \) -prune -o \
    -type f -name '*.md' -print0
)

if [[ "$found" -eq 0 ]]; then
  echo "No .md files found under \"$search_root\"."
  exit 0
fi

if [[ "$failed" -ne 0 ]]; then
  echo "Completed with $failed failure(s) out of $found file(s)." >&2
  exit 2
fi

echo "Completed successfully: $found file(s) rendered to .pdf."
exit 0
