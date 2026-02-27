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

if ! command -v python >/dev/null 2>&1; then
  echo "Error: python not found in PATH. Python is required for SVG->PDF conversion." >&2
  exit 1
fi

if ! python - <<'PY' >/dev/null 2>&1
import importlib.util
raise SystemExit(0 if importlib.util.find_spec("cairosvg") else 1)
PY
then
  echo "Error: Python package 'cairosvg' is required for high-quality local SVG embedding." >&2
  echo "Install with: pip install cairosvg" >&2
  exit 1
fi

is_marp_file() {
  local file_path="$1"
  head -n 20 "$file_path" | grep -Eq '^marp:[[:space:]]*true'
}

ensure_local_svg_pdf_artifacts() {
  local file_path="$1"
  local input_dir="$2"
  local tmp_refs
  tmp_refs="$(mktemp)"

  python - "$file_path" >"$tmp_refs" <<'PY'
import re
import sys

path = sys.argv[1]
text = open(path, "r", encoding="utf-8").read()
seen = set()
for match in re.finditer(r"\((?!https?://)([^)]+\.svg)\)", text):
    rel = match.group(1).strip()
    if rel and rel not in seen:
        seen.add(rel)
        print(rel)
PY

  while IFS= read -r rel_svg_path; do
    [[ -z "$rel_svg_path" ]] && continue
    src_svg="$(realpath -m "$input_dir/$rel_svg_path")"
    dst_pdf="${src_svg%.svg}.pdf"

    if [[ ! -f "$src_svg" ]]; then
      continue
    fi

    if [[ ! -f "$dst_pdf" || "$src_svg" -nt "$dst_pdf" ]]; then
      python - "$src_svg" "$dst_pdf" <<'PY'
import sys
import cairosvg

src, dst = sys.argv[1], sys.argv[2]
cairosvg.svg2pdf(url=src, write_to=dst)
PY
    fi
  done < "$tmp_refs"

  rm -f "$tmp_refs"
}

rewrite_svg_references_for_pdf() {
  perl -pe '
    # Local SVG assets: use sibling PDF exports generated via cairosvg.
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
  ensure_local_svg_pdf_artifacts "$input_file" "$input_dir"

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
