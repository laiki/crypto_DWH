#!/usr/bin/env bash
set -uo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: $(basename "$0") <output_format> [search_root]"
  echo "Example: $(basename "$0") svg ."
  exit 1
fi

output_format="${1#.}"
search_root="${2:-.}"
kroki_base_url="${KROKI_BASE_URL:-https://kroki.io}"

if [[ ! -d "$search_root" ]]; then
  echo "Error: search_root does not exist: \"$search_root\"" >&2
  exit 1
fi

render_with_kroki() {
  local input_file="$1"
  local output_file="$2"

  if ! command -v curl >/dev/null 2>&1; then
    echo "Error: curl not found in PATH. Cannot use Kroki fallback." >&2
    return 1
  fi

  case "$output_format" in
    svg|png|pdf)
      curl -fsSL -X POST \
        -H 'Content-Type: text/plain' \
        --data-binary @"$input_file" \
        "$kroki_base_url/mermaid/$output_format" \
        -o "$output_file"
      ;;
    *)
      echo "Error: Kroki fallback supports only svg/png/pdf, got \"$output_format\"." >&2
      return 1
      ;;
  esac
}

has_mmdc=0
mmdc_disabled=0
if command -v mmdc >/dev/null 2>&1; then
  has_mmdc=1
fi

pup_cfg_file=""
if [[ "$has_mmdc" -eq 1 ]]; then
  pup_cfg_file="$(mktemp)"
  trap '[[ -n "$pup_cfg_file" ]] && rm -f "$pup_cfg_file"' EXIT

  if [[ -n "${MERMAID_CHROME_PATH:-}" ]]; then
    cat > "$pup_cfg_file" <<EOF
{
  "executablePath": "${MERMAID_CHROME_PATH}",
  "headless": true,
  "args": [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-software-rasterizer",
    "--no-zygote",
    "--single-process"
  ]
}
EOF
  else
    cat > "$pup_cfg_file" <<'EOF'
{
  "headless": true,
  "args": [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-software-rasterizer",
    "--no-zygote",
    "--single-process"
  ]
}
EOF
  fi
fi

found=0
failed=0

while IFS= read -r -d '' input_file; do
  found=$((found + 1))
  output_file="${input_file%.*}.${output_format}"

  echo "Rendering \"$input_file\" -> \"$output_file\""

  rendered=0
  if [[ "$has_mmdc" -eq 1 && "$mmdc_disabled" -eq 0 ]]; then
    mmdc_log="$(mktemp)"
    if mmdc -q -p "$pup_cfg_file" -i "$input_file" -o "$output_file" >"$mmdc_log" 2>&1; then
      rendered=1
    else
      if rg -q "Failed to launch the browser process|error while loading shared libraries|No suitable browser found" "$mmdc_log"; then
        mmdc_disabled=1
        echo "Warning: mmdc browser runtime unavailable. Switching to Kroki fallback for remaining files." >&2
      else
        echo "Warning: mmdc failed for \"$input_file\". Trying Kroki fallback." >&2
      fi
      sed -n '1,60p' "$mmdc_log" >&2
    fi
    rm -f "$mmdc_log"
  fi

  if [[ "$rendered" -eq 0 ]]; then
    if render_with_kroki "$input_file" "$output_file"; then
      rendered=1
    fi
  fi

  if [[ "$rendered" -eq 0 ]]; then
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
