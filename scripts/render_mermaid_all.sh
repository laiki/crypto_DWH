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

text_has_pattern() {
  local pattern="$1"
  local file_path="$2"
  if command -v rg >/dev/null 2>&1; then
    rg -q "$pattern" "$file_path"
    return $?
  fi
  grep -Eq "$pattern" "$file_path"
}

convert_svg_to_pdf() {
  local input_svg="$1"
  local output_pdf="$2"

  if command -v cairosvg >/dev/null 2>&1; then
    cairosvg "$input_svg" -o "$output_pdf"
    return $?
  fi

  if command -v rsvg-convert >/dev/null 2>&1; then
    rsvg-convert -f pdf -o "$output_pdf" "$input_svg"
    return $?
  fi

  if command -v inkscape >/dev/null 2>&1; then
    inkscape "$input_svg" --export-type=pdf --export-filename="$output_pdf" >/dev/null 2>&1
    return $?
  fi

  if command -v magick >/dev/null 2>&1; then
    magick -density 300 "$input_svg" "$output_pdf"
    return $?
  fi

  if command -v convert >/dev/null 2>&1; then
    convert -density 300 "$input_svg" "$output_pdf"
    return $?
  fi

  echo "Error: No SVG->PDF converter found (tried cairosvg, rsvg-convert, inkscape, magick, convert)." >&2
  return 1
}

convert_png_to_pdf() {
  local input_png="$1"
  local output_pdf="$2"

  if command -v magick >/dev/null 2>&1; then
    magick "$input_png" "$output_pdf"
    return $?
  fi

  if command -v convert >/dev/null 2>&1; then
    convert "$input_png" "$output_pdf"
    return $?
  fi

  if command -v python >/dev/null 2>&1; then
    if python - "$input_png" "$output_pdf" <<'PY'
import sys
try:
    from PIL import Image
except Exception:
    raise SystemExit(2)

src, dst = sys.argv[1], sys.argv[2]
img = Image.open(src).convert("RGB")
img.save(dst, "PDF", resolution=300.0)
PY
    then
      return 0
    fi
  fi

  echo "Error: No PNG->PDF converter found (tried magick, convert, python+Pillow)." >&2
  return 1
}

render_with_kroki() {
  local input_file="$1"
  local output_file="$2"
  local tmp_response
  local tmp_svg
  local tmp_png
  local http_code

  if ! command -v curl >/dev/null 2>&1; then
    echo "Error: curl not found in PATH. Cannot use Kroki fallback." >&2
    return 1
  fi

  tmp_response="$(mktemp)"
  case "$output_format" in
    svg|png)
      http_code="$(
        curl -sS -X POST \
        -H 'Content-Type: text/plain' \
        --data-binary @"$input_file" \
        "$kroki_base_url/mermaid/$output_format" \
        -o "$tmp_response" \
        -w "%{http_code}"
      )"
      if [[ "$http_code" -lt 200 || "$http_code" -ge 300 ]]; then
        echo "Error: Kroki returned HTTP $http_code for \"$input_file\"." >&2
        sed -n '1,40p' "$tmp_response" >&2
        rm -f "$tmp_response"
        return 1
      fi
      mv "$tmp_response" "$output_file"
      ;;
    pdf)
      http_code="$(
        curl -sS -X POST \
        -H 'Content-Type: text/plain' \
        --data-binary @"$input_file" \
        "$kroki_base_url/mermaid/svg" \
        -o "$tmp_response" \
        -w "%{http_code}"
      )"
      if [[ "$http_code" -lt 200 || "$http_code" -ge 300 ]]; then
        echo "Error: Kroki returned HTTP $http_code for \"$input_file\"." >&2
        sed -n '1,40p' "$tmp_response" >&2
        rm -f "$tmp_response"
        return 1
      fi

      tmp_svg="$(mktemp)"
      mv "$tmp_response" "$tmp_svg"

      # Mermaid often emits labels via foreignObject; cairosvg drops those in PDF.
      # If detected, render PNG and convert PNG->PDF to preserve visible text.
      if text_has_pattern "<foreignObject" "$tmp_svg"; then
        echo "Info: Detected foreignObject labels in \"$input_file\". Using PNG-based PDF fallback to preserve text." >&2
        tmp_png="$(mktemp)"
        http_code="$(
          curl -sS -X POST \
          -H 'Content-Type: text/plain' \
          --data-binary @"$input_file" \
          "$kroki_base_url/mermaid/png" \
          -o "$tmp_png" \
          -w "%{http_code}"
        )"
        if [[ "$http_code" -lt 200 || "$http_code" -ge 300 ]]; then
          echo "Error: Kroki returned HTTP $http_code for PNG fallback of \"$input_file\"." >&2
          sed -n '1,40p' "$tmp_png" >&2
          rm -f "$tmp_svg" "$tmp_png"
          return 1
        fi
        if ! convert_png_to_pdf "$tmp_png" "$output_file"; then
          rm -f "$tmp_svg" "$tmp_png"
          return 1
        fi
        rm -f "$tmp_svg" "$tmp_png"
      else
        if ! convert_svg_to_pdf "$tmp_svg" "$output_file"; then
          rm -f "$tmp_svg"
          return 1
        fi
        rm -f "$tmp_svg"
      fi
      ;;
    *)
      rm -f "$tmp_response"
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
      if text_has_pattern "Failed to launch the browser process|error while loading shared libraries|No suitable browser found" "$mmdc_log"; then
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
