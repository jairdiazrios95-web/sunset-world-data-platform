#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
pandoc docs/RESPUESTAS_EXAMEN.md -o deliverables/Sunset_World_Respuestas_Examen.pdf \
  --pdf-engine=xelatex -V mainfont='DejaVu Sans' -V monofont='DejaVu Sans Mono' \
  -V geometry:margin=20mm -V fontsize=10pt -V colorlinks=true --highlight-style=tango
