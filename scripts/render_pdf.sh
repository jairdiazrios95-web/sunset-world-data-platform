#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python scripts/pdf_preprocess.py docs/RESPUESTAS_EXAMEN.md .build/RESPUESTAS_EXAMEN_pdf.md
pandoc .build/RESPUESTAS_EXAMEN_pdf.md -o deliverables/Sunset_World_Respuestas_Examen.pdf \
  --pdf-engine=xelatex -V mainfont='DejaVu Sans' -V monofont='DejaVu Sans Mono' \
  -V geometry:margin=20mm -V fontsize=11pt -V colorlinks=true --highlight-style=tango
