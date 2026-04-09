#!/bin/bash
set -e

STATIC_DIR="/home/care/acoharmony/.venv/lib/python3.13/site-packages/marimo/_static"
CUSTOM_DIR="/opt/marimo-custom"

# Inject custom homepage script inline into marimo's index.html
if [ -f "$CUSTOM_DIR/homepage.js" ] && [ -f "$STATIC_DIR/index.html" ]; then
    echo "Injecting ACOharmony homepage customizations..."
    # Read the JS file and inject it as an inline <script> before </body>
    SCRIPT_CONTENT=$(cat "$CUSTOM_DIR/homepage.js")
    # Use python for reliable multi-line insertion (sed struggles with large multi-line content)
    python3 -c "
import sys
html = open('$STATIC_DIR/index.html').read()
js = open('$CUSTOM_DIR/homepage.js').read()
tag = '<script data-marimo=\"true\">' + js + '</script>'
html = html.replace('</body>', tag + '\n</body>')
open('$STATIC_DIR/index.html', 'w').write(html)
"
    echo "Custom homepage injected."
fi

# Replace custom logo if provided
if [ -f "$CUSTOM_DIR/logo.png" ]; then
    cp "$CUSTOM_DIR/logo.png" "$STATIC_DIR/logo.png"
    echo "Custom logo applied."
fi

exec uv run --project /home/care/acoharmony marimo edit . \
    --watch --host 0.0.0.0 --port 10012 --headless --no-token
