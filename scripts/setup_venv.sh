#!/usr/bin/env bash
set -euo pipefail

PY=${PYTHON:-python3}
VENV_DIR=${VENV_DIR:-.venv}

if [ ! -d "$VENV_DIR" ]; then
  echo "Creating venv in $VENV_DIR"
  "$PY" -m venv "$VENV_DIR"
fi

echo "Activating venv and installing requirements"
source "$VENV_DIR/bin/activate"
pip install --upgrade pip
pip install -r requirements.txt

echo "Done. Activate with: source $VENV_DIR/bin/activate"
