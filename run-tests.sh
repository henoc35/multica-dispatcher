#!/usr/bin/env bash
# Lance tous les tests (faux serveur Multica local, aucun appel réel).
set -euo pipefail
cd "$(dirname "$0")"
python3 -c "import websockets" 2>/dev/null || pip install -q -r requirements.txt
python3 tests/test_guards.py
python3 tests/test_e2e.py | grep -E '^(PASS|FAIL)'
