#!/usr/bin/env bash
# Arranca el monitor EcoFlow → Telegram.
# Uso: ./start.sh           (loop continuo cada INTERVAL_HOURS)
#      ./start.sh --once    (una sola ejecución, para cron)
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$DIR/ecoflow.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "No existe $ENV_FILE" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

for var in TELEGRAM_BOT_TOKEN TELEGRAM_CHAT_ID ECOFLOW_ACCESS_KEY ECOFLOW_SECRET_KEY ECOFLOW_SN_DELTA2; do
  if [[ -z "${!var:-}" ]]; then
    echo "Falta completar $var en ecoflow.env" >&2
    exit 1
  fi
done

if ! python3 -c "import requests" 2>/dev/null; then
  pip3 install --quiet requests
fi

exec python3 "$DIR/ecoflow_telegram_monitor.py" "$@"
