#!/usr/bin/env bash
set -euo pipefail

COMMAND="${1:-batch}"
shift || true

echo "Starting Energy Intelligence Pipeline"
echo "Command: ${COMMAND}"
echo "RAW_DIR: ${RAW_DIR:-data/raw/dispatch_inbox}"

# Ensure project root is on Python import path
export PYTHONPATH="${PYTHONPATH:-/app}"

INGEST_ARGS=()
PROCESS_ARGS=()

# Defaults from env (can be overridden by CLI flags)
DEFAULT_REPORT="${AEMO_REPORT:-DispatchIS_Reports}"
DEFAULT_INGEST_LIMIT="${INGEST_LIMIT:-50}"
DEFAULT_PROCESS_MAX_FILES="${PROCESS_MAX_FILES:-50}"

HAS_LIMIT=0
HAS_MAX_FILES=0

add_kv_arg() {
  local arr_name="$1"
  local key="$2"
  local val="${3:-}"
  if [[ -z "$val" ]]; then
    echo "Missing value for ${key}"
    exit 2
  fi
  eval "${arr_name}+=(\"${key}\" \"${val}\")"
}

add_flag_arg() {
  local arr_name="$1"
  local key="$2"
  eval "${arr_name}+=(\"${key}\")"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    # Ingestion args
    --limit)
      HAS_LIMIT=1
      key="$1"; shift
      add_kv_arg INGEST_ARGS "$key" "${1:-}"
      ;;
    --since|--backfill-days|--report)
      key="$1"; shift
      add_kv_arg INGEST_ARGS "$key" "${1:-}"
      ;;
    --dry-run|--no-update-watermark)
      add_flag_arg INGEST_ARGS "$1"
      ;;

    # Processing args
    --max-files)
      HAS_MAX_FILES=1
      key="$1"; shift
      add_kv_arg PROCESS_ARGS "$key" "${1:-}"
      ;;
    --cleanup)
      add_flag_arg PROCESS_ARGS "$1"
      ;;

    *)
      echo "Unknown argument: $1"
      exit 2
      ;;
  esac
  shift
done

# Apply defaults only if not explicitly passed
if [[ $HAS_LIMIT -eq 0 ]]; then
  INGEST_ARGS+=(--limit "$DEFAULT_INGEST_LIMIT")
fi

if [[ $HAS_MAX_FILES -eq 0 ]]; then
  PROCESS_ARGS+=(--max-files "$DEFAULT_PROCESS_MAX_FILES")
fi

case "$COMMAND" in
  migrate)
    echo "Running DB migrations"
    python -m scripts.migrations.migrate
    ;;

  ingest)
    echo "Running ingestion only"
    python -m scripts.ingestion.ingest_dispatch --report "${DEFAULT_REPORT}" "${INGEST_ARGS[@]}"
    ;;

  batch)
    echo "Running processing only (batch processor)"
    python -m scripts.processing.process_dispatch_price_batch "${PROCESS_ARGS[@]}"
    ;;

  process_dispatch_price)
    echo "Processing dispatch price into DB (incremental batch, metadata tables)"
    python -m scripts.processing.process_dispatch_price_batch "${PROCESS_ARGS[@]}"
    ;;


  ingest_process)
    echo "Ingest -> Process (with cleanup)"
    python -m scripts.ingestion.ingest_dispatch --report "${DEFAULT_REPORT}" "${INGEST_ARGS[@]}"

    if [[ " ${PROCESS_ARGS[*]} " != *" --cleanup "* ]]; then
      PROCESS_ARGS+=(--cleanup)
    fi

    python -m scripts.processing.process_dispatch_price "${PROCESS_ARGS[@]}"
    ;;

  parquet)
    echo "Converting clean dataset to parquet"
    python -m scripts.processing.convert_dispatch_price_clean_to_parquet
    ;;

  rollups)
    echo "Building daily region rollups"
    python -m scripts.processing.build_daily_region_rollups
    ;;

  all)
    echo "Full pipeline: migrate -> ingest -> process -> parquet -> rollups"
    python -m scripts.migrations.migrate
    python -m scripts.ingestion.ingest_dispatch --report "${DEFAULT_REPORT}" "${INGEST_ARGS[@]}"

    if [[ " ${PROCESS_ARGS[*]} " != *" --cleanup "* ]]; then
      PROCESS_ARGS+=(--cleanup)
    fi

    python -m scripts.processing.process_dispatch_price "${PROCESS_ARGS[@]}"
    python -m scripts.processing.convert_dispatch_price_clean_to_parquet
    python -m scripts.processing.build_daily_region_rollups
    ;;

  shell|bash)
    exec bash
    ;;

  *)
    echo "Unknown command: $COMMAND"
    exit 1
    ;;
esac
