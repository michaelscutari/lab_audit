#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

# Config
TARGET_DIR="/hpc/group/singhlab/tools"
OUTPUT_DIR="/hpc/group/singhlab/reports"
GDU_BIN="/hpc/group/singhlab/tools/bin/gdu"
PYTHON_BIN="python3"
CONVERT_SCRIPT="${SCRIPT_DIR}/convert_gdu.py"

if [[ "${TARGET_DIR}" != /* ]]; then
  echo "TARGET_DIR must be an absolute path: ${TARGET_DIR}" >&2
  exit 1
fi

if [[ "${GDU_BIN}" = /* ]]; then
  if [[ ! -x "${GDU_BIN}" ]]; then
    echo "GDU not found: ${GDU_BIN}" >&2
    exit 1
  fi
else
  if ! command -v "${GDU_BIN}" >/dev/null 2>&1; then
    echo "GDU not found in PATH: ${GDU_BIN}" >&2
    exit 1
  fi
fi

python_exec="${PYTHON_CMD[0]}"
if [[ "${python_exec}" = /* ]]; then
  if [[ ! -x "${python_exec}" ]]; then
    echo "Python runner not found: ${python_exec}" >&2
    exit 1
  fi
else
  if ! command -v "${python_exec}" >/dev/null 2>&1; then
    echo "Python runner not found in PATH: ${python_exec}" >&2
    exit 1
  fi
fi

if [[ ! -f "${CONVERT_SCRIPT}" ]]; then
  echo "Missing converter script: ${CONVERT_SCRIPT}" >&2
  exit 1
fi

mkdir -p "${OUTPUT_DIR}"
TIMESTAMP="$(date +"%Y%m%d_%H%M%S")"
JSON_OUT="${OUTPUT_DIR}/scan_${TIMESTAMP}.json"
PARQUET_OUT="${OUTPUT_DIR}/scan_${TIMESTAMP}.parquet"

echo "=== STARTING AUDIT: ${TIMESTAMP} ==="
echo "Target: ${TARGET_DIR}"

echo "[1/2] Scanning filesystem..."
"${GDU_BIN}" -n -c -o "${JSON_OUT}" "${TARGET_DIR}"

echo "[2/2] Converting to Parquet..."
"${PYTHON_CMD[@]}" "${CONVERT_SCRIPT}" --input "${JSON_OUT}" --output "${PARQUET_OUT}"

if [[ -f "${PARQUET_OUT}" ]]; then
  echo "Cleanup: Removing raw JSON..."
  rm -f "${JSON_OUT}"
  echo "SUCCESS: Report ready at ${PARQUET_OUT}"
else
  echo "ERROR: Parquet file was not created." >&2
  exit 1
fi

# Keep only the most recent scan.
old_scans="$(ls -1t "${OUTPUT_DIR}"/scan_*.parquet 2>/dev/null | tail -n +2 || true)"
if [[ -n "${old_scans}" ]]; then
  printf '%s\n' "${old_scans}" | xargs rm -f --
fi
