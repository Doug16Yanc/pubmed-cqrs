#!/usr/bin/env bash
#
# Baixa arquivos do PubMed com verificação de integridade (checksum MD5) e retomada
# em caso de falha. Idempotente: pula arquivos já baixados e íntegros.
#
# Uso:
#   ./download-baseline.sh <diretório-destino> <número1> [número2] [número3] ...
#
# Exemplo (baixa os 2 primeiros arquivos do baseline — artigos mais antigos):
#   ./download-baseline.sh ./pubmed-data 1 2
#
# Exemplo (artigos recentes — update files, mesmo formato XML, incremental diário):
#   SOURCE=updates ./download-baseline.sh ./pubmed-data 1335 1336
#
# Variáveis de ambiente opcionais:
#   YEAR_PREFIX (padrão: 26, referente ao baseline 2026 -> arquivos pubmed26nXXXX.xml.gz)
#   SOURCE      (padrão: baseline. Use "updates" pra pegar update files — artigos
#                novos/revisados publicados depois do corte do baseline anual. O
#                primeiro update file de 2026 é o pubmed26n1335; a NLM anuncia o
#                número mais recente em https://www.nlm.nih.gov/pubs/techbull —
#                procure por "PubMed Update" ou confira o índice em
#                https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/)

set -euo pipefail

YEAR_PREFIX="${YEAR_PREFIX:-26}"
SOURCE="${SOURCE:-baseline}"

case "$SOURCE" in
  baseline) BASE_URL="https://ftp.ncbi.nlm.nih.gov/pubmed/baseline" ;;
  updates)  BASE_URL="https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles" ;;
  *) echo "SOURCE inválido: '$SOURCE' (use 'baseline' ou 'updates')" >&2; exit 1 ;;
esac

if [ $# -lt 2 ]; then
  echo "Uso: $0 <diretório-destino> <número1> [número2] ..." >&2
  echo "Ex:  $0 ./pubmed-data 1 2" >&2
  echo "Ex (recentes): SOURCE=updates $0 ./pubmed-data 1335 1336" >&2
  exit 1
fi

DEST_DIR="$1"
shift
FILE_NUMBERS=("$@")

mkdir -p "$DEST_DIR"

verify_checksum() {
  local xml_path="$1" md5_path="$2"
  local expected actual
  expected=$(awk '{print $NF}' "$md5_path" | tr -d '\r\n')
  actual=$(md5sum "$xml_path" | awk '{print $1}')
  [ -n "$expected" ] && [ "$expected" = "$actual" ]
}

for n in "${FILE_NUMBERS[@]}"; do
  padded=$(printf "%04d" "$n")
  fname="pubmed${YEAR_PREFIX}n${padded}.xml.gz"
  md5name="${fname}.md5"
  xml_path="$DEST_DIR/$fname"
  md5_path="$DEST_DIR/$md5name"

  echo "== ${fname} =="

  # Checksum é pequeno, baixa sempre de novo para garantir que está atualizado
  curl -fsSL --retry 5 --retry-delay 3 -o "$md5_path" "$BASE_URL/$md5name"

  if [ -f "$xml_path" ] && verify_checksum "$xml_path" "$md5_path"; then
    echo "  já baixado e íntegro, pulando."
    continue
  fi

  echo "  baixando..."
  curl -fSL --retry 5 --retry-delay 3 -C - -o "$xml_path" "$BASE_URL/$fname"

  if verify_checksum "$xml_path" "$md5_path"; then
    echo "  OK — checksum verificado."
  else
    echo "  ERRO: checksum inválido para $fname, removendo arquivo corrompido." >&2
    rm -f "$xml_path"
    exit 1
  fi
done

echo
echo "Download concluído em: $DEST_DIR"
echo "Arquivos:"
ls -lh "$DEST_DIR"/*.xml.gz 2>/dev/null || true