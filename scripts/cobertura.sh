#!/usr/bin/env bash
# Gate de cobertura unitária (F4-5.1). Mesmas exclusões do
# sonar.coverage.exclusions em sonar-project.properties — mantenha as duas
# listas iguais:
#   - cmd/: bootstrap (lê ambiente, conecta AWS, sobe os laços);
#   - internal/infra/dynamo/: adaptador testado contra DynamoDB real
#     (LocalStack, job "Integração (LocalStack)").
set -euo pipefail
MINIMO="${MINIMO:-80}"
EXCLUSOES='(/cmd/|/internal/infra/dynamo/)'

go test -race -count=1 -covermode=atomic -coverprofile=coverage.out ./...
{ head -1 coverage.out; tail -n +2 coverage.out | grep -vE "$EXCLUSOES"; } > coverage.unit.out
total=$(go tool cover -func=coverage.unit.out | awk '/^total:/ { sub("%", "", $3); print $3 }')
echo "Cobertura unitária (critério do Sonar): ${total}%"
echo "cobertura=${total}" >> "${GITHUB_OUTPUT:-/dev/null}"
awk -v t="$total" -v m="$MINIMO" 'BEGIN { exit !(t >= m) }' \
  || { echo "::error::Cobertura ${total}% abaixo do mínimo de ${MINIMO}%"; exit 1; }
