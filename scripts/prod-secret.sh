#!/usr/bin/env bash
# Cria/atualiza o Secret execution-secrets no namespace execution-prod a
# partir do AWS Secrets Manager. Rodar uma vez antes do primeiro deploy.
#
#   ./scripts/prod-secret.sh
#
# Fontes:
#   oficina/prod/app        — jwt_secret COMPARTILHADO com o OS e a Lambda
#                             (criado pelo scripts/prod-secret.sh do oficina-app)
#   NEW_RELIC_LICENSE_KEY   — opcional, do ambiente
#
# O Execution não tem senha de banco: o DynamoDB é autorizado pela role do
# pod (Pod Identity). Nada passa por argumento de linha de comando (aparece
# em `ps`) e nada é escrito em disco.
set -euo pipefail

REGIAO=${REGIAO:-us-east-1}
NS=${NS:-execution-prod}

APP_JSON=$(aws secretsmanager get-secret-value --secret-id oficina/prod/app --region "$REGIAO" \
  --query SecretString --output text 2>/dev/null) \
  || { echo "segredo oficina/prod/app não encontrado — rode o scripts/prod-secret.sh do oficina-app antes"; exit 1; }

JWT_SECRET=$(printf '%s' "$APP_JSON" | python3 -c 'import sys,json;print(json.load(sys.stdin)["jwt_secret"])')
export JWT_SECRET

python3 - <<'PY' | kubectl apply -n "$NS" -f -
import json, os
dados = {"JWT_SECRET": os.environ["JWT_SECRET"]}
if os.environ.get("NEW_RELIC_LICENSE_KEY"):
    dados["NEW_RELIC_LICENSE_KEY"] = os.environ["NEW_RELIC_LICENSE_KEY"]
print(json.dumps({
    "apiVersion": "v1", "kind": "Secret", "type": "Opaque",
    "metadata": {"name": "execution-secrets",
                 "labels": {"app.kubernetes.io/name": "execution", "app.kubernetes.io/part-of": "oficina"}},
    "stringData": dados,
}))
PY
echo "==> execution-secrets aplicado em $NS"
