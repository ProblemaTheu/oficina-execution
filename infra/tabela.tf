# Banco PRÓPRIO do Execution — o requisito de banco NoSQL do enunciado.
# Single-table (internal/infra/dynamo/repositorio.go):
#   Tarefa    pk=TAREFA#<id>  sk=TAREFA       gsi1: FILA#<tipo>#<estado> / <prioridade>#<criado_em>#<id>
#   Ponteiro  pk=OS#<os_id>   sk=TAREFA#<id>
#   Inbox     pk=MSG#<msg_id> sk=MSG          ttl de 7 dias
#   Outbox    pk=OUTBOX#<id>  sk=OUTBOX       gsi1 esparso: OUTBOX#PENDENTE / ...
# Mesmo esquema do scripts/localstack-init.sh do oficina-app.

resource "aws_dynamodb_table" "tarefas" {
  name         = "execution-tarefas"
  billing_mode = "PAY_PER_REQUEST" # sem capacidade ociosa: custo ≈ 0 na demo
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }
  attribute {
    name = "sk"
    type = "S"
  }
  attribute {
    name = "gsi1pk"
    type = "S"
  }
  attribute {
    name = "gsi1sk"
    type = "S"
  }

  # key_schema no lugar de hash_key/range_key (depreciados no provider 6.x).
  global_secondary_index {
    name            = "gsi1"
    projection_type = "ALL"
    key_schema {
      attribute_name = "gsi1pk"
      key_type       = "HASH"
    }
    key_schema {
      attribute_name = "gsi1sk"
      key_type       = "RANGE"
    }
  }

  # Inbox de idempotência expira sozinho (7 dias).
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }

  deletion_protection_enabled = false # ambiente de desafio, destruído após a entrega
}
