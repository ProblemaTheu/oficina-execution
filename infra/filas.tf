# Fila de ENTRADA do Execution (F4-4.1). Cada serviço é dono da fila que
# consome; quem envia recebe permissão na própria role (identity policy).
# Mesmos atributos do scripts/localstack-init.sh do oficina-app.

resource "aws_sqs_queue" "comandos_dlq" {
  name                      = "execution-commands-dlq.fifo"
  fifo_queue                = true
  message_retention_seconds = 1209600 # 14 dias para investigar
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "comandos" {
  name                       = "execution-commands.fifo"
  fifo_queue                 = true
  deduplication_scope        = "messageGroup"
  fifo_throughput_limit      = "perMessageGroupId"
  visibility_timeout_seconds = 60 # > tempo máximo de processamento
  receive_wait_time_seconds  = 20 # long polling
  message_retention_seconds  = 345600
  sqs_managed_sse_enabled    = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.comandos_dlq.arn
    maxReceiveCount     = 5
  })
}

# Só esta fila pode usar a DLQ como destino.
resource "aws_sqs_queue_redrive_allow_policy" "comandos_dlq" {
  queue_url = aws_sqs_queue.comandos_dlq.id
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.comandos.arn]
  })
}
