# Alarme da DLQ (F4-7.4): mensagem que falhou 5× é falha de saga que ninguém
# vai ver no APM. Métrica da AWS, então fica no CloudWatch (+ SNS).

# Tópico cifrado com chave KMS PRÓPRIA. A chave gerenciada aws/sns não
# serve: a key policy dela não pode liberar cloudwatch.amazonaws.com, e o
# alarme falharia em silêncio ao publicar. US$ 1/mês enquanto o ambiente
# existir.
data "aws_iam_policy_document" "alertas_kms" {
  statement {
    sid       = "AdministracaoPelaConta"
    actions   = ["kms:*"]
    resources = ["*"] # numa key policy, "*" é a própria chave
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.atual.account_id}:root"]
    }
  }
  statement {
    sid       = "CloudWatchPublicaNoTopico"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey*"]
    resources = ["*"]
    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.atual.account_id]
    }
  }
}

resource "aws_kms_key" "alertas" {
  description             = "SNS de alertas do ${local.nome}"
  enable_key_rotation     = true
  deletion_window_in_days = 7
  policy                  = data.aws_iam_policy_document.alertas_kms.json
}

resource "aws_sns_topic" "alertas" {
  name              = "${local.nome}-alertas"
  kms_master_key_id = aws_kms_key.alertas.arn
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alerta_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alertas.arn
  protocol  = "email"
  endpoint  = var.alerta_email
}

resource "aws_cloudwatch_metric_alarm" "dlq" {
  alarm_name          = "${local.nome}-dlq-com-mensagens"
  alarm_description   = "Mensagem na DLQ do Execution: comando da saga falhou 5 vezes (ver payload na fila ${aws_sqs_queue.comandos_dlq.name})"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.comandos_dlq.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alertas.arn]
  ok_actions          = [aws_sns_topic.alertas.arn]
}
