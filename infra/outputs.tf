output "fila_comandos_url" { value = aws_sqs_queue.comandos.url }
output "fila_comandos_dlq_url" { value = aws_sqs_queue.comandos_dlq.url }
output "tabela" { value = aws_dynamodb_table.tarefas.name }
output "pod_role_arn" { value = aws_iam_role.pod.arn }
output "alertas_topic_arn" { value = aws_sns_topic.alertas.arn }
