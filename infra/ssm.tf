# Contrato com os outros repositórios (e com o CD deste): parâmetros nomeados.
# O CD lê estes valores e gera o ConfigMap execution-ambiente; o oficina-app
# lê a URL da fila de comandos para enviar IniciarDiagnostico etc.

resource "aws_ssm_parameter" "fila_comandos_url" {
  name  = "/oficina/${var.ambiente}/execution/fila-comandos-url"
  type  = "String"
  value = aws_sqs_queue.comandos.url
}

resource "aws_ssm_parameter" "tabela" {
  name  = "/oficina/${var.ambiente}/execution/dynamodb-tabela"
  type  = "String"
  value = aws_dynamodb_table.tarefas.name
}
