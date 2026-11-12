# O cluster vem do oficina-infra-k8s pela SSM — nenhum state lê o state de
# outro repositório. O Execution não tem recurso de rede próprio: o DynamoDB
# é acessado pela API da AWS, autorizado pela role do pod.

data "aws_caller_identity" "atual" {}

data "aws_ssm_parameter" "cluster_name" {
  name = "/oficina/shared/eks/cluster_name"
}

locals {
  nome = "oficina-execution"

  # Fila de respostas do orquestrador: é do oficina-app. ARN por convenção de
  # nome para não criar dependência circular entre os states (arquitetura §7).
  fila_respostas_arn = "arn:aws:sqs:${var.regiao}:${data.aws_caller_identity.atual.account_id}:os-saga-replies.fifo"
}
