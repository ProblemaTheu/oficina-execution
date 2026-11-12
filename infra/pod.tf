# Identidade AWS do pod do Execution por EKS Pod Identity (ADR-016): o addon
# eks-pod-identity-agent já existe no cluster (oficina-infra-k8s).
#
# Menor privilégio: só a PRÓPRIA tabela (e seus índices), consumir a PRÓPRIA
# fila e enviar só para a fila de respostas do orquestrador. Nada de RDS,
# nada das filas do Billing — `aws iam simulate-principal-policy` sobre esta
# role é a evidência de "nenhum serviço acessa o banco de outro".

resource "aws_iam_role" "pod" {
  name = "${local.nome}-pod"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "pods.eks.amazonaws.com" }
      Action    = ["sts:AssumeRole", "sts:TagSession"]
      Condition = {
        StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.atual.account_id }
      }
    }]
  })
}

resource "aws_iam_role_policy" "pod" {
  name = "saga-execution"
  role = aws_iam_role.pod.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # TransactWriteItems não é ação IAM: cada item da transação é
        # autorizado pela ação correspondente (PutItem, UpdateItem,
        # ConditionCheckItem).
        Sid      = "TabelaPropria"
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:Query", "dynamodb:ConditionCheckItem"]
        Resource = [aws_dynamodb_table.tarefas.arn, "${aws_dynamodb_table.tarefas.arn}/index/*"]
      },
      {
        Sid      = "ConsumirComandos"
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"]
        Resource = aws_sqs_queue.comandos.arn
      },
      {
        Sid      = "ResponderAoOrquestrador"
        Effect   = "Allow"
        Action   = ["sqs:SendMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"]
        Resource = local.fila_respostas_arn
      }
    ]
  })
}

resource "aws_eks_pod_identity_association" "execution" {
  cluster_name    = data.aws_ssm_parameter.cluster_name.value
  namespace       = var.namespace
  service_account = var.service_account
  role_arn        = aws_iam_role.pod.arn
}
