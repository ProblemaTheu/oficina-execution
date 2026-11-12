variable "regiao" {
  description = "Região AWS"
  type        = string
  default     = "us-east-1"
}

variable "ambiente" {
  description = "Ambiente único (corte 10 da Fase 3)"
  type        = string
  default     = "prod"
}

variable "namespace" {
  description = "Namespace do serviço no EKS (criado pelo oficina-infra-k8s)"
  type        = string
  default     = "execution-prod"
}

variable "service_account" {
  description = "ServiceAccount do Deployment (k8s/base/serviceaccount.yaml)"
  type        = string
  default     = "execution"
}

variable "alerta_email" {
  description = "E-mail que recebe o alarme da DLQ (vazio = só o tópico SNS, sem assinatura)"
  type        = string
  default     = ""
}
