terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }

  # Mesmo bucket dos outros repositórios, chave própria: o state do Execution
  # não é lido por ninguém (o contrato é a SSM).
  backend "s3" {
    bucket       = "oficina-tfstate-584481982243"
    key          = "execution/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region = var.regiao

  default_tags {
    tags = {
      Projeto   = "oficina"
      Fase      = "4"
      Ambiente  = var.ambiente
      Servico   = "execution"
      ManagedBy = "terraform"
      Repo      = "oficina-execution"
    }
  }
}
