terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }
  # remote state for a shared account; local state is fine for a single developer
  # backend "s3" {
  #   bucket         = "my-tf-state"
  #   key            = "cdc-debezium-kafka-pipeline/terraform.tfstate"
  #   region         = "ap-south-1"
  #   dynamodb_table = "tf-locks"
  # }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      project     = var.project
      environment = var.environment
      owner       = "data-engineering"
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
