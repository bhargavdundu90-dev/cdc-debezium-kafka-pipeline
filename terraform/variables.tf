variable "project" {
  type    = string
  default = "bank-cdc"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "region" {
  type    = string
  default = "ap-south-1"
}

variable "glue_version" {
  type    = string
  default = "4.0" # Spark 3.3 / Python 3.10
}

variable "glue_worker_type" {
  type    = string
  default = "G.1X"
}

variable "glue_workers" {
  description = "Customer dimension is small; 2 workers is the Glue minimum and enough for a daily merge"
  type        = number
  default     = 2
}

variable "lookback_days" {
  description = "event_date partitions re-read by the daily incremental merge to catch late events"
  type        = number
  default     = 2
}

variable "daily_schedule" {
  description = "Daily incremental run (UTC). 01:30 UTC = 07:00 IST"
  type        = string
  default     = "cron(30 1 * * ? *)"
}

variable "weekly_full_schedule" {
  description = "Weekly full rebuild from bronze (UTC), reconciles anything outside the lookback"
  type        = string
  default     = "cron(30 22 ? * SAT *)"
}

variable "bronze_rejected_expiry_days" {
  type    = number
  default = 30
}

variable "create_msk" {
  description = "Create an MSK Serverless cluster for the Debezium topic (off by default: it costs money while idle)"
  type        = bool
  default     = false
}

variable "vpc_subnet_ids" {
  description = "Private subnets for MSK Serverless (only used when create_msk = true)"
  type        = list(string)
  default     = []
}

variable "vpc_security_group_ids" {
  type    = list(string)
  default = []
}

variable "alarm_email" {
  description = "Email subscribed to Glue job failure alerts; empty to skip"
  type        = string
  default     = ""
}
