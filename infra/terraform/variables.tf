variable "project_id" {
  type        = string
  description = "gcp project id"
}

variable "region" {
  type        = string
  default     = "us-central1"
  description = "region para recursos regionales"
}

variable "bq_location" {
  type        = string
  default     = "US"
  description = "location del bigquery dataset"
}

variable "pubsub_topic" {
  type        = string
  default     = "cdc-events"
  description = "nombre del topic pub/sub con eventos cdc"
}

variable "pubsub_subscription" {
  type        = string
  default     = "cdc-events-pipeline"
  description = "subscription para el pipeline streaming"
}

variable "bq_dataset_raw" {
  type    = string
  default = "raw"
}

variable "bq_dataset_bronze" {
  type    = string
  default = "bronze"
}

variable "bq_dataset_silver" {
  type    = string
  default = "silver"
}

variable "bq_dataset_gold" {
  type    = string
  default = "gold"
}
