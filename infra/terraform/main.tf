# infraestructura minima en gcp para el pipeline
# incluye: pub/sub topic + subscription, dlq topic, bigquery datasets,
# service account para dataflow con permisos minimos, cloud function de validacion

terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.30"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# pub/sub: topic principal + dead letter queue
resource "google_pubsub_topic" "cdc_events" {
  name = var.pubsub_topic
}

resource "google_pubsub_topic" "cdc_events_dlq" {
  name = "${var.pubsub_topic}-dlq"
}

resource "google_pubsub_subscription" "cdc_pipeline" {
  name  = var.pubsub_subscription
  topic = google_pubsub_topic.cdc_events.name

  ack_deadline_seconds       = 60
  message_retention_duration = "604800s" # 7 dias

  # dead letter policy: mensajes que fallan mas de N veces van al dlq
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.cdc_events_dlq.id
    max_delivery_attempts = 5
  }

  # retry policy exponencial
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

# bigquery: datasets bronze/silver/gold + raw
resource "google_bigquery_dataset" "raw" {
  dataset_id                 = var.bq_dataset_raw
  location                   = var.bq_location
  description                = "raw staging desde el pipeline cdc, particionado por dia"
  delete_contents_on_destroy = false
}

resource "google_bigquery_dataset" "bronze" {
  dataset_id                 = var.bq_dataset_bronze
  location                   = var.bq_location
  description                = "bronze: limpieza y tipos"
  delete_contents_on_destroy = false
}

resource "google_bigquery_dataset" "silver" {
  dataset_id                 = var.bq_dataset_silver
  location                   = var.bq_location
  description                = "silver: reglas de negocio aplicadas"
  delete_contents_on_destroy = false
}

resource "google_bigquery_dataset" "gold" {
  dataset_id                 = var.bq_dataset_gold
  location                   = var.bq_location
  description                = "gold: data marts listos para consumo"
  delete_contents_on_destroy = false
}

# service account para dataflow workers con permisos minimos
resource "google_service_account" "dataflow_worker" {
  account_id   = "dataflow-worker"
  display_name = "dataflow worker sa para cdc pipeline"
}

resource "google_project_iam_member" "dataflow_worker_roles" {
  for_each = toset([
    "roles/dataflow.worker",
    "roles/pubsub.subscriber",
    "roles/pubsub.publisher", # para el dlq
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectAdmin", # staging y temp locations
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

# cloud function para validacion post-load
# codigo se sube por separado o via CI, aca solo declaramos el recurso
resource "google_service_account" "validator" {
  account_id   = "cdc-validator"
  display_name = "sa para cloud function de validacion"
}

resource "google_project_iam_member" "validator_roles" {
  for_each = toset([
    "roles/bigquery.dataViewer",
    "roles/bigquery.jobUser",
    "roles/pubsub.publisher",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.validator.email}"
}

resource "google_pubsub_topic" "validation_alerts" {
  name = "cdc-validation-alerts"
}
