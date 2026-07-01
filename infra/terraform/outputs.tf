output "pubsub_topic" {
  value = google_pubsub_topic.cdc_events.id
}

output "pubsub_topic_dlq" {
  value = google_pubsub_topic.cdc_events_dlq.id
}

output "pubsub_subscription" {
  value = google_pubsub_subscription.cdc_pipeline.id
}

output "dataflow_worker_sa" {
  value = google_service_account.dataflow_worker.email
}

output "validator_sa" {
  value = google_service_account.validator.email
}

output "validation_alerts_topic" {
  value = google_pubsub_topic.validation_alerts.id
}
