# Billing Collector

tracks CPU and memory usage for each Kubernetes namespace (each namespace represents a user's workspace). 

uses **Prometheus**  to query resource usage and uses **Apache Pulsar** to send billing events every 5 minutes.