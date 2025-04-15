# Billing Collector

tracks CPU and memory usage for each user's workspace.

uses **Prometheus**  to query resource usage and uses **Apache Pulsar** to send billing events every X seconds.


### For Local Testing

You must have `kubectl` connected to the correct environment's k8s cluster.

```bash
k port-forward -n pulsar svc/pulsar-proxy 6650:6650 # in one terminal

k port-forward svc/prometheus-server 9090:9090 -n prometheus # in another terminal

python consumer.py # in another terminal

python run.py # in another terminal
```
