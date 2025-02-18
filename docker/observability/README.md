## Observability

Run the observability stack

Contains:
- otel-collector
- prometheus
- grafana
- elasticsearch
- kibana

### Running the stack

Uses the following configs
otel-config.yaml - Used for the otel-collector
    - Exports metrics to prometheus
    - Exports traces to elasticsearch
prom-config.yaml - Used for the prometheus server
    - Scrapes otel-collector metrics

```
docker-compose up
```

Once the containers are running, you can access the following URLs:
- otel-collector: http://localhost:9464/metrics
- prometheus: http://localhost:9090
- grafana: http://localhost:3000
- elasticsearch: http://localhost:9200
- kibana: http://localhost:5601

Use the following command to connect our local dev server to the observability stack:
```
docker network connect springtail-fdw-network dev
```