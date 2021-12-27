# tap-prometheus

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

Module loads PromQL query result for every period specified, calculates an aggregation locally and pushes the result as a record.

Sample config to calculate daily online peak by customer and environment:

```$json
{
  "endpoint": "http://localhost:9000",
  "start_date": "2018-11-01T00:00:00Z",
  "metrics": [
    {
      "name": "online_peak",
      "query": "sum by(customer, environment) (sessions_count)",
      "aggregations": [
        "max"
      ],
      "period": "day",
      "step": "120s",
      "labels": {
        "type": "object",
        "properties": {
          "customer": {
            "type": "string"
          },
          "environment": {
            "type": "string"
          }
        }
      }
    }
  ]
}
```

This tap emits a stream for every `metric` definition.

* `query`: PromQL query, must be a range query
* `aggregations`: The type of aggregation to run on the result, one of "max", "min", "avg"
* `period`: The aggregation period to run on the result, only "day" is supported
* `step`: metrics resolution as a [duration string](https://prometheus.io/docs/prometheus/latest/querying/basics/#time-durations)
* `labels`: JSON Schema for labels returned by the query. You can set this to `{"type": "null"}` if your query does not return any labels.

Several source code parts copied from:

* tap-stripe: https://github.com/singer-io/tap-stripe

Module based on the patched python Prometheus API client *promalyze*: https://github.com/meshcloud/promalyze
