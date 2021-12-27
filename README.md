# tap-prometheus

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

Module loads PromQL query result for every period specified, calculates an aggregation locally and pushes the result as a record.

Sample config to calculate daily online peak:

```$json
{
  "endpoint": "http://localhost:9000",
  "start_date": "2018-11-01T00:00:00Z",
  "metrics": [
    {
        "name": "online_peak",
        "query": "sum(sessions_count)",
        "aggregations": ["max"],
        "period": "day",
        "step": "120s"
    }
  ]
}
```

* `query`: PromQL query, must be a range query
* `aggregations`: The type of aggregation to run on the result, one of "max", "min", "avg"
* `period`: The aggregation period to run on the result, only "day" is supported
* `step`: metrics resolution as a [duration string](https://prometheus.io/docs/prometheus/latest/querying/basics/#time-durations)

Several source code parts copied from:

* tap-stripe: https://github.com/singer-io/tap-stripe

Module based on the patched python Prometheus API client *promalyze*: https://github.com/meshcloud/promalyze
