# Click counter

There was an interview system design question around counting web
clicks.  THe requirements were around high throughput but particularly
low latency (results not needed until the next day).

Of course, once a day ingest of daily web logs into a db would be
trivial, but got chatting about smaller apps that maybe just emit logs
to stdout, maybe geographically replicated; moved toward writing click
events to kafka and using stream processing to provide different
insights.

This application develops on that idea, a dummy producer rights click
events to a partitioned kafka topic; flink reads the click data and
provides multiple outputs based on different types of window.


```
docker compose up
```

Takes a while (due to a little hack in the producer healthcheck). Once
the job manager is running goto `localhost:8081` to see flink running
the job.
