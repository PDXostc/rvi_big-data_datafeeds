# Data Feeds

Puts data from GPS traces into a kafka topic
Use `sbt run` to start, `CTRL-C` to kill.

## Configuration

1. kafka.broker parameter can be used to provide host and port of the kafka broker.
2. feeds.limit limits number of traces to process. If not specified all traces in the data folder will be used
3. feeds.speed can be used to speed-up the data replay.
