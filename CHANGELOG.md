## 0.14.0 (2023-12-14)

### Feat

- getmany added to Stream. Closes #128 (#147)

## 0.13.1 (2023-12-06)

### Fix

- **typing**: StreamFunc type to match the udf signature. (#142)

## 0.13.0 (2023-12-06)

### Feat

- first steps to add dependency injection. Inspect udf coroutines in order to inject different args on it (#141)

## 0.12.6 (2023-11-27)

### Fix

- **TestClient**: call task_done after the topic has consumed the cr (#138)

## 0.12.5 (2023-11-22)

### Fix

- increase total events also when using sync testing (#136)

## 0.12.4 (2023-10-11)

### Fix

- spelling mistake in log message (#133)

## 0.12.3 (2023-09-25)

### Refactor

- **pydantic**: add support for pydantic v2 (#132)

## 0.12.2 (2023-09-20)

### Fix

- **Kafka backend**: use enum values when represent kafka backend as dict. Close #130 (#131)

## 0.12.1 (2023-08-02)

### Fix

- end_offsets off by 1

## 0.12.0 (2023-07-31)

### Feat

- option to disable monitoring during testing added (#125)

## 0.11.12 (2023-07-26)

### Fix

- Prometheus scrape metrics task fixed in order to have a proper shutdown (#124)

## 0.11.11 (2023-07-25)

### Fix

- race condition when creating metrics at the same time that a stream is removed (#121)
- consumer committed metrics should use committed and not last_stable_offset (#120)

## 0.11.10 (2023-07-25)

### Fix

- race condition when creating metrics at the same time that a stream is removed (#121)

## 0.11.9 (2023-07-20)

### Fix

- consumer committed metrics should use committed and not last_stable_offset (#120)

## 0.11.8 (2023-07-18)

### Fix

- pyyaml issue

## 0.11.7 (2023-06-26)

### Fix

- deserializer signature (#117)

## 0.11.6 (2023-05-23)

### Fix

- set default partition when producing with engine (#116)

## 0.11.5 (2023-05-23)

### Fix

- typo in docs (#113)

## 0.11.4 (2023-05-09)

### Fix

- call seek_to_initial_offsets  method after TestConsumer is subscribed (#112)

## 0.11.3 (2023-05-09)

### Fix

- seek to initial offsets using the rebalance listener (#111)

## 0.11.2 (2023-04-26)

### Fix

- skips removing metric if metrics dont exist

## 0.11.1 (2023-04-20)

### Fix

- remove metrics for removed stream

## 0.11.0 (2023-04-19)

### Feat

- position_lag metric added. consumer_lag now is based on last commited offset rather than consumer position (#106)

## 0.10.1 (2023-03-06)

### Fix

- Singlenton removed from PrometheusMonitor (#105)

## 0.10.0 (2023-03-02)

### Feat

- MetricsRebalanceListener added (#104)

## 0.9.1 (2023-02-28)

### Fix

- not set a default rebalance listener to a stream. ManualCommitRebalanceListener added (#103)

## 0.9.0 (2023-02-28)

### Feat

- KstreamsRebalanceListener added as default rebalance listener (#102)

## 0.8.0 (2023-02-27)

### Feat

- RebalanceListener interface added so a rebalance listener can be set to Streams (#100)

## 0.7.4 (2023-02-21)

### Fix

- dependencies updated (#98)

## 0.7.3 (2023-01-25)

### Fix

- TestStreamClient should not wait for topics that are empty (#93)

## 0.7.2 (2023-01-19)

### Fix

- default cadata to None

## 0.7.1 (2022-12-06)

### Fix

- call deserializer regardless consumer_record value (#83)

## 0.7.0 (2022-11-28)

### Feat

- adds ability for stream to be initiated with initial offsets

## 0.6.15 (2022-11-23)

### Perf

- **test_utils**: replace sleep with async queue join when stopping test stream (#78)

## 0.6.14 (2022-11-17)

### Fix

- add remove-stream function to stream_engine

## 0.6.13 (2022-11-11)

### Fix

- **stream**: recreate consumer when a stream instance is restarted (#77)

## 0.6.12 (2022-11-08)

### Fix

- **teststreamclient**: check if consumer is none before creating one

## 0.6.11 (2022-11-02)

### Fix

- **TestStreamClient**: remove unused/unclosed mock producer (#74)

## 0.6.10 (2022-10-13)

### Fix

- **test_clients.py**: adds end_offsets to consumer test client

## 0.6.9 (2022-10-12)

### Fix

- add partitions_for_topic method to consumer test client (#70)

## 0.6.8 (2022-10-12)

### Fix

- clean up topic events after leaving the async context (#68)

## 0.6.7 (2022-10-11)

### Fix

- test client initial partition (#67)

## 0.6.6 (2022-10-11)

### Fix

- TestConsumer partition assigments, TestProducer consumer record and record metadata (#66)

## 0.6.5 (2022-10-06)

### Fix

- use partition 1 as default partition when producing with test client. Producer test client record metadata fixed. Closes #64

## 0.6.4 (2022-10-05)

### Fix

- add commit and commited functions to consumer test client. Closes #61 (#63)

## 0.6.3 (2022-09-21)

### Refactor

- **examples**: rename some examples and fix broken links

## 0.6.2 (2022-09-07)

### Fix

- missing default deserializer

## 0.6.1 (2022-08-31)

### Fix

- module singlenton renamed to singleton (#56)

## 0.6.0 (2022-08-25)

### Feat

- get topics using the TestStreamClient

## 0.5.3 (2022-08-25)

### Fix

- decorator wrapper and tests

## 0.5.2 (2022-08-23)

### Fix

- comment error

## 0.5.1 (2022-08-22)

### Fix

- singlenton removed from StreamEngine

## 0.5.0 (2022-08-22)

### Feat

- add `stream` decorator

### Refactor

- rename some types and typos in docs
- expose `ConsumerRecord` from `kstreams`

## 0.4.4 (2022-08-17)

### Refactor

- remove the `value_` prefix from `value_serializer` and `value_deserializer`

## 0.4.3 (2022-08-11)

### Fix

- typing

## 0.4.2 (2022-08-11)

### Fix

- remove unsubscribe before stopping consumer (#40)

## 0.4.1 (2022-08-10)

### Fix

- unsubscribe consumer before stopping (#37)

## 0.4.0 (2022-08-08)

### Feat

- add kafka backend module

## 0.3.1 (2022-08-03)

### Fix

- remove stream base class (#34)

## 0.3.0 (2022-07-28)

### Feat

- add streams instances without decorator (#28)

## 0.2.4 (2022-07-27)

### Fix

- add mypy and some type hints

## 0.2.3 (2022-07-27)

### Fix

- call serialization methods only when value is present (#27)

## 0.2.2 (2022-07-26)

### Fix

- replace engine initializers methods with start and stop (#24)

## 0.2.1 (2022-07-22)

### Fix

- pipeline tag filter (#19)

## 0.2.0 (2022-07-21)

### Feat

- Yield from stream. Closes #4

## v0.1.0 (2022-07-18)
