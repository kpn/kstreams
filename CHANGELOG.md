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
