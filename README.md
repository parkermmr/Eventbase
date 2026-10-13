[![CI Status][ci-shield]][ci-url]
[![Coverage][coverage-shield]][coverage-url]
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]

# Eventbase

Couriers that publish and poll events. An event is binary content with
json metadata. It travels on a bus (a kafka topic or a watched
directory), and its content can be claim checked into a store (an s3
bucket or a directory) so only a small json claim crosses the bus.

```
pip install eventbase            # directory only, standard library
pip install "eventbase[kafka]"   # + kafka bus (aiokafka)
pip install "eventbase[s3]"      # + s3 store (aiobotocore)
pip install "eventbase[all]"
```

## Couriers

| Mode                          | Construct                                          | Metadata lives in      |
|-------------------------------|----------------------------------------------------|------------------------|
| Directory only (default)      | `Courier.directory(path, prefixes=())`             | filesystem stat        |
| Kafka only                    | `Courier.kafka(topic)`                             | kafka headers          |
| Kafka + S3 claim check        | `Courier.kafka(topic, store=S3Store(bucket))`      | s3 user metadata       |
| Kafka + directory claim check | `Courier.kafka(topic, store=DirectoryStore(path))` | kafka headers + stat   |

```python
import asyncio
from eventbase import Courier, S3Store

async def main():
    store = S3Store("events", prefix="inbox", endpoint="http://localhost:9000")
    async with Courier.kafka("events", store=store, bootstrap="localhost:9092", parallel=16) as courier:
        assert await courier.ping(timeout=5)
        await courier.publish(b"\x89PNG...", {"source": "camera", "frame": 12})
        for message in await courier.poll(limit=50):
            print(message.id, len(message.content), message.metadata)
            await courier.acknowledge(message)

asyncio.run(main())
```

## API

- `connect()` / `disconnect()`, or `async with`.
- `ping(timeout=5.0) -> bool` - bus and store both answer in time.
- `publish(content, metadata=None, key=None) -> str` - returns the key.
- `poll(limit=10, sequential=False, timeout=1.0) -> list[Message]` - claimed
  content is fetched concurrently, at most `parallel` at a time (set on the
  courier), or one by one with `sequential=True`. Order is preserved.
- `acknowledge(message)` - kafka commits the offset; the directory bus moves
  the file into `.done/`. Stores are never touched.
- `release(messages)` - hand polled, unacknowledged messages back so the next
  poll redelivers them (kafka seeks back; the directory bus forgets them).

## Runner

A run-forever loop over a courier. `handle` may be `async def` or a plain
function (run on a worker thread).

```python
from eventbase import Courier, Runner

async def handle(message):
    ...                                   # raise to retry

runner = Runner(
    Courier.directory("/inbox", prefixes=["invoice_"]),
    handle,
    retries=3,                            # then give up on the message
    dead=Courier.directory("/dead"),      # where given-up messages go
    concurrency=1,                        # >1 handles a batch concurrently
)
await runner.run()                        # until runner.stop(), SIGINT or SIGTERM
```

- At least once: a message is acknowledged only after `handle` returns, in bus
  order, never past a failure. A failure releases the rest of the batch and
  retries after `pause`.
- After `retries` failures the message is published to `dead` (metadata gains
  `error` and `failures`), or dropped and logged when there is no `dead`, then
  acknowledged.
- Broker, store or dead-letter errors disconnect and reconnect with a doubling
  backoff from `pause` up to `ceiling`.
- Stopping finishes the batch in hand before returning.

## Behaviour notes

- Metadata is encoded one key per kafka header / `x-amz-meta-*` entry, each
  value json. S3 lowercases keys and caps metadata at 2KB per object.
- Files cannot hold metadata, so directory messages carry `key`, `name`,
  `size`, `modified` and `type` from the filesystem. With kafka in front,
  published metadata rides the headers and merges over those.
- The directory bus picks up any visible file, oldest first, recursively;
  `prefixes` restricts it to paths starting with one of them. Hidden names
  (in-flight `.tmp` writes, `.done/`) are skipped. Polled but unacknowledged
  files are not handed out twice until the courier reconnects.
- Kafka commits a position: acknowledging a record acknowledges every
  earlier record of its partition. The consumer joins its group on the
  first poll, so publish-only couriers never join one.

## Tests

```
pytest --cov                             # unit tests with coverage
EVENTBASE_KAFKA=localhost:9092 EVENTBASE_S3=http://localhost:9000 \
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... pytest tests/test_live.py
```

[ci-shield]: https://img.shields.io/github/actions/workflow/status/parkermmr/eventbase/ci.yml?branch=main&style=for-the-badge&label=CI
[ci-url]: https://github.com/parkermmr/eventbase/actions/workflows/ci.yml
[coverage-shield]: https://img.shields.io/codecov/c/github/parkermmr/eventbase?style=for-the-badge
[coverage-url]: https://codecov.io/gh/parkermmr/eventbase
[contributors-shield]: https://img.shields.io/github/contributors/parkermmr/eventbase.svg?style=for-the-badge
[contributors-url]: https://github.com/parkermmr/eventbase/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/parkermmr/eventbase.svg?style=for-the-badge
[forks-url]: https://github.com/parkermmr/eventbase/network/members
[stars-shield]: https://img.shields.io/github/stars/parkermmr/eventbase.svg?style=for-the-badge
[stars-url]: https://github.com/parkermmr/eventbase/stargazers
[issues-shield]: https://img.shields.io/github/issues/parkermmr/eventbase.svg?style=for-the-badge
[issues-url]: https://github.com/parkermmr/eventbase/issues
