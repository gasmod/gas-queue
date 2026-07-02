# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-07-03

First open source release. Versions prior to 0.3.0 were developed in a private
repository; this entry summarizes the package as published.

### Added

- **`sqs.Service`** — an AWS SQS-backed queue implementing `gas.Service`,
  `gas.JobQueueProvider`, and `gas.ReadyReporter` (`CheckReady` errors before
  `Init` and after `Close`, so callers can drain traffic during graceful
  shutdown). Constructed via `sqs.New()` for DI-based injection.
- **Enqueue options** — `gas.WithDelay` (initial visibility delay),
  `gas.WithGroupID` and `gas.WithDedupeID` (FIFO ordering and
  deduplication), and `gas.WithJobAttributes` (custom message attributes),
  applied through `gas.ApplyEnqueueOptions`.
- **Dequeue with long-polling** — `Dequeue` clamps `maxMessages` to SQS's
  1-10 range (logging a warning if clamped) and falls back to the
  configured `Queue.WaitTimeSeconds` when no explicit wait is given. Both
  system attributes (e.g. `SentTimestamp`, `ApproximateReceiveCount`) and
  message attributes are merged onto the returned `gas.Job.Attributes`.
- **Ack/Nack semantics** — `Ack` deletes the message from the queue; `Nack`
  zeroes the message's visibility timeout via `ChangeMessageVisibility`,
  making it immediately available for redelivery.
- **Configuration** — `Config`/`Settings`, bindable via `gas.ConfigProvider`
  when `WithConfig` is not supplied, covering `Queue.Region`,
  `Queue.Endpoint` (custom endpoint for ElasticMQ or other SQS-compatible
  services), `Queue.AccessKeyID`/`Queue.SecretAccessKey` (falling back to
  the default AWS credential chain when unset), `Queue.VisibilityTimeout`,
  and `Queue.WaitTimeSeconds`, all validated on `Init`.
- **`WithClient`** to inject a pre-configured SQS client (or any type
  satisfying the minimal `sqsClient` interface), and **`Client()`** to
  access the underlying `*sqs.Client` directly for advanced operations
  (`CreateQueue`, `PurgeQueue`, `GetQueueAttributes`, etc.) beyond the
  `gas.JobQueueProvider` interface.
- **`queue.ErrClosed`** sentinel error, returned by `Enqueue`, `Dequeue`,
  `Ack`, `Nack`, and `CheckReady` once the service has been closed.
- **`queuetest` package** with `MockQueue`, a configurable mock of
  `gas.JobQueueProvider` that records calls (`CallCount`, `Reset`) and
  delegates to per-method `Fn` fields when set.

[Unreleased]: https://github.com/gasmod/gas-queue/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/gasmod/gas-queue/releases/tag/v0.3.0
