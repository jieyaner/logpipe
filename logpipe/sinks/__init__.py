"""Sink implementations for logpipe.

A *sink* is any object that exposes:

    write(record: dict) -> None
    flush() -> None

Sinks receive fully-parsed log records (dicts) from the pipeline and are
responsible for delivering them to an external destination.

Available sinks
---------------
- S3Sink  – buffers records and uploads gzipped NDJSON batches to Amazon S3.
"""

from logpipe.sinks.s3_sink import S3Sink

__all__ = ["S3Sink"]
