"""Serialization helpers for generated datasets."""

import csv
import io
import json
from collections.abc import Sequence
from typing import Any


def serialize(dataset: Sequence[dict[str, Any]], output: str) -> None:
    """Write records to stdout in the requested format.

    Dates and other non-JSON-native values are converted to strings. An empty
    dataset is represented faithfully in each format without raising an index
    error when CSV output is requested.
    """
    if output == "json":
        print(json.dumps(dataset, default=str))
    elif output == "jsonline":
        for record in dataset:
            print(json.dumps(record, default=str))
    elif output == "csv":
        if not dataset:
            return
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=dataset[0].keys())
        writer.writeheader()
        writer.writerows(dataset)
        print(buffer.getvalue(), end="")
    else:
        raise ValueError(f"Unsupported output format: {output!r}")
