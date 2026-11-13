"""Immutable evidence values and their JSON representation for ChatService."""

from datetime import UTC, date, datetime
from math import isfinite
from typing import Literal, Self
from uuid import uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_serializer, field_validator, model_validator

type Cell = str | int | float | bool | None


class EvidenceSource(BaseModel):
    """One result set with provenance shared by its rows."""

    model_config = ConfigDict(frozen=True, strict=True)

    table: str = Field(min_length=1)
    metric: str = Field(min_length=1)
    units: str = Field(min_length=1)
    identifiers: tuple[str, ...]
    time_start: AwareDatetime | date
    time_end: AwareDatetime | date
    rows: tuple[tuple[tuple[str, Cell], ...], ...]
    checkpoint: int | None = Field(default=None, ge=-1)
    watermark: AwareDatetime | None = None
    window_state: Literal["open", "closed"] | None = None

    @model_validator(mode="after")
    def validate_provenance(self) -> Self:
        if type(self.time_start) is not type(self.time_end) or self.time_start >= self.time_end:
            raise ValueError("Evidence requires ordered time bounds of the same type")
        if self.window_state is not None and self.checkpoint is None:
            raise ValueError("Streaming windows require a checkpoint")
        for row in self.rows:
            if not set(self.identifiers) <= dict(row).keys():
                raise ValueError("Evidence row is missing a declared identifier")
            if any(isinstance(value, float) and not isfinite(value) for _, value in row):
                raise ValueError("Evidence values must be finite")
        return self

    @field_validator("rows", mode="before")
    @classmethod
    def freeze_rows(cls, rows):
        if not isinstance(rows, (list, tuple)) or any(not isinstance(row, dict) for row in rows):
            raise ValueError("Evidence rows must be objects")
        return tuple(tuple(row.items()) for row in rows)

    @field_serializer("rows")
    def serialize_rows(self, rows):
        return [dict(row) for row in rows]


class EvidencePackage(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    question: str = Field(min_length=1)
    sources: tuple[EvidenceSource, ...]
    snapshot_id: str = Field(default_factory=lambda: uuid4().hex)
    captured_at: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))
