"""Configuration and filesystem validation for the Parquet analytics layer."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import tempfile

from dotenv import load_dotenv


class ParquetConfigurationError(ValueError):
    """Raised when the Parquet layer cannot safely use its configuration."""


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ParquetConfigurationError(
        f"{name} must be one of true/false, yes/no, on/off, or 1/0."
    )


def _env_positive_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ParquetConfigurationError(f"{name} must be an integer.") from exc
    if value <= 0:
        raise ParquetConfigurationError(f"{name} must be greater than zero.")
    return value


@dataclass(frozen=True)
class ParquetConfig:
    enabled: bool
    root: Path | None
    compression: str = "zstd"
    compression_level: int = 3
    target_file_size_bytes: int = 256 * 1024 * 1024
    refresh_batch_size: int = 100_000
    scheduler_poll_seconds: int = 30

    @classmethod
    def from_env(cls) -> "ParquetConfig":
        load_dotenv()
        root_value = os.getenv("PARQUET_ROOT")
        return cls(
            enabled=_env_bool("PARQUET_ENABLED", False),
            root=Path(root_value) if root_value else None,
            compression="zstd",
            compression_level=_env_positive_int("PARQUET_COMPRESSION_LEVEL", 3),
            target_file_size_bytes=_env_positive_int(
                "PARQUET_TARGET_FILE_SIZE_MB", 256
            )
            * 1024
            * 1024,
            refresh_batch_size=_env_positive_int(
                "PARQUET_REFRESH_BATCH_SIZE", 100_000
            ),
            scheduler_poll_seconds=_env_positive_int(
                "PARQUET_SCHEDULER_POLL_SECONDS", 30
            ),
        )

    def validate(self, *, require_enabled: bool = True) -> Path:
        if require_enabled and not self.enabled:
            raise ParquetConfigurationError(
                "The Parquet layer is disabled. Set PARQUET_ENABLED=true to use it."
            )
        if self.root is None:
            raise ParquetConfigurationError("PARQUET_ROOT is required.")
        if not self.root.is_absolute():
            raise ParquetConfigurationError("PARQUET_ROOT must be an absolute path.")
        if not 1 <= self.compression_level <= 22:
            raise ParquetConfigurationError(
                "PARQUET_COMPRESSION_LEVEL must be between 1 and 22 for Zstandard."
            )

        try:
            self.root.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise ParquetConfigurationError(
                f"Cannot create PARQUET_ROOT {self.root}: {exc}"
            ) from exc
        if not self.root.is_dir():
            raise ParquetConfigurationError(
                f"PARQUET_ROOT is not a directory: {self.root}"
            )

        probe_path: Path | None = None
        renamed_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", prefix="_parquet_probe_", dir=self.root, delete=False
            ) as probe:
                probe.write(b"probe")
                probe.flush()
                os.fsync(probe.fileno())
                probe_path = Path(probe.name)
            renamed_path = probe_path.with_name(probe_path.name + ".renamed")
            os.replace(probe_path, renamed_path)
            renamed_path.unlink()
        except OSError as exc:
            for candidate in (probe_path, renamed_path):
                if candidate is not None:
                    try:
                        candidate.unlink(missing_ok=True)
                    except OSError:
                        pass
            raise ParquetConfigurationError(
                f"PARQUET_ROOT is not writable with same-directory rename support: {exc}"
            ) from exc

        return self.root.resolve()
