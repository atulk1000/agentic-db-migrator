from __future__ import annotations

import hashlib
from pathlib import Path

from amo.core.workflow_models import ArtifactDigest


class ArtifactIntegrityError(ValueError):
    """Raised when an artifact no longer matches its approved identity."""


def sha256_file(path: str | Path) -> str:
    artifact_path = Path(path)
    digest = hashlib.sha256()
    with artifact_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def describe_artifact(path: str | Path) -> ArtifactDigest:
    artifact_path = Path(path)
    if not artifact_path.is_file():
        raise ArtifactIntegrityError(f"Artifact does not exist: {artifact_path}")
    return ArtifactDigest(path=str(artifact_path), sha256=sha256_file(artifact_path))


def verify_artifact(
    artifact: ArtifactDigest,
    *,
    override_path: str | Path | None = None,
    label: str = "artifact",
) -> Path:
    artifact_path = Path(override_path or artifact.path)
    if not artifact_path.is_file():
        raise ArtifactIntegrityError(f"Approved {label} does not exist: {artifact_path}")
    actual_digest = sha256_file(artifact_path)
    if actual_digest != artifact.sha256:
        raise ArtifactIntegrityError(
            f"{label} SHA-256 does not match the approved artifact. "
            f"Regenerate approval after reviewing the updated {label}."
        )
    return artifact_path
