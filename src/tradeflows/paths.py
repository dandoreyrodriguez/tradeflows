# ensures paths are correctly set up

from dataclasses import dataclass, fields, is_dataclass
from pathlib import Path

def get_project_root() -> Path:
    """
    Get the root path of the project.
    """
    here = Path(__file__).resolve()

    for candidate in [here.parent, *here.parents]:
        if (candidate / "pyproject.toml").exists():
            return candidate

    raise RuntimeError("Could not find project root. Ensure 'pyproject.toml' exists in the project root.")


@dataclass(frozen=True)
class MetadataPaths:
    manifest: Path
    availability: Path
    jobs: Path


@dataclass(frozen=True)
class Paths:
    metadata: MetadataPaths
    raw: Path
    parquet: Path


def build_paths(data_dir: str | Path) -> Paths:

    data_dir = Path(data_dir)

    metadata_dir = data_dir / "metadata"

    return Paths(
        metadata=MetadataPaths(
            manifest=metadata_dir / "manifest",
            availability=metadata_dir / "availability",
            jobs=metadata_dir / "jobs",
        ),
        raw=data_dir / "raw",
        parquet=data_dir / "parquet",
    )


def iter_paths(obj):
    """Yield all Path objects from a nested dataclass."""
    if isinstance(obj, Path):
        yield obj

    elif is_dataclass(obj):
        for field in fields(obj):
            yield from iter_paths(getattr(obj, field.name))


def ensure_paths(paths: Paths) -> None:
    for path in iter_paths(paths):
        path.mkdir(parents=True, exist_ok=True)