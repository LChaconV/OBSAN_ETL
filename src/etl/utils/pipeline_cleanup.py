from __future__ import annotations

import logging
import os
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import TypeVar

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_ROOT = PROJECT_ROOT / "data"
TRANSFORM_CONFIG_DIR = PROJECT_ROOT / "config" / "transform"
GENERATED_ROOTS = (
    DATA_ROOT / "bronze",
    DATA_ROOT / "silver",
    DATA_ROOT / "golden",
    DATA_ROOT / "tmp",
)
ACTIVE_ENV_VAR = "OBSAN_ETL_CLEANUP_ACTIVE"
DISABLE_ENV_VAR = "OBSAN_ETL_CLEANUP_DISABLED"
PIPELINE_TRANSFORM_CONFIGS = {
    "api_beneficiarios_iraca": ("beneficiarios_iraca_transform.yaml",),
    "api_edu_escolar": ("edu_escolar_transform.yaml",),
    "api_edu_superior": ("edu_superior_transform.yaml",),
    "api_erradicacion_cultivos_coca": ("erradicacion_cultivos_transform.yaml",),
    "api_familias_accion": ("familias_accion_transform.yaml",),
    "api_indice_riesgo_irca": ("irca_transform.yaml",),
    "api_minerales": ("minerales_transform.yaml",),
    "api_produc_gas": ("produc_gas_transform.yaml",),
    "api_produc_petroleo": ("produc_petroleo_transform.yaml",),
    "api_regalias": ("regalias_transform.yaml",),
    "api_test": (),
    "api_victimas": ("victimas_transform.yaml",),
    "dw_departamento": ("departamentos_transform.yaml",),
    "dw_divipola": ("divipola_transform.yaml",),
    "dw_mun_pdet": ("mun_pdet_transform.yaml",),
    "dw_subregiones_antioquia": (),
    "ld_agricultura": ("agricola_transform.yaml",),
    "ld_bajo_peso_nacer": ("bajo_peso_nacer_transform.yaml",),
    "ld_censo_pecuario": ("censo_pecuario_transform.yaml",),
    "ld_desnutricion_aguda_5": ("desnutricion_aguda_5_transform.yaml",),
    "ld_mercado_laboral": ("mercado_laboral_transform.yaml",),
    "ld_mercados_campesinos": ("mercados_campesinos_transform.yaml",),
    "ld_mortalidad_desnutricion": ("mortalidad_desnutricion_transform.yaml",),
    "ld_perfil_antioquia": (),
    "ld_pme_jefe_hogar": ("pme_jefe_hogar_transform.yaml",),
    "ld_pme_por_genero": ("pme_por_genero_transform.yaml",),
    "ld_poblacion": ("poblacion_transform.yaml",),
    "ld_resguardo_indigena": ("resguardo_indigena_transform.yaml",),
    "url_terraclimate": ("terraclimate_transform.yaml",),
}
PIPELINE_EXTRA_PATHS = {
    "dw_subregiones_antioquia": (
        "data/silver/subregiones",
        "data/golden/subregion.parquet",
    ),
}

T = TypeVar("T")


def run_with_cleanup(
    pipeline_name: str,
    pipeline_func: Callable[..., T],
    *args,
    **kwargs,
) -> T:
    if _cleanup_disabled() or os.environ.get(ACTIVE_ENV_VAR) == "1":
        return pipeline_func(*args, **kwargs)

    cleanup_targets = _cleanup_targets_for_pipeline(pipeline_name)
    before_files = _snapshot_generated_files(cleanup_targets)
    before_dirs = _snapshot_generated_dirs(cleanup_targets)
    previous_active = os.environ.get(ACTIVE_ENV_VAR)
    os.environ[ACTIVE_ENV_VAR] = "1"

    try:
        result = pipeline_func(*args, **kwargs)
    except BaseException:
        raise
    else:
        cleanup_generated_files(
            before_files,
            before_dirs=before_dirs,
            cleanup_targets=cleanup_targets,
            pipeline_name=pipeline_name,
        )
        return result
    finally:
        if previous_active is None:
            os.environ.pop(ACTIVE_ENV_VAR, None)
        else:
            os.environ[ACTIVE_ENV_VAR] = previous_active


def cleanup_generated_files(
    before: dict[Path, tuple[int, int]],
    *,
    before_dirs: set[Path] | None = None,
    cleanup_targets: tuple[Path, ...] | None = None,
    pipeline_name: str,
) -> None:
    targets = GENERATED_ROOTS if cleanup_targets is None else cleanup_targets
    paths_to_delete = set(_new_or_modified_files(before, targets))
    input_file = _input_file_to_cleanup()
    if input_file is not None:
        paths_to_delete.add(input_file)

    deleted_files = 0
    deleted_paths: list[Path] = []
    errors: list[str] = []

    for path in sorted(paths_to_delete, key=lambda p: len(p.parts), reverse=True):
        if not _is_inside_generated_roots(path) or not path.exists() or not path.is_file():
            continue

        try:
            path.unlink()
            deleted_files += 1
            deleted_paths.append(path)
        except OSError as exc:
            errors.append(f"{path}: {exc}")

    deleted_dirs = _remove_empty_dirs(
        _parent_dirs(deleted_paths) | _new_generated_dirs(before_dirs or set(), targets)
    )

    message = (
        f"Limpieza ETL completada para {pipeline_name}: "
        f"{deleted_files} archivos y {deleted_dirs} carpetas vacias eliminadas."
    )
    logging.info(message)
    print(message)

    if errors:
        details = "\n".join(errors)
        raise RuntimeError(
            "La carga terminó, pero no se pudieron eliminar todos los archivos "
            f"temporales:\n{details}"
        )


def _cleanup_disabled() -> bool:
    return os.environ.get(DISABLE_ENV_VAR, "").strip().lower() in {
        "1",
        "true",
        "yes",
        "si",
    }


def _cleanup_targets_for_pipeline(pipeline_name: str) -> tuple[Path, ...]:
    source_name = _source_name_from_pipeline(pipeline_name)
    targets: set[Path] = set()

    for file_name in PIPELINE_TRANSFORM_CONFIGS.get(source_name, ()):
        targets.update(_generated_paths_from_yaml(TRANSFORM_CONFIG_DIR / file_name))

    for relative_path in PIPELINE_EXTRA_PATHS.get(source_name, ()):
        targets.add(PROJECT_ROOT / relative_path)

    targets = {path.resolve() for path in targets if _is_inside_generated_roots(path)}

    if targets or source_name in PIPELINE_TRANSFORM_CONFIGS or source_name in PIPELINE_EXTRA_PATHS:
        return tuple(sorted(targets, key=str))

    return GENERATED_ROOTS


def _source_name_from_pipeline(pipeline_name: str) -> str:
    parts = pipeline_name.split(".")
    if len(parts) >= 2 and parts[-1] == "pipeline":
        return parts[-2]

    return pipeline_name


def _generated_paths_from_yaml(path: Path) -> set[Path]:
    if not path.exists():
        logging.warning("No existe config de limpieza ETL: %s", path)
        return set()

    with path.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file) or {}

    paths: set[Path] = set()

    def walk(value) -> None:
        if isinstance(value, dict):
            for nested_value in value.values():
                walk(nested_value)
            return

        if isinstance(value, list):
            for nested_value in value:
                walk(nested_value)
            return

        if not isinstance(value, str) or not value.startswith("data/"):
            return

        candidate = PROJECT_ROOT / value
        if _is_inside_generated_roots(candidate):
            paths.add(candidate)

    walk(data)
    return paths


def _iter_target_files(targets: Iterable[Path]) -> Iterable[Path]:
    for target in targets:
        if target.is_file():
            yield target
            continue

        if not target.is_dir():
            continue

        for path in target.rglob("*"):
            if path.is_file():
                yield path


def _iter_target_dirs(targets: Iterable[Path]) -> Iterable[Path]:
    for target in targets:
        if not target.is_dir():
            continue

        yield target

        for path in target.rglob("*"):
            if path.is_dir():
                yield path


def _snapshot_generated_files(targets: Iterable[Path]) -> dict[Path, tuple[int, int]]:
    snapshot: dict[Path, tuple[int, int]] = {}

    for path in _iter_target_files(targets):
        try:
            stat = path.stat()
        except OSError:
            continue

        snapshot[path.resolve()] = (stat.st_mtime_ns, stat.st_size)

    return snapshot


def _snapshot_generated_dirs(targets: Iterable[Path]) -> set[Path]:
    snapshot: set[Path] = set()

    for path in _iter_target_dirs(targets):
        try:
            snapshot.add(path.resolve())
        except OSError:
            continue

    return snapshot


def _new_or_modified_files(
    before: dict[Path, tuple[int, int]],
    targets: Iterable[Path],
) -> list[Path]:
    paths: list[Path] = []

    for path in _iter_target_files(targets):
        try:
            resolved = path.resolve()
            stat = path.stat()
        except OSError:
            continue

        current = (stat.st_mtime_ns, stat.st_size)
        if before.get(resolved) != current:
            paths.append(resolved)

    return paths


def _input_file_to_cleanup() -> Path | None:
    raw_path = os.environ.get("OBSAN_INPUT_FILE")
    if not raw_path:
        return None

    path = Path(raw_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path

    try:
        resolved = path.resolve()
    except OSError:
        return None

    if resolved.exists() and resolved.is_file() and _is_inside_generated_roots(resolved):
        return resolved

    return None


def _is_inside_generated_roots(path: Path) -> bool:
    try:
        resolved = path.resolve()
    except OSError:
        return False

    for root in GENERATED_ROOTS:
        try:
            resolved.relative_to(root.resolve())
            return True
        except ValueError:
            continue

    return False


def _parent_dirs(paths: Iterable[Path]) -> set[Path]:
    candidates: set[Path] = set()
    roots = [root.resolve() for root in GENERATED_ROOTS]

    for path in paths:
        directory = path.parent.resolve()

        while _is_inside_generated_roots(directory) and directory not in roots:
            candidates.add(directory)
            directory = directory.parent

    return candidates


def _new_generated_dirs(before_dirs: set[Path], targets: Iterable[Path]) -> set[Path]:
    dirs: set[Path] = set()

    for path in _iter_target_dirs(targets):
        try:
            resolved = path.resolve()
        except OSError:
            continue

        if resolved not in before_dirs:
            dirs.add(resolved)

    return dirs


def _remove_empty_dirs(candidates: Iterable[Path]) -> int:
    deleted_dirs = 0

    for directory in sorted(candidates, key=lambda p: len(p.parts), reverse=True):
        try:
            directory.rmdir()
            deleted_dirs += 1
        except OSError:
            continue

    return deleted_dirs
