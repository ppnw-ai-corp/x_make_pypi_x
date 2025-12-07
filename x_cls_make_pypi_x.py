from __future__ import annotations

import argparse
import importlib
import json

# Inlined minimal helpers from x_make_common_x.helpers
import logging
import os
import shutil
import subprocess as _subprocess
import sys
import sys as _sys
import urllib.request
import uuid
from collections.abc import Mapping, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from typing import IO, TYPE_CHECKING, TypeVar, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

from jsonschema import ValidationError
from x_0_make_all_x.manifest import ManifestEntry, ManifestOptions
from x_make_common_x.json_contracts import validate_payload

from x_make_pypi_x.json_contracts import ERROR_SCHEMA, INPUT_SCHEMA, OUTPUT_SCHEMA
from x_make_pypi_x.publish_flow import PublisherFactory, publish_manifest_entries

_LOGGER = logging.getLogger("x_make")
_T = TypeVar("_T")

_EMPTY_MAPPING: Mapping[str, object] = MappingProxyType({})


def _info(*args: object) -> None:
    msg = " ".join(str(a) for a in args)
    with suppress(Exception):
        _LOGGER.info("%s", msg)
    if not _emit_print(msg):
        with suppress(Exception):
            _sys.stdout.write(msg + "\n")


def _error(*args: object) -> None:
    msg = " ".join(str(a) for a in args)
    with suppress(Exception):
        _LOGGER.error("%s", msg)
    if not _emit_error_print(msg):
        with suppress(Exception):
            _sys.stderr.write(msg + "\n")


def _emit_print(msg: str) -> bool:
    try:
        print(msg)
    except (OSError, RuntimeError):
        return False
    return True


def _emit_error_print(msg: str) -> bool:
    try:
        print(msg, file=_sys.stderr)
    except (OSError, RuntimeError):
        return False
    return True


def _ctx_flag(ctx: object | None, attr: str, *, default: bool = False) -> bool:
    """Best-effort bool coercion for optional orchestrator contexts."""

    if ctx is None:
        return default
    try:
        value_obj: object = getattr(ctx, attr)
    except (AttributeError, RuntimeError, ValueError, TypeError):
        return default
    value = value_obj
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    if isinstance(value, (int, float)):
        return bool(value)
    return bool(value)


class BaseMake:
    TOKEN_ENV_VAR: str = "GITHUB_TOKEN"  # noqa: S105 - documented env var

    @classmethod
    def get_env(cls, name: str, default: _T | None = None) -> str | _T | None:
        value = os.environ.get(name)
        if value is None:
            return default
        return value

    @classmethod
    def get_env_bool(cls, name: str, *, default: bool = False) -> bool:
        v = os.environ.get(name, None)
        if v is None:
            return default
        return str(v).lower() in ("1", "true", "yes")

    def get_token(self) -> str | None:
        return os.environ.get(self.TOKEN_ENV_VAR)

    def run_cmd(
        self,
        args: Iterable[str],
        *,
        check: bool = False,
        cwd: str | None = None,
        timeout: float | None = None,
        env: Mapping[str, str] | None = None,
    ) -> _subprocess.CompletedProcess[str]:
        return _subprocess.run(  # noqa: S603 - shell=False by default
            list(args),
            check=check,
            capture_output=True,
            text=True,
            cwd=cwd,
            timeout=timeout,
            env=env,
        )


"""Twine-backed PyPI publisher implementation (installed shim)."""


_ALLOWED_URL_SCHEMES = {"http", "https"}


@contextmanager
def _safe_urlopen(url: str, *, timeout: float) -> Iterator[IO[bytes]]:
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    if scheme not in _ALLOWED_URL_SCHEMES:
        message = f"Refusing to open URL with unsupported scheme '{scheme}'"
        raise ValueError(message)
    response = urllib.request.urlopen(  # noqa: S310 - scheme validated above
        url,
        timeout=timeout,
    )
    try:
        yield cast("IO[bytes]", response)
    finally:
        response.close()


class XClsMakePypiX(BaseMake):
    # Configurable endpoints and env names
    PYPI_INDEX_URL: str = "https://pypi.org"
    TEST_PYPI_URL: str = "https://test.pypi.org"
    TEST_PYPI_TOKEN_ENV: str = "TEST_PYPI_TOKEN"  # noqa: S105 - documented env var name

    def version_exists_on_pypi(self) -> bool:
        """Check if the current package name and version already exist on PyPI."""
        try:
            url = f"{self.PYPI_INDEX_URL}/pypi/{self.name}/json"
            with _safe_urlopen(url, timeout=10) as response:
                response_bytes = response.read()
            payload_text = response_bytes.decode("utf-8")
            payload_raw: object = json.loads(payload_text)
        except (
            HTTPError,
            URLError,
            TimeoutError,
            json.JSONDecodeError,
            OSError,
        ) as exc:
            _info(
                "WARNING:",
                f"Could not check PyPI for {self.name}=={self.version}:",
                exc,
            )
            return False
        if not isinstance(payload_raw, dict):
            return False
        payload = cast("dict[str, object]", payload_raw)
        releases_raw = payload.get("releases")
        if isinstance(releases_raw, dict):
            return self.version in releases_raw
        return False

    def __init__(  # noqa: PLR0913 - orchestrator surface preserved
        self,
        name: str,
        version: str,
        author: str,
        email: str,
        description: str,
        license_text: str,
        dependencies: list[str],
        ctx: object | None = None,
        **kwargs: object,
    ) -> None:
        # accept optional orchestrator context (backwards compatible)
        self._ctx = ctx

        # store basic metadata
        self.name = name
        self.version = version
        self.author = author
        self.email = email
        self.description = description
        self.license_text = license_text
        self.dependencies = dependencies
        self._project_dir: Path | None = None

        # Prefer ctx-provided dry_run when available (tests expect this)
        self.dry_run = _ctx_flag(self._ctx, "dry_run", default=False)

        self._extra: dict[str, object] = dict(kwargs)
        self.debug = bool(self._extra.get("debug", False))

        # Print preparation message when verbose is requested (or always is OK)
        if _ctx_flag(self._ctx, "verbose", default=False):
            _info(f"[pypi] prepared publisher for {self.name}=={self.version}")

    def update_pyproject_toml(self, _project_dir: str) -> None:
        # Intentionally removed: no metadata file manipulation in this publisher.
        # Older behavior updated project metadata here; that logic was removed
        # to ensure this module does not touch or create packaging metadata files.
        return

    def ensure_type_metadata(  # noqa: C901, PLR0912, PLR0915
        self,
        repo_name: str,
        base_dir: str,
        ancillary_files: list[str] | None = None,
    ) -> None:
        """Inject PEP 561 artifacts and minimal build metadata without recursion."""
        pkg_path = Path(base_dir)
        bd = Path(repo_name)

        py_typed = pkg_path / "py.typed"
        if not py_typed.exists():
            with suppress(OSError):
                py_typed.write_text("", encoding="utf-8")
            if not py_typed.exists():
                return

        explicit_files: list[str] = []
        for ancillary in ancillary_files or []:
            rel = ancillary.replace("\\", "/").lstrip("/")
            candidate = pkg_path / rel
            with suppress(OSError, ValueError):
                if candidate.is_file():
                    explicit_files.append(rel)

        manifest_lines = [f"include {pkg_path.name}/py.typed"]
        manifest_lines.extend(
            f"include {pkg_path.name}/{rel}" for rel in explicit_files
        )
        unique_lines: list[str] = []
        seen_lines: set[str] = set()
        for line in manifest_lines:
            if line in seen_lines:
                continue
            seen_lines.add(line)
            unique_lines.append(line)
        man_path = bd / "MANIFEST.in"
        with suppress(OSError):
            man_path.write_text("\n".join(unique_lines) + "\n", encoding="utf-8")

        base_pyproject = (
            "[build-system]\n"
            'requires = ["setuptools", "wheel"]\n'
            'build-backend = "setuptools.build_meta"\n\n'
            "[project]\n"
            f'name = "{self.name}"\n'
            f'version = "{self.version}"\n'
            f'description = "{self.description or f"Package {self.name}"}"\n'
            'requires-python = ">=3.8"\n'
        )
        if self.author or self.email:
            author_name = self.author or "Unknown"
            author_email = self.email or "unknown@example.com"
            author_entry = (
                "authors = ["
                f'{{name = "{author_name}", email = "{author_email}"}}'
                "]\n"
            )
            base_pyproject += author_entry
        if self.dependencies:
            deps_serial = ",\n    ".join(f'"{dep}"' for dep in self.dependencies)
            base_pyproject += "dependencies = [\n    " + deps_serial + "\n]\n"

        pkg_data_items = [
            '"py.typed"',
            *[f'"{rel}"' for rel in explicit_files],
        ]
        pkg_block = (
            "\n[tool.setuptools]\ninclude-package-data = true\n"
            '\n[tool.setuptools.packages.find]\nwhere = ["."]\n'
            f'include = ["{pkg_path.name}"]\n'
            "\n[tool.setuptools.package-data]\n"
            f"{pkg_path.name} = [{', '.join(pkg_data_items)}]\n"
        )
        base_pyproject += pkg_block

        pyproject = bd / "pyproject.toml"
        if not pyproject.exists():
            with suppress(OSError):
                pyproject.write_text(base_pyproject, encoding="utf-8")
            return

        try:
            txt = pyproject.read_text(encoding="utf-8")
        except OSError:
            txt = ""
        changed = False
        if "include-package-data" not in txt:
            if "[tool.setuptools]" in txt:
                txt += "\ninclude-package-data = true\n"
            else:
                txt += "\n[tool.setuptools]\ninclude-package-data = true\n"
            changed = True
        if "[tool.setuptools.packages.find]" not in txt:
            txt += '\n[tool.setuptools.packages.find]\nwhere = ["."]\n'
            changed = True
        pkg_data_block = (
            "\n[tool.setuptools.package-data]\n"
            f"{pkg_path.name} = [{', '.join(pkg_data_items)}]\n"
        )
        if "[tool.setuptools.package-data]" not in txt or pkg_data_block not in txt:
            txt += pkg_data_block
            changed = True
        if "name =" not in txt or "version =" not in txt:
            txt += "\n" + base_pyproject
            changed = True
        if changed:
            with suppress(OSError):
                pyproject.write_text(txt, encoding="utf-8")

    def create_files(  # noqa: C901, PLR0912, PLR0915
        self,
        main_file: str,
        ancillary_files: list[str],
    ) -> None:
        """
        Create a minimal package tree in a temporary build directory and
        copy files.
        """
        package_name = self.name
        repo_build_root = Path(__file__).resolve().parent / "_build_temp_x_pypi_x"
        repo_build_root.mkdir(parents=True, exist_ok=True)
        build_dir = repo_build_root / f"_build_{package_name}_{uuid.uuid4().hex}"
        build_dir.mkdir(parents=True, exist_ok=True)
        package_dir = build_dir / package_name
        if package_dir.exists():
            if package_dir.is_dir():
                shutil.rmtree(package_dir)
            else:
                package_dir.unlink()
        package_dir.mkdir(parents=True, exist_ok=True)

        main_path = Path(main_file)
        shutil.copy2(main_path, package_dir / main_path.name)
        init_path = package_dir / "__init__.py"
        if not init_path.exists():
            init_path.write_text("# Package init\n", encoding="utf-8")

        def _is_allowed(p: str) -> bool:
            """Allow-list files copied into the build."""
            path_obj = Path(p)
            ext = path_obj.suffix.lower()
            allowed = {".py", ".txt", ".md", ".rst"}
            return ext in allowed or path_obj.name.lower() == "__init__.py"

        # Copy ancillaries: files only; do not recurse directories
        for entry in ancillary_files or []:
            rel_norm = entry.replace("\\", "/").lstrip("/")
            src_path = Path(entry)
            if src_path.is_dir():
                _info(f"Ignoring ancillary directory (no recursion): {rel_norm}")
                continue
            if src_path.is_file() and _is_allowed(str(src_path)):
                dest_path = package_dir / rel_norm
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src_path, dest_path)

        # Ensure lightweight stub files (.pyi) exist for typing in every
        # package directory and for each copied module. These are minimal
        # and safe: they do not attempt to reconstruct full signatures, but
        # provide placeholders so downstream tools won't fail to find stubs.
        with suppress(OSError, ValueError):
            for pkg_dir in [package_dir, *package_dir.rglob("*")]:
                if not pkg_dir.is_dir():
                    continue
                pyi_init = pkg_dir / "__init__.pyi"
                if pyi_init.exists():
                    continue
                with suppress(OSError):
                    pyi_init.write_text(
                        (
                            f"# Type stubs for package {pkg_dir.name}\n"
                            "from typing import Any\n\n__all__: list[str]\n"
                        ),
                        encoding="utf-8",
                    )

        with suppress(OSError, ValueError):
            for py_file in package_dir.rglob("*.py"):
                if py_file.suffix == ".pyi":
                    continue
                stub_path = py_file.with_suffix(".pyi")
                if stub_path.exists():
                    continue
                with suppress(OSError):
                    stub_path.write_text(
                        f"# Stub for {py_file.name}\nfrom typing import Any\n\n",
                        encoding="utf-8",
                    )

        # After stubs generated, ensure PEP 561 artifacts & metadata
        with suppress(OSError, ValueError, ImportError):
            self.ensure_type_metadata(str(build_dir), str(package_dir), ancillary_files)
        self._project_dir = build_dir

    def prepare(self, main_file: str, ancillary_files: list[str]) -> None:
        main_path = Path(main_file)
        if not main_path.exists():
            message = f"Main file '{main_file}' does not exist."
            raise FileNotFoundError(message)
        for ancillary_file in ancillary_files or []:
            if not Path(ancillary_file).exists():
                message = f"Ancillary file '{ancillary_file}' is not found."
                raise FileNotFoundError(message)

    def publish(  # noqa: C901, PLR0912, PLR0915
        self,
        main_file: str,
        ancillary_files: list[str],
    ) -> bool:
        """Build and upload package to PyPI using build + twine.

        Returns True on success; False only for explicit stub behavior.
        """
        # If version already exists, skip
        if self.version_exists_on_pypi():
            msg = (
                f"SKIP: {self.name} version {self.version} already "
                "exists on PyPI. Skipping publish."
            )
            _info(msg)
            return True
        self.create_files(main_file, ancillary_files or [])
        project_dir = self._project_dir
        if project_dir is None:
            message = "Build directory not prepared; call create_files first"
            raise RuntimeError(message)
        dist_dir = project_dir / "dist"
        if dist_dir.exists():
            shutil.rmtree(dist_dir)

        build_cmd = [sys.executable, "-m", "build"]
        _info("Running build:", " ".join(build_cmd))
        build_result = self.run_cmd(build_cmd, check=False, cwd=str(project_dir))
        if build_result.stdout:
            _info(build_result.stdout)
        if build_result.stderr:
            _error(build_result.stderr)
        if build_result.returncode != 0:
            message = "Build failed. Aborting publish."
            raise RuntimeError(message)

        if not dist_dir.exists():
            message = "dist/ directory not found after build."
            raise RuntimeError(message)

        expected_prefix = f"{self.name}-{self.version}".lower()
        artifacts = [
            path
            for path in dist_dir.iterdir()
            if path.is_file()
            and (path.suffix == ".whl" or path.name.endswith(".tar.gz"))
        ]
        if not artifacts:
            artifacts = [
                *dist_dir.rglob("*.whl"),
                *dist_dir.rglob("*.tar.gz"),
            ]
        preferred = [
            path for path in artifacts if path.name.lower().startswith(expected_prefix)
        ]
        files = preferred or artifacts
        if not files:
            message = "No valid distribution files found. Aborting publish."
            raise RuntimeError(message)

        pypirc_path = Path.home() / ".pypirc"
        has_env_creds = any(
            [
                self.get_env("TWINE_USERNAME"),
                self.get_env("TWINE_PASSWORD"),
                self.get_env("TWINE_API_TOKEN"),
            ]
        )
        if not pypirc_path.exists() and not has_env_creds:
            _info(
                "WARNING: No PyPI credentials found (.pypirc or TWINE env vars)."
                " Upload will likely fail."
            )

        skip_existing = self.get_env_bool("TWINE_SKIP_EXISTING", default=True)
        env_snapshot = {
            "TWINE_USERNAME": bool(os.environ.get("TWINE_USERNAME")),
            "TWINE_PASSWORD": bool(os.environ.get("TWINE_PASSWORD")),
            "TWINE_API_TOKEN": bool(os.environ.get("TWINE_API_TOKEN")),
        }
        snapshot_parts = [
            f"{name}={'set' if present else 'unset'}"
            for name, present in env_snapshot.items()
        ]
        _info(
            "Twine environment snapshot (pre-upload):",
            "; ".join(snapshot_parts),
        )
        _info("Twine publisher module path:", __file__)
        base_cmd = [sys.executable, "-m", "twine", "upload"]
        if skip_existing:
            base_cmd.append("--skip-existing")
            _info("Running upload (with --skip-existing):", " ".join(base_cmd))
        else:
            _info("Running upload:", " ".join(base_cmd))
        twine_cmd = [*base_cmd, *(str(path) for path in files)]

        result = self.run_cmd(twine_cmd, check=False, cwd=str(project_dir))
        if result.stdout:
            _info(result.stdout)
        if result.stderr:
            _error(result.stderr)
        if result.returncode != 0:
            message = "Twine upload failed. See output above."
            raise RuntimeError(message)
        return True

    def prepare_and_publish(self, main_file: str, ancillary_files: list[str]) -> None:
        # Always validate inputs (evidence cleanup is enforced unconditionally).
        self.prepare(main_file, ancillary_files or [])
        self.publish(main_file, ancillary_files or [])


def _failure_payload(
    message: str, *, details: Mapping[str, object] | None = None
) -> dict[str, object]:
    payload: dict[str, object] = {"status": "failure", "message": message}
    if details:
        payload["details"] = {str(key): value for key, value in details.items()}
    with suppress(ValidationError):
        validate_payload(payload, ERROR_SCHEMA)
    return payload


def _normalize_string(value: object) -> str | None:
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return None


def _normalize_string_list(values: object) -> tuple[str, ...]:
    if isinstance(values, Sequence) and not isinstance(values, (str, bytes, bytearray)):
        collected: list[str] = []
        for item in values:
            if isinstance(item, str):
                text = item.strip()
                if text:
                    collected.append(text)
        return tuple(collected)
    return ()


def _mapping_from_object(raw: object) -> Mapping[str, object] | None:
    if isinstance(raw, Mapping):
        return MappingProxyType({str(key): value for key, value in raw.items()})
    return None


def _options_from_json(raw: Mapping[str, object] | None) -> ManifestOptions:
    if raw is None:
        return ManifestOptions()
    dependencies = _normalize_string_list(raw.get("dependencies"))
    allowlist = _normalize_string_list(raw.get("ancillary_allowlist"))
    ancillary_list = _normalize_string_list(raw.get("ancillary_list"))
    extra_dict = _mapping_from_object(raw.get("extra"))
    extra_proxy = (
        MappingProxyType(dict(extra_dict)) if extra_dict else MappingProxyType({})
    )
    return ManifestOptions(
        author=_normalize_string(raw.get("author")),
        email=_normalize_string(raw.get("email")),
        description=_normalize_string(raw.get("description")),
        license_text=_normalize_string(raw.get("license_text")),
        dependencies=dependencies,
        pypi_name=_normalize_string(raw.get("pypi_name")),
        ancillary_allowlist=allowlist,
        ancillary_list=ancillary_list,
        extra=extra_proxy,
    )


def _entry_from_json(entry: Mapping[str, object]) -> ManifestEntry:
    options_raw = entry.get("options")
    options_mapping = (
        _mapping_from_object(options_raw) if isinstance(options_raw, Mapping) else None
    )
    options = _options_from_json(options_mapping)
    ancillary = _normalize_string_list(entry.get("ancillary"))
    package = _normalize_string(entry.get("package"))
    version = _normalize_string(entry.get("version"))
    if package is None or version is None:
        message = "manifest entry is missing required fields"
        raise ValueError(message)
    return ManifestEntry(
        package=package,
        version=version,
        ancillary=ancillary,
        options=options,
    )


def _resolve_publisher_factory(identifier: str | None) -> PublisherFactory:
    default_factory = cast("PublisherFactory", XClsMakePypiX)
    if identifier is None:
        return default_factory
    cleaned = identifier.strip()
    if not cleaned:
        return default_factory
    if cleaned in {"XClsMakePypiX", "x_cls_make_pypi_x"}:
        return default_factory
    module_name: str
    attr_name: str
    if ":" in cleaned:
        module_name, attr_name = cleaned.split(":", 1)
    elif "." in cleaned:
        module_name, attr_name = cleaned.rsplit(".", 1)
    else:
        module_name, attr_name = "x_make_pypi_x.x_cls_make_pypi_x", cleaned
    module = importlib.import_module(module_name)
    candidate = cast("object", getattr(module, attr_name))
    if not callable(candidate):
        message = f"publisher_factory '{identifier}' did not resolve to a callable"
        raise TypeError(message)
    return cast("PublisherFactory", candidate)


def _build_context(
    ctx: object | None, overrides: Mapping[str, object] | None
) -> object | None:
    if not overrides:
        return ctx
    namespace = SimpleNamespace(**{str(key): value for key, value in overrides.items()})
    if ctx is not None:
        namespace._parent_ctx = ctx  # noqa: SLF001 - preserve legacy attribute
    return namespace


def _validate_input_schema(payload: Mapping[str, object]) -> dict[str, object] | None:
    try:
        validate_payload(payload, INPUT_SCHEMA)
    except ValidationError as exc:
        return _failure_payload(
            "input payload failed validation",
            details={
                "error": exc.message,
                "path": [str(part) for part in exc.path],
                "schema_path": [str(part) for part in exc.schema_path],
            },
        )
    return None


def _parameters_from_payload(payload: Mapping[str, object]) -> Mapping[str, object]:
    parameters_obj = payload.get("parameters")
    if isinstance(parameters_obj, Mapping):
        return MappingProxyType(
            {str(key): value for key, value in parameters_obj.items()}
        )
    return _EMPTY_MAPPING


@dataclass(frozen=True)
class _InputParameters:
    entries: tuple[ManifestEntry, ...]
    repo_root: str
    token_env: str
    publisher_identifier: str | None
    context_overrides: Mapping[str, object] | None


def _entries_from_parameters(
    parameters: Mapping[str, object],
) -> tuple[ManifestEntry, ...]:
    entries_raw = parameters.get("entries")
    manifest_entries: list[ManifestEntry] = []
    if isinstance(entries_raw, Sequence) and not isinstance(
        entries_raw, (str, bytes, bytearray)
    ):
        manifest_entries.extend(
            _entry_from_json(entry_obj)
            for entry_obj in entries_raw
            if isinstance(entry_obj, Mapping)
        )
    return tuple(manifest_entries)


def _extract_inputs(
    parameters: Mapping[str, object],
) -> _InputParameters | dict[str, object]:
    try:
        manifest_entries = _entries_from_parameters(parameters)
    except ValueError as exc:
        return _failure_payload(
            "failed to parse manifest entries",
            details={"error": str(exc)},
        )

    repo_root = _normalize_string(parameters.get("repo_parent_root"))
    if repo_root is None:
        return _failure_payload(
            "repo_parent_root must be a non-empty string",
            details={"field": "repo_parent_root"},
        )

    token_env = _normalize_string(parameters.get("token_env"))
    token_env_effective = token_env or XClsMakePypiX.TEST_PYPI_TOKEN_ENV

    publisher_identifier = _normalize_string(parameters.get("publisher_factory"))

    context_raw = parameters.get("context")
    context_overrides = _mapping_from_object(context_raw)

    return _InputParameters(
        entries=manifest_entries,
        repo_root=repo_root,
        token_env=token_env_effective,
        publisher_identifier=publisher_identifier,
        context_overrides=context_overrides,
    )


def main_json(  # noqa: PLR0911 - routing handles multiple exit paths explicitly
    payload: Mapping[str, object], *, ctx: object | None = None
) -> dict[str, object]:
    schema_failure = _validate_input_schema(payload)
    if schema_failure:
        return schema_failure

    parameters = _parameters_from_payload(payload)
    inputs = _extract_inputs(parameters)
    if isinstance(inputs, dict):
        return inputs

    try:
        publisher_factory = _resolve_publisher_factory(inputs.publisher_identifier)
    except Exception as exc:  # noqa: BLE001
        return _failure_payload(
            "failed to resolve publisher factory",
            details={"error": str(exc)},
        )

    runtime_ctx = _build_context(ctx, inputs.context_overrides)

    cloner = SimpleNamespace(target_dir=inputs.repo_root)

    try:
        _, _, report_path = publish_manifest_entries(
            inputs.entries,
            cloner=cloner,
            ctx=runtime_ctx,
            repo_parent_root=inputs.repo_root,
            publisher_factory=publisher_factory,
            token_env=inputs.token_env,
        )
    except Exception as exc:  # noqa: BLE001
        details: dict[str, object] = {"error": str(exc)}
        report_path_obj = cast("object | None", getattr(exc, "run_report_path", None))
        if isinstance(report_path_obj, Path):
            details["run_report_path"] = str(report_path_obj)
        return _failure_payload("publishing manifest entries failed", details=details)

    try:
        report_text = Path(report_path).read_text(encoding="utf-8")
        result_payload_raw: object = json.loads(report_text)
    except (OSError, json.JSONDecodeError) as exc:
        return _failure_payload(
            "failed to read run report",
            details={
                "error": str(exc),
                "report_path": str(report_path),
            },
        )

    if not isinstance(result_payload_raw, Mapping):
        return _failure_payload(
            "run report did not contain a JSON object",
            details={"report_path": str(report_path)},
        )

    typed_result_mapping = cast("Mapping[str, object]", result_payload_raw)
    result_payload = dict(typed_result_mapping.items())
    errors_obj = result_payload.get("errors")
    if not isinstance(errors_obj, list):
        result_payload["errors"] = cast("list[object]", [])

    outputs: dict[str, object] = dict(result_payload)
    try:
        validate_payload(outputs, OUTPUT_SCHEMA)
    except ValidationError as exc:
        return _failure_payload(
            "generated output failed schema validation",
            details={
                "error": exc.message,
                "path": [str(part) for part in exc.path],
                "schema_path": [str(part) for part in exc.schema_path],
            },
        )
    return outputs


def _load_json_payload(file_path: str | None) -> Mapping[str, object]:
    def _load(stream: IO[str]) -> Mapping[str, object]:
        payload_obj: object = json.load(stream)
        if not isinstance(payload_obj, Mapping):
            message = "JSON payload must be a mapping"
            raise TypeError(message)
        payload_mapping = cast("Mapping[str, object]", payload_obj)
        typed_payload = {str(key): value for key, value in payload_mapping.items()}
        return MappingProxyType(typed_payload)

    if file_path:
        with Path(file_path).open("r", encoding="utf-8") as handle:
            return _load(handle)
    return _load(sys.stdin)


def _run_json_cli(args: Sequence[str]) -> None:
    parser = argparse.ArgumentParser(description="x_make_pypi_x JSON runner")
    parser.add_argument(
        "--json", action="store_true", help="Read JSON payload from stdin"
    )
    parser.add_argument("--json-file", type=str, help="Path to JSON payload file")
    parsed = parser.parse_args(args)

    json_flag_source: object = getattr(parsed, "json", False)
    json_flag = bool(json_flag_source)
    json_file_arg = cast("str | None", getattr(parsed, "json_file", None))

    if not (json_flag or json_file_arg):
        parser.error("JSON input required. Use --json for stdin or --json-file <path>.")

    payload = _load_json_payload(json_file_arg if json_file_arg else None)
    result = main_json(payload)
    json.dump(result, sys.stdout, indent=2)
    sys.stdout.write("\n")


x_cls_make_pypi_x = XClsMakePypiX

__all__ = ["XClsMakePypiX", "main_json", "x_cls_make_pypi_x"]


if __name__ == "__main__":
    _run_json_cli(sys.argv[1:])
