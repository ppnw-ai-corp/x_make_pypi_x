from __future__ import annotations

import importlib
import json
import os
import sys
import uuid
from collections.abc import Callable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from types import ModuleType, SimpleNamespace
from typing import TYPE_CHECKING, NoReturn, Protocol, cast

from x_make_common_x.json_contracts import validate_payload

from x_make_pypi_x import publish_flow
from x_make_pypi_x.json_contracts import ERROR_SCHEMA, OUTPUT_SCHEMA
from x_make_pypi_x.x_cls_make_pypi_x import main_json

_SIMPLE_NAMESPACE_TYPE = type(SimpleNamespace())

if TYPE_CHECKING:
    from pathlib import Path

    from x_make_pypi_x.publish_flow import PublisherFactory
else:

    class PublisherFactory(Protocol):
        def __call__(self, *args: object, **kwargs: object) -> object: ...


class SupportsMonkeyPatch(Protocol):
    def setitem(
        self,
        mapping: MutableMapping[str, object],
        key: str,
        value: object,
    ) -> None: ...

    def setattr(
        self,
        obj: object,
        name: str,
        value: object,
        *,
        raising: bool = ...,
    ) -> None: ...

    def delenv(self, name: str, *, raising: bool = ...) -> None: ...

    def setenv(self, name: str, value: str) -> None: ...


pypi_module = importlib.import_module("x_make_pypi_x.x_cls_make_pypi_x")


def _raise_failure(message: str) -> NoReturn:
    failure_message = message
    raise AssertionError(failure_message)


def expect(*, condition: bool, message: str) -> None:
    if not condition:
        _raise_failure(message)


_PrimeCredentials = Callable[[str], str]


def _invoke_prime_twine_credentials(token_env: str) -> str:
    prime_callable = cast(
        "_PrimeCredentials",
        publish_flow._prime_twine_credentials,  # noqa: SLF001
    )
    return prime_callable(token_env)


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _run_report_payload(repo_root: Path) -> dict[str, object]:
    return {
        "run_id": "0123456789abcdef0123456789abcdef",
        "started_at": _iso(datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)),
        "inputs": {
            "entry_count": 1,
            "manifest_entries": [
                {
                    "package": "demo_pkg",
                    "version": "1.2.3",
                    "pypi_name": "demo_pkg",
                    "ancillary": ["README.md"],
                    "options_kwargs": {"force_publish": True},
                }
            ],
            "repo_parent_root": str(repo_root),
            "token_env": "CUSTOM_ENV",
        },
        "execution": {"publisher_factory": "FakePublisher"},
        "result": {
            "status": "completed",
            "entries": [
                {
                    "package": "demo_pkg",
                    "distribution": "demo_pkg",
                    "version": "1.2.3",
                    "main_file": "x_cls_make_demo_pkg.py",
                    "ancillary_publish": ["README.md"],
                    "ancillary_manifest": ["README.md"],
                    "package_dir": f"{repo_root}/demo_pkg",
                    "safe_kwargs": {"force_publish": True},
                    "status": "published",
                }
            ],
            "published_versions": {"demo_pkg": "1.2.3"},
            "published_artifacts": {
                "demo_pkg": {"main": "x_cls_make_demo_pkg.py", "anc": ["README.md"]}
            },
        },
        "status": "completed",
        "completed_at": _iso(datetime(2025, 1, 1, 12, 5, 0, tzinfo=UTC)),
        "duration_seconds": 300.0,
        "tool": "x_make_pypi_x",
        "generated_at": _iso(datetime(2025, 1, 1, 12, 5, 0, tzinfo=UTC)),
        "errors": [],
    }


def _install_fake_publisher(monkeypatch: SupportsMonkeyPatch, module_name: str) -> None:
    fake_module = ModuleType(module_name)

    class FakePublisher:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.args = args
            self.kwargs = kwargs

        def publish(self, main_rel_path: str, ancillary_rel_paths: list[str]) -> bool:
            self.main_path = main_rel_path
            self.ancillary = ancillary_rel_paths
            return True

    fake_module.FakePublisher = FakePublisher  # type: ignore[attr-defined]
    monkeypatch.setitem(
        cast("MutableMapping[str, object]", sys.modules),
        module_name,
        fake_module,
    )


def _payload(template_repo_root: Path, publisher_identifier: str) -> dict[str, object]:
    return {
        "command": "x_make_pypi_x",
        "parameters": {
            "entries": [
                {
                    "package": "demo_pkg",
                    "version": "1.2.3",
                    "ancillary": ["README.md"],
                    "options": {
                        "author": "Author",
                        "dependencies": ["requests>=2"],
                        "ancillary_allowlist": ["docs/list.txt"],
                        "extra": {"force_publish": True},
                    },
                }
            ],
            "repo_parent_root": str(template_repo_root),
            "token_env": "CUSTOM_ENV",
            "context": {"dry_run": True},
            "publisher_factory": publisher_identifier,
        },
    }


@dataclass
class _PublishCall:
    entries: Sequence[object]
    cloner: object
    ctx: object | None
    repo_parent_root: str
    publisher_factory: PublisherFactory
    token_env: str


def _assert_publish_call(
    captured: _PublishCall,
    result: Mapping[str, object],
    repo_root: Path,
    expected_token_env: str,
) -> None:
    entries = list(captured.entries)
    expect(condition=bool(entries), message="No manifest entries captured")
    first_entry = entries[0]
    package_attr: object = getattr(first_entry, "package", None)
    expect(
        condition=isinstance(package_attr, str),
        message="Captured entry missing package attribute",
    )
    package_name = cast("str", package_attr)
    expect(condition=package_name == "demo_pkg", message="Unexpected package name")

    ctx = captured.ctx
    expect(
        condition=isinstance(ctx, _SIMPLE_NAMESPACE_TYPE),
        message="Context should be a SimpleNamespace",
    )
    ctx_namespace = cast("SimpleNamespace", ctx)
    dry_run_attr: object = getattr(ctx_namespace, "dry_run", False)
    expect(
        condition=isinstance(dry_run_attr, bool),
        message="Context dry_run flag must be boolean",
    )
    expect(
        condition=cast("bool", dry_run_attr) is True,
        message="Context missing dry_run flag",
    )
    expect(
        condition=isinstance(captured.cloner, _SIMPLE_NAMESPACE_TYPE),
        message="Cloner should be a SimpleNamespace",
    )
    expect(
        condition=captured.repo_parent_root == str(repo_root),
        message="Incorrect repo parent root",
    )
    expect(
        condition=captured.token_env == expected_token_env,
        message="Unexpected token env value",
    )

    name_attr: object = getattr(captured.publisher_factory, "__name__", "")
    expect(
        condition=isinstance(name_attr, str) and name_attr == "FakePublisher",
        message="Publisher factory override not applied",
    )

    status_value = result.get("status")
    expect(condition=isinstance(status_value, str), message="Status must be a string")
    expect(condition=status_value == "completed", message="Expected completed status")


def test_main_json_success(monkeypatch: SupportsMonkeyPatch, tmp_path: Path) -> None:
    module_name = "tests.fake_publisher"
    _install_fake_publisher(monkeypatch, module_name)

    captured_call: _PublishCall | None = None

    def fake_publish(
        entries: Sequence[object],
        **kwargs: object,
    ) -> tuple[dict[str, str | None], dict[str, dict[str, object]], Path]:
        nonlocal captured_call
        cloner = kwargs.get("cloner")
        ctx = kwargs.get("ctx")
        repo_parent_root_obj = kwargs.get("repo_parent_root")
        publisher_factory_obj = kwargs.get("publisher_factory")
        token_env_obj = kwargs.get("token_env")
        expect(
            condition=isinstance(repo_parent_root_obj, str),
            message="repo_parent_root missing",
        )
        expect(
            condition=isinstance(token_env_obj, str),
            message="token_env missing",
        )
        expect(
            condition=callable(publisher_factory_obj),
            message="publisher_factory missing",
        )
        repo_parent_root = cast("str", repo_parent_root_obj)
        token_env = cast("str", token_env_obj)
        publisher_factory = cast("PublisherFactory", publisher_factory_obj)
        captured_call = _PublishCall(
            entries=tuple(entries),
            cloner=cloner,
            ctx=ctx,
            repo_parent_root=repo_parent_root,
            publisher_factory=publisher_factory,
            token_env=token_env,
        )

        report_payload = _run_report_payload(tmp_path)
        report_path = tmp_path / "reports" / "x_make_pypi_x_run_test.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report_payload), encoding="utf-8")
        versions: dict[str, str | None] = {"demo_pkg": "1.2.3"}
        artifacts: dict[str, dict[str, object]] = {
            "demo_pkg": {"main": "x_cls_make_demo_pkg.py", "anc": ["README.md"]}
        }
        return versions, artifacts, report_path

    monkeypatch.setattr(pypi_module, "publish_manifest_entries", fake_publish)

    payload = _payload(tmp_path, f"{module_name}:FakePublisher")
    result = main_json(payload)

    validate_payload(result, OUTPUT_SCHEMA)

    if captured_call is None:
        _raise_failure("publish_manifest_entries was not invoked")

    parameters_obj = cast("Mapping[str, object]", payload["parameters"])
    expected_token_env = cast("str", parameters_obj["token_env"])
    _assert_publish_call(
        captured_call,
        result,
        tmp_path,
        expected_token_env,
    )


def test_main_json_publish_failure(
    monkeypatch: SupportsMonkeyPatch, tmp_path: Path
) -> None:
    def failing_publish(
        *_args: object, **_kwargs: object
    ) -> tuple[dict[str, str | None], dict[str, dict[str, object]], Path]:
        report_path = tmp_path / "reports" / "failed.json"
        exc = RuntimeError("publish boom")
        exc.run_report_path = report_path  # type: ignore[attr-defined]
        raise exc

    monkeypatch.setattr(pypi_module, "publish_manifest_entries", failing_publish)

    payload = _payload(tmp_path, "XClsMakePypiX")
    result = main_json(payload)

    validate_payload(result, ERROR_SCHEMA)

    details_obj = result.get("details")
    expect(
        condition=isinstance(details_obj, Mapping),
        message="Failure details missing mapping payload",
    )
    details_mapping = cast("Mapping[str, object]", details_obj)
    expect(
        condition="run_report_path" in details_mapping,
        message="Missing run report path in failure details",
    )


def test_main_json_rejects_invalid_payload() -> None:
    result = main_json({})
    validate_payload(result, ERROR_SCHEMA)
    status_value = result.get("status")
    expect(condition=isinstance(status_value, str), message="Status must be a string")
    expect(condition=status_value == "failure", message="Invalid payload should fail")


def test_prime_twine_credentials_sets_username_and_password(
    monkeypatch: SupportsMonkeyPatch,
) -> None:
    token_value = uuid.uuid4().hex
    monkeypatch.delenv("TWINE_API_TOKEN", raising=False)
    monkeypatch.delenv("TWINE_USERNAME", raising=False)
    monkeypatch.delenv("TWINE_PASSWORD", raising=False)
    custom_env = "CUSTOM_TOKEN_ENV"
    monkeypatch.setenv(custom_env, token_value)

    selected = _invoke_prime_twine_credentials(custom_env)

    expect(
        condition=selected == custom_env,
        message="Custom token environment should be selected",
    )
    expect(
        condition=os.environ["TWINE_API_TOKEN"] == token_value,
        message="Token environment should copy the token value",
    )
    expect(
        condition=os.environ["TWINE_USERNAME"] == "__token__",
        message="Twine username should default to __token__",
    )
    expect(
        condition=os.environ["TWINE_PASSWORD"] == token_value,
        message="Twine password should mirror the token value",
    )


def test_prime_twine_credentials_preserves_existing_user(
    monkeypatch: SupportsMonkeyPatch,
) -> None:
    existing_token = uuid.uuid4().hex
    existing_password = uuid.uuid4().hex
    monkeypatch.setenv("TWINE_API_TOKEN", existing_token)
    monkeypatch.setenv("TWINE_USERNAME", "custom-user")
    monkeypatch.setenv("TWINE_PASSWORD", existing_password)

    selected = _invoke_prime_twine_credentials("")

    expect(
        condition=selected == "TWINE_API_TOKEN",
        message="Existing TWINE_API_TOKEN should remain preferred",
    )
    expect(
        condition=os.environ["TWINE_API_TOKEN"] == existing_token,
        message="Existing token should remain unchanged",
    )
    expect(
        condition=os.environ["TWINE_USERNAME"] == "__token__",
        message="Twine username should still default to __token__",
    )
    expect(
        condition=os.environ["TWINE_PASSWORD"] == existing_token,
        message="Twine password should mirror the token when present",
    )
