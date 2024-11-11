import json
import os
import shutil
import subprocess
import sys
import zipfile
from importlib import import_module
from pathlib import Path
from typing import List, Mapping, Optional, Sequence

import click
from dagster import _check as check
from dagster._cli.workspace.cli_target import has_pyproject_dagster_block
from dagster._core.remote_representation.origin import ManagedGrpcPythonEnvCodeLocationOrigin
from dagster._core.workspace.load_target import PyProjectFileTarget
from dagster._utils.warnings import disable_dagster_warnings

from dagster_blueprints.load_from_yaml import YamlBlueprintsLoader

from .version import __version__


def infer_vscode_path(base_path: Path) -> Optional[str]:
    """Utility which attempts to find the VS Code workspace root folder by looking for a `.vscode` directory in the current
    directory or any of its parents.
    """
    while base_path != base_path.parent:
        if (base_path / ".vscode").exists():
            return os.fspath(base_path)
        base_path = base_path.parent
    return None


def get_python_modules_from_pyproject(pyproject_path: str) -> list[str]:
    """Utility to get the Python modules from a `pyproject.toml` file."""
    origins = PyProjectFileTarget(pyproject_path).create_origins()

    modules = []
    for origin in origins:
        if isinstance(origin, ManagedGrpcPythonEnvCodeLocationOrigin):
            module = origin.loadable_target_origin.module_name
            if module:
                modules.append(module)
    return modules


def generate_schema_file_for_loader(loader: YamlBlueprintsLoader) -> Path:
    """Generates a schema file for the provided YamlBlueprintsLoader and
    writes it to the same directory as the loader.
    """
    schema_path = Path(loader.path) / "dagster.autogenerated.schema.json"
    schema_path.write_text(json.dumps(loader.model_json_schema(), indent=2))
    return schema_path


@click.command(
    help="Generates JSON schema files for Blueprint types specified by YamlBlueprintsLoader objects."
)
@click.option(
    "--loader-module",
    type=click.STRING,
    help="Path of Python module that contains YamlBlueprintsLoader objects. Defaults to Dagster project module, if `pyproject.toml` exists.",
)
@click.option(
    "--loader-name",
    type=click.STRING,
    help="Name of the YamlBlueprintsLoader object to generate a schema for. Required if the specified module contains multiple loaders.",
)
@click.option(
    "--pretty",
    "-p",
    is_flag=True,
    help="Whether to pretty-print the generated schema.",
    default=True,
)
def print_schema(loader_module: Optional[str], loader_name: Optional[str], pretty: bool) -> None:
    loaders: Mapping[str, YamlBlueprintsLoader] = load_blueprints_loaders_from_module_path_or_infer(
        loader_module
    )

    check.invariant(
        len(loaders) > 0, "No YamlBlueprintsLoader objects found in the provided module."
    )
    check.invariant(
        loader_name or len(loaders) == 1,
        "Must provide a loader name since the specified module contains multiple lodaers.",
    )

    check.invariant(
        loader_name is None or loader_name in loaders,
        f"Loader name {loader_name} not found in the provided module.",
    )

    loader = loaders[loader_name] if loader_name else next(iter(loaders.values()))
    click.echo(json.dumps(loader.model_json_schema(), indent=2 if pretty else None))


@click.command(
    help="Generates and installs a VS Code extension which provides JSON schemas for Blueprint types specified by YamlBlueprintsLoader objects."
)
@click.option(
    "--loader-module",
    type=click.STRING,
    help="Path of Python module that contains YamlBlueprintsLoader objects. Defaults to Dagster project module, if `pyproject.toml` exists.",
)
@click.option(
    "--vscode-folder-path",
    type=click.STRING,
    help="Path to the VS Code workspace root folder. If not provided, attempts to find the .vscode directory in the current directory or any of its parents.",
)
def configure_vscode(loader_module: Optional[str] = None, vscode_folder_path: Optional[str] = None):
    loaders = load_blueprints_loaders_from_module_path_or_infer(loader_module)

    if not vscode_folder_path:
        vscode_folder_path = check.not_none(
            infer_vscode_path(Path.cwd()),
            "Could not find a .vscode directory in the current directory or any of its parents.",
        )

    check.invariant(len(loaders) > 0, "No YamlBlueprintsLoader objects found in the module")

    vscode_folder_path_absolute = Path(vscode_folder_path).absolute()
    dot_vscode_path = vscode_folder_path_absolute / ".vscode"
    check.invariant(
        dot_vscode_path.exists(),
        f"Could not find a .vscode directory in {dir} or any of its parents.",
    )

    recommend_yaml_extension()

    schema_paths = []
    for loader in loaders.values():
        schema_path = generate_schema_file_for_loader(loader)
        click.echo(f"Wrote schema for {loader.per_file_blueprint_type} to {schema_path}")
        schema_paths.append(schema_path)

    loader_paths = [Path(loader.path) for loader in loaders.values()]
    install_yaml_schema_extension(dot_vscode_path, loader_paths, schema_paths)


def load_blueprints_loaders_from_module_path_or_infer(
    module_path: Optional[str],
) -> Mapping[str, YamlBlueprintsLoader]:
    """Loads YamlBlueprintsLoader objects from the provided module path, or infers the module path from the current
    directory's `pyproject.toml` file. If no module path is provided and no `pyproject.toml` file is found, raises an
    error.
    """
    with disable_dagster_warnings():
        if module_path:
            return load_blueprints_loaders_from_module_path(module_path)
        else:
            check.invariant(
                has_pyproject_dagster_block("pyproject.toml"),
                "No `pyproject.toml` found in the current directory, or no `tool.dagster` block found in `pyproject.toml`.",
            )
            return {
                loader_name: loader
                for module in get_python_modules_from_pyproject("pyproject.toml")
                for loader_name, loader in load_blueprints_loaders_from_module_path(module).items()
            }


def load_blueprints_loaders_from_module_path(
    module_path: str,
) -> Mapping[str, YamlBlueprintsLoader]:
    sys.path.append(".")

    module = import_module(module_path)

    out = {}
    for attr in dir(module):
        value = getattr(module, attr)
        if isinstance(value, YamlBlueprintsLoader):
            out = {**out, attr: value}
    return out


def has_vscode_cli_command() -> bool:
    return bool(shutil.which("code"))


def run_vscode_cli_command(args: list[str]) -> bytes:
    return subprocess.check_output(["code"] + args)


def recommend_yaml_extension() -> None:
    if not has_vscode_cli_command():
        click.echo(
            "Could not find `code` executable in PATH. In order to use the dagster-blueprint-schema extension, "
            "please install the redhat.vscode-yaml extension manually."
        )
        return

    click.echo("Checking whether redhat.vscode-yaml extension is installed.")
    extensions = run_vscode_cli_command(["--list-extensions"]).decode("utf-8").split("\n")
    if "redhat.vscode-yaml" in extensions:
        click.echo("redhat.vscode-yaml extension is already installed.")
    else:
        if click.confirm(
            "The redhat.vscode-yaml extension is not installed. Would you like to install it now?"
        ):
            run_vscode_cli_command(["--install-extension", "redhat.vscode-yaml"])


def install_yaml_schema_extension(
    dot_vscode_path: Path, yaml_dirs: Sequence[Path], schema_paths: Sequence[Path]
) -> None:
    """Builds a VS Code extension which associates the built JSON schema files with YAML
    files in the provided directory, provided that the user has the Red Hat YAML extension
    already installed.
    """
    extension_working_dir = dot_vscode_path / "dagster-blueprint-schema"
    extension_package_json_path = extension_working_dir / "package.json"

    template_package_json_path = Path(__file__).parent / "vscode_extension_package.json"
    template_package_json = json.loads(template_package_json_path.read_text())
    template_package_json["contributes"]["yamlValidation"] = [
        {"fileMatch": f"{yaml_dir}/**/*.y*ml", "url": f"{schema_path}"}
        for yaml_dir, schema_path in zip(yaml_dirs, schema_paths)
    ]

    extension_working_dir.mkdir(parents=True, exist_ok=True)
    extension_package_json_path.write_text(json.dumps(template_package_json, indent=2))
    click.echo(f"Set up package.json for VS Code extension in {extension_package_json_path}")

    extension_zip_path = extension_working_dir / "dagster-blueprint-schema.vsix"
    with zipfile.ZipFile(extension_zip_path, "w") as z:
        z.write(extension_package_json_path, "extension/package.json")

    click.echo(f"Packaged extension to {extension_zip_path}")

    try:
        run_vscode_cli_command(["--uninstall-extension", "dagster.dagster-blueprint-schema"])
    except subprocess.CalledProcessError:
        click.echo("No existing dagster.dagster-blueprint-schema extension to uninstall.")
    run_vscode_cli_command(["--install-extension", os.fspath(extension_zip_path.resolve())])
    click.echo("Successfully installed Dagster Blueprint schema extension.")


def main():
    @click.group(
        commands=[configure_vscode, print_schema],
        context_settings={"max_content_width": 120, "help_option_names": ["-h", "--help"]},
    )
    @click.version_option(__version__, "--version", "-v")
    def group():
        """CLI tools for working with Dagster Blueprints."""

    return group()
