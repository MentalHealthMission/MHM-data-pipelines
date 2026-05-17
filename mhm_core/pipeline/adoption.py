"""Contracts for adopting reusable MHM modules inside project profiles."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Tuple


@dataclass(frozen=True)
class PipelineModuleContract:
    """Neutral contract exposed by a reusable MHM module."""

    module_id: str
    display_name: str = ""
    capabilities: Tuple[str, ...] = field(default_factory=tuple)
    required_inputs: Tuple[str, ...] = field(default_factory=tuple)
    produced_outputs: Tuple[str, ...] = field(default_factory=tuple)
    config_keys: Tuple[str, ...] = field(default_factory=tuple)
    optional_dependencies: Tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class PipelineModuleBinding:
    """Project-specific binding from local conventions to a module contract."""

    module_id: str
    project_id: str
    profile_id: str
    input_bindings: Mapping[str, str] = field(default_factory=dict)
    output_bindings: Mapping[str, str] = field(default_factory=dict)
    config_bindings: Mapping[str, str] = field(default_factory=dict)
    asset_bindings: Mapping[str, str] = field(default_factory=dict)
    provenance_bindings: Tuple[str, ...] = field(default_factory=tuple)
    deployment_bindings: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ModuleAdoptionBridge:
    """Pair a neutral MHM module contract with one project binding."""

    contract: PipelineModuleContract
    binding: PipelineModuleBinding

    def validate(self) -> list[str]:
        return validate_module_adoption_bridge(self.contract, self.binding)


def validate_module_adoption_bridge(
    contract: PipelineModuleContract,
    binding: PipelineModuleBinding,
) -> list[str]:
    """Return human-readable adoption-bridge validation errors."""

    errors: list[str] = []
    if not contract.module_id.strip():
        errors.append("module contract must define module_id")
    if not binding.module_id.strip():
        errors.append("module binding must define module_id")
    if contract.module_id and binding.module_id and contract.module_id != binding.module_id:
        errors.append(
            f"module binding '{binding.module_id}' does not match contract '{contract.module_id}'"
        )
    if not binding.project_id.strip():
        errors.append("module binding must define project_id")
    if not binding.profile_id.strip():
        errors.append("module binding must define profile_id")

    missing_inputs = [
        name
        for name in contract.required_inputs
        if not str(binding.input_bindings.get(name, "")).strip()
    ]
    if missing_inputs:
        errors.append(f"module binding missing required inputs: {sorted(missing_inputs)}")

    missing_outputs = [
        name
        for name in contract.produced_outputs
        if not str(binding.output_bindings.get(name, "")).strip()
    ]
    if missing_outputs:
        errors.append(f"module binding missing produced outputs: {sorted(missing_outputs)}")

    missing_config = [
        name
        for name in contract.config_keys
        if not str(binding.config_bindings.get(name, "")).strip()
    ]
    if missing_config:
        errors.append(f"module binding missing config keys: {sorted(missing_config)}")

    return errors


__all__ = [
    "ModuleAdoptionBridge",
    "PipelineModuleBinding",
    "PipelineModuleContract",
    "validate_module_adoption_bridge",
]
