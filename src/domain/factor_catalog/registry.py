from __future__ import annotations

import ast
from importlib import import_module
from pathlib import Path


LEGACY_FORMULA_PREFIX = "src.factor.assets.factors.factors."
DOMAIN_FORMULA_PREFIX = "src.domain.factor_formulas."


def normalize_module_name(module_name: str) -> str:
    if module_name.startswith(DOMAIN_FORMULA_PREFIX):
        return module_name
    if module_name.startswith(LEGACY_FORMULA_PREFIX):
        return module_name.replace(LEGACY_FORMULA_PREFIX, DOMAIN_FORMULA_PREFIX, 1)
    return module_name


def load_factor_function(module_name: str, function_name: str):
    module = import_module(normalize_module_name(module_name))
    return getattr(module, function_name)


def get_factor_category(factor_name: str) -> str:
    module_name = normalize_module_name(FACTOR_LIST[factor_name]["module"])
    marker = f"{DOMAIN_FORMULA_PREFIX}"
    if marker not in module_name:
        raise ValueError(f"无法从 module 路径解析因子分类: {module_name}")
    return module_name.split(marker, maxsplit=1)[1].split(".", maxsplit=1)[0]


_LEGACY_REGISTRY_PATH = Path(__file__).with_name("legacy_registry_snapshot.py")


def _load_registry_mapping(name: str):
    module = ast.parse(_LEGACY_REGISTRY_PATH.read_text(encoding="utf-8"))
    for node in module.body:
        if isinstance(node, ast.Assign) and any(getattr(target, "id", None) == name for target in node.targets):
            return ast.literal_eval(node.value)
    raise ValueError(f"{name} not found in {_LEGACY_REGISTRY_PATH}")


FACTOR_LIST = _load_registry_mapping("FACTOR_LIST")
FACTOR_PARAMETERS = _load_registry_mapping("FACTOR_PARAMETERS")


__all__ = [
    "DOMAIN_FORMULA_PREFIX",
    "FACTOR_LIST",
    "FACTOR_PARAMETERS",
    "LEGACY_FORMULA_PREFIX",
    "get_factor_category",
    "load_factor_function",
    "normalize_module_name",
]
