from __future__ import annotations

from importlib import import_module

def load_factor_function(module_name: str, function_name: str):
    module = import_module(module_name)
    func = getattr(module, function_name)
    return func


FACTOR_LIST = {
    "asi_26": {
        "label": "ASI振动升降指标",
        "formula": "ASI(i,t) = sum(SI(i,t-25), ..., SI(i,t))",
        "required_fields": ["open_hfq", "high_hfq", "low_hfq", "close_hfq"],
        "module": "src.factor.assets.factors.trend_factors.asi_26",
        "function": "compute_asi_26",
        "output_columns": ["asi_26"],
    },
    "asit_26_10": {
        "label": "ASIT平滑振动升降指标",
        "formula": "ASIT(i,t) = mean(ASI(i,t-9), ..., ASI(i,t))",
        "required_fields": ["open_hfq", "high_hfq", "low_hfq", "close_hfq"],
        "module": "src.factor.assets.factors.trend_factors.asi_26_10",
        "function": "compute_asi_26_10",
        "output_columns": ["asit_26_10"]
    },
}
