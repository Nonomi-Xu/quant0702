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
        "module": "src.factor.assets.factors.factors.asi_26",
        "function": "compute_asi_26",
        "output_columns": ["asi_26"],
    },
    "asit_26_10": {
        "label": "ASIT平滑振动升降指标",
        "formula": "ASIT(i,t) = mean(ASI(i,t-9), ..., ASI(i,t))",
        "required_fields": ["open_hfq", "high_hfq", "low_hfq", "close_hfq"],
        "module": "src.factor.assets.factors.factors.asit_26_10",
        "function": "compute_asit_26_10",
        "output_columns": ["asit_26_10"]
    },
    "atr_20": {
        "label": "真实波动20日平均值",
        "formula": "ATR(i,t) = mean(TR(i,t-19), ..., TR(i,t))",
        "required_fields": ["high_hfq", "low_hfq", "close_hfq"],
        "module": "src.factor.assets.factors.factors.atr_20",
        "function": "compute_atr_20",
        "output_columns": ["atr_20"]
    },
    "bbi_3_6_12_21": {
        "label": "BBI多空指标",
        "formula": "BBI(i,t) = (MA3 + MA6 + MA12 + MA21) / 4",
        "required_fields": ["close_hfq"],
        "module": "src.factor.assets.factors.factors.bbi_3_6_12_21",
        "function": "compute_bbi_3_6_12_21",
        "output_columns": ["bbi_3_6_12_21"]
    },
        "bias_6": {
        "label": "BIAS6乖离率",
        "formula": "BIAS6(i,t) = ((C(i,t) - MA6(i,t)) / MA6(i,t)) * 100",
        "required_fields": ["close_hfq"],
        "module": "src.factor.assets.factors.factors.bias",
        "function": "compute_bias_6",
        "output_columns": ["bias_6"],
    },
    "bias_12": {
        "label": "BIAS12乖离率",
        "formula": "BIAS12(i,t) = ((C(i,t) - MA12(i,t)) / MA12(i,t)) * 100",
        "required_fields": ["close_hfq"],
        "module": "src.factor.assets.factors.factors.bias",
        "function": "compute_bias_12",
        "output_columns": ["bias_12"],
    },
    "bias_24": {
        "label": "BIAS24乖离率",
        "formula": "BIAS24(i,t) = ((C(i,t) - MA24(i,t)) / MA24(i,t)) * 100",
        "required_fields": ["close_hfq"],
        "module": "src.factor.assets.factors.factors.bias",
        "function": "compute_bias_24",
        "output_columns": ["bias_24"],
    },
    "boll_lower_20_2": {
        "label": "BOLL下轨",
        "formula": "BOLL_LOWER(i,t) = MA20(i,t) - 2 * STD20(i,t)",
        "required_fields": ["close_hfq"],
        "module": "src.factor.assets.factors.boll",
        "function": "compute_boll_lower_20_2",
        "output_columns": ["boll_lower_20_2"],
    },
    "boll_mid_20_2": {
        "label": "BOLL中轨",
        "formula": "BOLL_MID(i,t) = MA20(i,t)",
        "required_fields": ["close_hfq"],
        "module": "src.factor.assets.factors.boll",
        "function": "compute_boll_mid_20_2",
        "output_columns": ["boll_mid_20_2"],
    },
    "boll_upper_20_2": {
        "label": "BOLL上轨",
        "formula": "BOLL_UPPER(i,t) = MA20(i,t) + 2 * STD20(i,t)",
        "required_fields": ["close_hfq"],
        "module": "src.factor.assets.factors.boll",
        "function": "compute_boll_upper_20_2",
        "output_columns": ["boll_upper_20_2"],
    },
}
