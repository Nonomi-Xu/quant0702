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
    "brar_ar_26": {
        "label": "BRAR人气指标AR",
        "formula": "AR(i,t) = sum(H(i,t-k)-O(i,t-k), k=0..25) / sum(O(i,t-k)-L(i,t-k), k=0..25) * 100",
        "required_fields": ["open_hfq", "high_hfq", "low_hfq", "close_hfq"],
        "module": "src.factor.assets.factors.factors.brar_ar_26",
        "function": "compute_brar_ar_26",
        "output_columns": ["brar_ar_26"],
    },
    "brar_br_26": {
        "label": "BRAR意愿指标BR",
        "formula": "BR(i,t) = sum(max(0,H(i,t-k)-C(i,t-k-1)), k=0..25) / sum(max(0,C(i,t-k-1)-L(i,t-k)), k=0..25) * 100",
        "required_fields": ["open_hfq", "high_hfq", "low_hfq", "close_hfq"],
        "module": "src.factor.assets.factors.factors.brar_br_26",
        "function": "compute_brar_br_26",
        "output_columns": ["brar_br_26"],
    },
    "boll_lower_20_2": {
        "label": "BOLL下轨",
        "formula": "BOLL_LOWER(i,t) = MA20(i,t) - 2 * STD20(i,t)",
        "required_fields": ["close_hfq"],
        "module": "src.factor.assets.factors.factors.boll_lower_20_2",
        "function": "compute_boll_lower_20_2",
        "output_columns": ["boll_lower_20_2"],
    },
    "boll_mid_20_2": {
        "label": "BOLL中轨",
        "formula": "BOLL_MID(i,t) = MA20(i,t)",
        "required_fields": ["close_hfq"],
        "module": "src.factor.assets.factors.factors.boll_mid_20_2",
        "function": "compute_boll_mid_20_2",
        "output_columns": ["boll_mid_20_2"],
    },
    "boll_upper_20_2": {
        "label": "BOLL上轨",
        "formula": "BOLL_UPPER(i,t) = MA20(i,t) + 2 * STD20(i,t)",
        "required_fields": ["close_hfq"],
        "module": "src.factor.assets.factors.factors.boll_upper_20_2",
        "function": "compute_boll_upper_20_2",
        "output_columns": ["boll_upper_20_2"],
    },
    "bias_6": {
        "label": "BIAS6乖离率",
        "formula": "BIAS6(i,t) = ((C(i,t) - MA6(i,t)) / MA6(i,t)) * 100",
        "required_fields": ["close_hfq"],
        "module": "src.factor.assets.factors.factors.bias_6",
        "function": "compute_bias_6",
        "output_columns": ["bias_6"],
    },
    "bias_12": {
        "label": "BIAS12乖离率",
        "formula": "BIAS12(i,t) = ((C(i,t) - MA12(i,t)) / MA12(i,t)) * 100",
        "required_fields": ["close_hfq"],
        "module": "src.factor.assets.factors.factors.bias_12",
        "function": "compute_bias_12",
        "output_columns": ["bias_12"],
    },
    "bias_24": {
        "label": "BIAS24乖离率",
        "formula": "BIAS24(i,t) = ((C(i,t) - MA24(i,t)) / MA24(i,t)) * 100",
        "required_fields": ["close_hfq"],
        "module": "src.factor.assets.factors.factors.bias_24",
        "function": "compute_bias_24",
        "output_columns": ["bias_24"],
    },
    "cci_14": {
        "label": "CCI顺势指标",
        "formula": "CCI(i,t) = (TP(i,t) - MA14(TP,i,t)) / (0.015 * MD14(TP,i,t))",
        "required_fields": ["close_hfq", "high_hfq", "low_hfq"],
        "module": "src.factor.assets.factors.factors.cci_14",
        "function": "compute_cci_14",
        "output_columns": ["cci_14"],
    },
    "cr_20": {
        "label": "CR价格动量指标",
        "formula": "CR(i,t) = sum(max(0,H(i,t-k)-REF(MID,i,t-k,1)), k=0..19) / sum(max(0,REF(MID,i,t-k,1)-L(i,t-k)), k=0..19) * 100",
        "required_fields": ["close_hfq", "high_hfq", "low_hfq"],
        "module": "src.factor.assets.factors.factors.cr_20",
        "function": "compute_cr_20",
        "output_columns": ["cr_20"],
    },
    "dfma_dif_10_50": {
        "label": "DFMA平行线差指标DIF",
        "formula": "DFMA_DIF(i,t) = MA10(C,i,t) - MA50(C,i,t)",
        "required_fields": ["close_hfq"],
        "module": "src.factor.assets.factors.factors.dfma_dif_10_50",
        "function": "compute_dfma_dif_10_50",
        "output_columns": ["dfma_dif_10_50"],
    },
    "dfma_difma_10_50_10": {
        "label": "DFMA平行线差指标DIFMA",
        "formula": "DFMA_DIFMA(i,t) = MA10(DFMA_DIF,i,t)",
        "required_fields": ["close_hfq"],
        "module": "src.factor.assets.factors.factors.dfma_difma_10_50_10",
        "function": "compute_dfma_difma_10_50_10",
        "output_columns": ["dfma_difma_10_50_10"],
    },
}
