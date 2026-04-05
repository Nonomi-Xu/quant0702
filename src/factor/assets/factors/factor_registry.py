from __future__ import annotations

factor = [
    "asi_26",
    "asit_26_10",
]

FACTOR_SPECS = {
    "asi_26": {
        "label": "ASI振动升降指标",
        "formula": "ASI(i,t) = sum(SI(i,t-25), ..., SI(i,t))",
        "tushare_fields": ["open_hfq", "high_hfq", "low_hfq", "close_hfq"],
        "required_source_fields": ["open", "high", "low", "close"],
    },
    "asit_26_10": {
        "label": "ASIT平滑振动升降指标",
        "formula": "ASIT(i,t) = mean(ASI(i,t-9), ..., ASI(i,t))",
        "tushare_fields": ["open_hfq", "high_hfq", "low_hfq", "close_hfq"],
        "required_source_fields": ["open", "high", "low", "close"],
    },
}
