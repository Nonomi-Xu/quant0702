# A-Stock Quant Pipeline

面向 A 股市场的数据摄取、因子计算和因子评估流水线。项目使用 Dagster 组织每日任务，使用 Polars 进行表计算，使用 Parquet/DuckDB 作为分析存储层，并支持将数据缓存到本地、同步到腾讯云 COS。

## 项目能力

- 每日拉取 A 股基础数据、行情、涨跌停、ST 状态、资金流、复权因子、指数列表等数据。
- 生成可研究股票池、因子源数据、横截面因子输入和 K 线形态因子输入。
- 维护技术指标、价量、风险、情绪、K 线形态等因子公式目录。
- 对横截面因子生成 IC、分组收益、多空收益、覆盖率等评估结果。
- 对 K 线形态因子生成事件收益、命中率和样本覆盖监控结果。
- 通过 Dagster UI 查看资产依赖、手动 materialize、运行 job 或排查失败日志。

## 目录结构

```text
.
├── definitions.py                         # Dagster definitions 入口
├── workspace.yaml                         # Dagster workspace 配置
├── pyproject.toml                         # 项目依赖与 dg 配置
├── resources/
│   ├── duckdb_io.py                       # DuckDB 本地缓存与 COS 同步资源
│   ├── parquet_io.py                      # Parquet 读写、本地缓存与 COS 同步资源
│   └── tushare_io.py                      # Tushare API 封装
├── scripts/
│   ├── export_factor_registry_metadata.py # 导出因子注册表元数据
│   └── manage.sh                          # 服务器 systemd 管理脚本
└── src/
    ├── data_ingestion/                    # 每日数据摄取 assets/jobs/schedules
    ├── orchestration/                     # Dagster 编排层
    ├── domain/
    │   ├── factor_analysis/               # 横截面因子评估流程
    │   ├── factor_catalog/                # 横截面因子注册表
    │   ├── factor_formulas/               # 横截面因子公式
    │   ├── pattern_factor_analysis/       # K 线形态因子评估流程
    │   ├── pattern_factor_catalog/        # K 线形态因子注册表
    │   └── pattern_factor_formulas/       # K 线形态公式
    └── shared/                            # 交易日、日期和通用校验工具
```

## 数据流

```text
Tushare / AkShare
    -> data_ingestion assets
    -> COS Parquet / DuckDB local cache
    -> factor_source
    -> factor_input / pattern_factor_input
    -> Factor_Analysis / Pattern_Factor_Analysis
    -> factor/analysis / factor/pattern_analysis
```

核心 Dagster 入口是 `definitions.py`，它合并三组定义：

- `Data_Ingestion_Daily_Job`: 每日刷新 A 股基础数据并计算因子输入。
- `Factor_Analysis_Job`: 分析横截面因子表现。
- `Pattern_Factor_Analysis_Job`: 分析 K 线形态因子的事件收益与命中率。

## 环境准备

项目要求 Python `>=3.10,<3.15`。推荐使用 `uv`，也可以用普通 `pip`。

### 使用 uv

```bash
uv sync
source .venv/bin/activate
```

### 使用 pip

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

如果运行时报 `qcloud_cos` 缺失，需要安装腾讯云 COS SDK：

```bash
pip install cos-python-sdk-v5
```

## 环境变量

运行数据摄取或云端读写前，需要配置数据源和 COS 相关变量。

```bash
export TUSHARE_TOKEN="your_tushare_token"

export COS_SECRET_ID="your_cos_secret_id"
export COS_SECRET_KEY="your_cos_secret_key"
export COS_BUCKET="your_bucket"
export COS_REGION="ap-guangzhou"
export COS_ENDPOINT="cos.ap-guangzhou.myqcloud.com"
```

可选缓存配置：

```bash
export ENABLE_PARQUET_CACHE="true"
export PARQUET_CACHE_DIR="/tmp/parquet_cache"
export PARQUET_CACHE_TTL="3600"

export ENABLE_DUCKDB_CACHE="true"
export DUCKDB_CACHE_DIR="/tmp/duckdb_cache"
export DUCKDB_CACHE_TTL="3600"
export DUCKDB_PATH="a-stock/data/a-stock.duckdb"
```

本地默认会把缓存放到项目根目录外层的 `a-stock` 目录或 `/tmp`。如果你希望完全控制本地缓存位置，建议显式设置上面的缓存目录变量。

## 启动 Dagster

在项目根目录运行：

```bash
dg dev
```

然后打开：

```text
http://localhost:3000
```

如果 Dagster 没有正确加载项目，检查 `workspace.yaml`。当前文件里写的是部署机路径：

```yaml
working_directory: /home/ubuntu/quant
```

本地开发时可以改成当前仓库路径，或直接用当前目录启动 `dg dev`。

## 常用操作

列出 Dagster assets 和 jobs：

```bash
dg list defs
```

启动本地开发服务：

```bash
dg dev
```

导出因子注册表元数据：

```bash
python scripts/export_factor_registry_metadata.py
```

服务器上如果已配置 `dagster-webserver` 和 `dagster-daemon` 的 systemd 服务，可以使用：

```bash
bash scripts/manage.sh start
bash scripts/manage.sh status
bash scripts/manage.sh logs
bash scripts/manage.sh stop
```

## 因子开发入口

横截面因子：

- 公式目录：`src/domain/factor_formulas/`
- 注册表：`src/domain/factor_catalog/registry.py`
- 分析流程：`src/domain/factor_analysis/`
- Dagster asset：`src/orchestration/factor_analysis/assets/factor_analysis.py`

K 线形态因子：

- 公式目录：`src/domain/pattern_factor_formulas/`
- 注册表：`src/domain/pattern_factor_catalog/registry.py`
- 分析流程：`src/domain/pattern_factor_analysis/`
- Dagster asset：`src/orchestration/pattern_factor_analysis/assets/pattern_factor_analysis.py`

新增因子时，优先保持现有 Polars 风格：输入列、输出列和空数据处理要与同类因子一致，然后把因子名称加入对应 registry。

## 注意事项

- `requirements.txt` 看起来来自完整本机环境导出，包含大量与本项目无关的包；日常开发优先使用 `pyproject.toml` 和 `uv.lock`。
- 数据读取依赖 COS 权限和 Tushare token，缺少环境变量时 Dagster asset 会在运行期失败。
- `src/shared/env_api.py` 默认分析起始日期为 `2016-09-01`。
- 当前代码中有部分历史目录和 `__pycache__` 文件，开发时尽量只修改源代码、配置和注册表。
