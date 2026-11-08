# 复现日志

记录本机从零到项目完成的真实命令、输出、报错和修复。

运行环境（实测）：

| 项 | 值 |
|---|---|
| OS | Windows 11 Home China 10.0.26200 |
| Python | 3.11.18（项目内 uv 安装，`.venv`） |
| PySpark | 3.5.1 |
| Java | Temurin 17.0.21（项目内 `.bootstrap/jdk17`） |
| winutils | Hadoop 3.3.5（项目内 `.bootstrap/hadoop`） |
| Spark master | `local[8]` |
| shuffle partitions | 16 |
| AQE | false（benchmark）/ 默认 false（其他任务） |

> 本机系统原本只有 Python 3.13/3.14 和 Java 21。没有改系统 Python，没有改
> 系统 `JAVA_HOME`；项目内便携运行时都放在 `.bootstrap/`，被 gitignore。

---

## 1. 环境准备

### 1.1 Python

系统 Python 3.13/3.14 下，`numpy==1.26.4` 没有 cp313 wheel，无法按
`requirements.txt` 安装。处理：

- 下载便携 `uv` 到 `.bootstrap/uv/uv.exe`；
- 用 uv 安装 CPython 3.11.18 到 `.bootstrap/python/`；
- 在项目根创建 `.venv`，安装固定版本：

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements.txt
```

实际安装成功，`python -c "import pyspark, pandas, numpy, pyarrow, streamlit"`
无报错。

### 1.2 Java

系统只有 Java 21，Spark 3.5.1 官方支持 JDK 8/11/17，不支持 21。处理：

- 下载 Temurin JDK 17.0.21 到 `.bootstrap/jdk17/jdk-17.0.21+9/`；
- `spark_jobs/common.py` 在 import PySpark 前设置 `JAVA_HOME`；
- smoke test 确认 Spark 3.5.1 能启动，`java.version=17.0.21`。

### 1.3 winutils

Windows 本地模式需要 `winutils.exe` 和 `hadoop.dll`。尝试
`cdarlint/winutils` 的 `hadoop-3.3.1` raw 路径返回 404，随后改用 GitHub API
的 raw media type 方式，成功下载 `hadoop-3.3.5/bin/winutils.exe` 和
`hadoop.dll` 到 `.bootstrap/hadoop/bin/`。`common.py` 自动设置
`HADOOP_HOME`。

### 1.4 PySpark listener

PySpark 3.5.1 没有 Python 侧 `SparkListener` 基类，`SparkContext` 也没有
`addSparkListener` 的 Python API。处理：

- 写 `spark_jobs/jvm/TaskTimingCollector.java`；
- 用项目内 JDK 17 的 `javac` 对 PySpark 自带 jars 编译；
- 打包 `.bootstrap/listener/SparkInsightListener.jar`；
- benchmark 启动前把它加入 `PYSPARK_SUBMIT_ARGS --driver-class-path`；
- controlled test（100,000 行 / 4 个 group）验证 listener 能正确报告
  task 数，shuffle read/write records。

---

## 2. 生成数据

命令：

```bash
python examples/generate_mock_data.py
```

实际输出（节选，完整 JSON 在 `.bootstrap/generate.log`）：

```text
rows: 1,000,000
unique_users: 50,000
unique_items: 20,000
hot_item_id: 9999
hot_item_rows: 380,421
hot_item_share_pct: 38.04%
behavior_counts:
  buy: 49,888
  cart: 69,689
  fav: 29,965
  pv: 850,458
late_user_count: 5,000
seed: 42
elapsed_sec: 11.598
```

产物：`data/raw/behavior.csv`，约 43MB，1,000,001 行（含 header）。

---

## 3. ODS → DWD → DWS

命令：

```bash
python spark_jobs/run_pipeline.py
```

实际输出（节选）：

```text
[ASSERT][PASS] row_count_positive: dwd.behavior_events rows=1,000,000 > 0
[ASSERT][PASS] primary_key_unique: dwd.behavior_events
  key=['user_id', 'item_id', 'event_time']
  distinct=1,000,000 total=1,000,000 duplicates=0
[ASSERT][PASS] key_non_null_rate: dwd.behavior_events
  threshold=0.999
  non_null_rates:
    user_id: 1.0
    item_id: 1.0
    event_time: 1.0
    behavior_type: 1.0
[ASSERT][PASS] dwd_persisted_row_count: actual=1,000,000 expected=1,000,000
[ASSERT][PASS] row_count_positive: dws.item_stats rows=20,000 > 0
[ASSERT][PASS] primary_key_unique: dws.item_stats
  key=['item_id'] distinct=20,000 total=20,000 duplicates=0
[ASSERT][PASS] dws_item_row_reconciliation: actual=20,000 == expected=20,000
[ASSERT][PASS] dws_event_total_reconciliation: actual=1,000,000 == expected=1,000,000
{
  "status": "PIPELINE_OK",
  "ods_rows": 1000000,
  "dwd_rows": 1000000,
  "dws_rows": 20000,
  "dws_event_total": 1000000
}
```

本次 `time` 观测总耗时约 **59.6s**。DWS 的 salt_buckets=10，
shuffle_partitions=16。

> 注：这次全链路运行发生在 `DESIGN_DECISIONS` 第 16 条之前，当时
> `dws_build.py` 固定走加盐路径。当前默认已改为不加盐；如需复现本次的
> 加盐路径，运行 `python spark_jobs/run_pipeline.py --salted`。

---

## 4. Shuffle benchmark

### 4.1 第一版 `.count()` 不可信，改用 `.collect()`

最初用 `aggregate.count()` 运行，测得两者几乎一样（差异在噪声范围）。检查
物理计划：salted 有两个 Exchange，unsalted 有一个；但 stage 指标显示第二次
聚合的完整 shuffle 没有明显物化。原因是 Catalyst 的 Count 优化可能只算
行数，跳过部分聚合/投影，两阶段计划没有被完整执行。

把 action 改为 `aggregate.collect()`，收集全部 20,000 行 item 指标，避免
Count 优化，才能完整执行并观察两阶段聚合。后续重跑也确认 `.count()` 下的
差异并不稳定；最终以 `.collect()` 的完整两阶段结果为准。

### 4.2 三方案重测：加入仅热点键加盐（2026-11-08）

代码从 `salted: bool` 重构为 `salt_scope: none | all | hot_only`，并加入
实际 group 基数测量。命令：

```bash
./.venv/Scripts/python.exe pipelines/benchmark_skew.py
```

实际 stdout（measured run 2；完整 JSON 在 `benchmark_results.json`）：

```text
=== Skew benchmark (measured run 2) ===
approach                            wall_s   longest_task_ms   shuffle_read_MB   shuffle_write_MB
unsalted_two_phase                   2.354               456             10.63              10.63
salted_two_phase                     3.408               846            18.418             18.418
salted_hot_only_two_phase            2.806               552            11.522             11.522

full salt vs unsalted: wall +44.77%, longest task +85.53%
hot-only salt vs unsalted: wall +19.20%, longest task +21.05%

Stage-1 logical group cardinality:
  unsalted_two_phase: actual=20,000 max_possible=20,000
  salted_two_phase: actual=190,924 max_possible=200,000
  salted_hot_only_two_phase: actual=20,009 max_possible=20,009
```

关键数字：

| 方案 | stage-1 shuffle write 记录 | stage-1 实际 group | wall clock | 最长 Task |
|---|---:|---:|---:|---:|
| 不加盐 | 440,303 | 20,000 | 2.354s | 456ms |
| 全量加盐 | 598,054 | 190,924 | 3.408s | 846ms |
| 仅热点键加盐 | 440,708 | 20,009 | 2.806s | 552ms |

解释：仅热点键加盐把 group 基数几乎压回不加盐，stage-1 shuffle write 只比
不加盐多 405 条，所以比全量加盐轻得多；但它仍然多一次 shuffle，因此 wall
clock 和最长 Task 仍高于不加盐。这个结果比“只比较全量加盐”更能说明成本来自
哪里。

### 4.3 盐桶敏感性 sweep（2026-11-08）

命令：

```bash
./.venv/Scripts/python.exe pipelines/benchmark_salt_sweep.py --salt-buckets 2 5 20
```

实际 stdout：

```text
=== Salt-bucket sweep (measured run 2) ===
 buckets    unsalted_s       all_s    hot_only_s   all_delta   hot_delta
       2         2.211       2.526         2.460     +14.25%     +11.26%
       5         2.033       2.788         2.348     +37.14%     +15.49%
      20         2.156       2.906         2.346     +34.79%      +8.81%
```

实际 group 基数（actual / max_possible）：

| 桶数 | 全量加盐 | 仅热点键加盐 |
|---:|---:|---:|
| 2 | 40,000 / 40,000 | 20,001 / 20,001 |
| 5 | 99,799 / 100,000 | 20,004 / 20,004 |
| 20 | 315,405 / 400,000 | 20,019 / 20,019 |

结论：桶数增加时，全量加盐的 group 基数增长并出现 Hash 碰撞，wall clock
没有单调变快；仅热点键加盐的 group 基数只随热点桶数线性增加，始终接近
20,000，wall clock 变化在 +8.81% 到 +15.49% 之间波动。该 sweep 不重复测量
10 桶，10 桶基准点见 4.2。完整 JSON 在 `benchmark_salt_sweep.json`。

sweep 口径：每个桶数都独立重跑了三种方案（包括不加盐 baseline），所以每一行
是桶内配对比较；但 baseline 自身在 2.033s–2.211s 之间漂移（10 桶的 2.354s
来自另一份 `benchmark_results.json`，不属于这次 sweep）。跨桶比较百分比时，
差异里混有 baseline 漂移和单次运行噪声。不能把 20 桶的 +8.81% 与
2 桶的 +11.26% 之间的差全部归因于桶数本身。sweep 同样只有每个桶 1 个
measured sample，没有做显著性检验。

---

## 5. 行为特征

命令：

```bash
python -m evaluation.features
```

第一次报错：

```text
ImportError: cannot import name 'vector_size' from 'pyspark.ml.functions'
```

原因：PySpark 3.5.1 的 `pyspark.ml.functions` 没有 `vector_size`（该函数在
更高版本才有）。修复：改用 `vector_to_array` + `F.size`：

```python
from pyspark.ml.functions import vector_to_array
F.size(vector_to_array("features"))
```

重新运行成功，实际结果：

```text
user_features: 50,000 rows, 17 features
item_features: 20,000 rows, 10 features
[ASSERT][PASS] features.user_features primary_key_unique
[ASSERT][PASS] features.item_features primary_key_unique
status: OK
```

产物：

- `data/warehouse/features/user_features`
- `data/warehouse/features/item_features`
- `data/warehouse/features/summary.json`

注：第一次成功运行写 Parquet 时出现
`MemoryManager: Total allocation exceeds 95% ... Scaling row group sizes`，
是 Parquet writer 的 heap 自适应警告，任务仍成功；第二次重新运行同样成功。

---

## 6. 隐式 ALS 评估

命令：

```bash
python -m evaluation.evaluate_als
```

第一次运行耗时约 **10m15s**，计算完成并写出了
`evaluation_results.json`，但在最后的打印阶段报错：

```text
KeyError: 'rank'
```

原因：`emit()` 里写的是 `training['rank']`，而 `rank` 在 `config` 下。
修复后又暴露第二个打印错误：

```text
KeyError: 'avg_predictions'
```

原因：`avg_predictions` 在 `recommendation` 下，不在 `evaluation` 下。
两处都已修复；然后用已写出的 JSON 回放 `emit()` 验证，打印成功。

真实评估结果（来自 `evaluation_results.json`）：

```text
split quantile=0.8: train_rows=799,907 test_rows=200,093 (actual test share=20.01%)
train_users=45,000 test_users=45,054 cold_start_users=5,000 (11.10% of test users)
ALS rank=32 maxIter=10 alpha=40.0 implicitPrefs=true fit_seconds=37.263
evaluated_users=33,528 avg_actual_items=1.8469 avg_predictions=20.0
K=10: NDCG=0.000963 MAP=0.000716 Precision=0.000209 Recall=0.001180
K=20: NDCG=0.001115 MAP=0.000749 Precision=0.000154 Recall=0.001702
cold-start popularity baseline:
  users=5,000 NDCG@10=0.225296 MAP@10=0.080858 Precision@10=0.100300
```

其他真实数字：

- 训练用户-item 交互 540,426，distinct users 45,000，distinct items 20,000，
  密度 0.00060047；
- 冷启动测试行 100,116，占测试行 50.0347%；
- 评估 33,528 人，relevant pairs 61,922；
- 测试中训练未见 item 的 pair 数 0（20,000 个 item 全部在训练期出现）；
- 推荐覆盖 11,383 / 20,000 = 56.915%。

**指标偏低的原因**：合成数据的非热门 item 基本均匀随机，用户之间没有
可学习的协同偏好；热门 item 9999 又几乎被每个训练用户见过，按协议从候选
中剔除。剔除后是 20,000 个近乎随机商品的排序问题，指标接近随机是数据
性质，并非评估代码计算错误所致。冷启动 popularity 指标高，是因为它可以把热门
item 推荐给没有历史的新用户；两个 cohort 条件不同，不能直接比较。

**RMSE**：隐式反馈没有显式评分和可靠负样本，**隐式反馈不适用 RMSE**，
本项目不输出 RMSE。

注：修复打印逻辑的错误后原本计划重新运行一次以获取完整日志，结果因机器内存不足被系统
终止。不过之前的 `evaluation_results.json` 是完整的，使用旧 JSON 回放打印
也成功了。如需重新运行全流程，需先确保内存充足。

---

## 7. Dashboard

启动：

```bash
streamlit run dashboards/app.py --server.port 8501 --server.headless true
curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8501/
```

实际返回 `HTTP=200`。停止后端口关闭（`HTTP_AFTER_STOP=000`）。

无头执行验证：

```python
from streamlit.testing.v1 import AppTest
at = AppTest.from_file("dashboards/app.py", default_timeout=120).run()
print(len(at.exception), len(at.tabs), len(at.dataframe), len(at.get("arrow_vega_lite_chart")))
```

2026-11-08 加入三方案对比和盐桶 sweep 后重新执行，实际结果：

```text
exceptions: 0
tabs: 4 dataframes: 11 charts: 3
```

`charts: 3` 是 benchmark tab 的三张图：wall clock 柱状图、最长 Task 柱状图、
盐桶敏感性折线图。

第一次 AppTest 出现 Arrow 序列化警告：某个 dataframe 的“值”列混合了
字符串和 double（`'true'` 与浮点数）。修复：把“模型与训练”表的所有值
统一转成字符串。重新运行后无异常，无错误。

---

## 8. 已知的良性报错

每次 `spark.stop()` 时可能出现：

```text
ERROR ShutdownHookManager: Exception while deleting Spark temp dir:
C:\Users\...\Temp\spark-...\pyspark-...
java.nio.file.NoSuchFileException: ...
```

这是 Windows 本地模式下 Spark 清理临时目录的已知竞态；进程仍以 0 退出，
不影响数据产物。没有通过修改代码消除该现象，仅在文档中标注。

---

## 9. 未在本地验证

- **集群模式**：没有在 YARN/K8s/真实多机环境验证，只在 Windows
  `local[8]` 运行过。
- **Linux/macOS**：winutils 只对 Windows 有意义；没有在 Linux/macOS 上
  重新运行整条链路。
- **浏览器视觉**：只验证了 HTTP 200 和 Streamlit AppTest 无头执行，没有
  在真实浏览器里逐页检查布局/字体/交互。
- **显式反馈 RMSE 对照**：没有显式评分数据集，因此没有 RMSE 对照。
- **更大规模（10 亿行）**：没有验证；相关扩展建议见
  `docs/PROJECT_FAQ.md` 的 6.2 节。
- **多热点 / 热点集合**：`hot_only` 只接受单个 `hot_item_id`，没有验证多个
  热点 key 同时加盐，也没有验证“热点是动态变化”的场景。
- **更细的桶数曲线**：只测了 2 / 5 / 10 / 20 四个点，没有做 3、7、15 等
  更细粒度扫描，也没有拟合成本模型；不能声称存在某个“最优桶数”。
- **选择性加盐的生产收益**：本机只验证了 `count/sum` 这个可 Combine 负载，
  没有在不可 Combine 的聚合或热 key join 上验证 `hot_only` 的正收益。

---

## 10. GitHub 发布记录（2026-11-07 完成）

- 本地下载并配置了便携版 `gh` 命令行工具；
- 设备授权成功，登录账号：`WhiteEagleNight`；
- 通过 `gh repo create` 命令创建公开仓库并推送（`--public --source=. --remote=origin --push`）；
- 仓库 URL：<https://github.com/WhiteEagleNight/SparkSkew-Benchmark>；
- 推送结果：`main` 分支已成功跟踪 `origin/main`，新分支推送成功；
- README 中的 `<this-repo-url>` 已替换为上述真实链接。

仍未验证：GitHub Actions/CI，Pages，Release，分支保护，协作者权限。
