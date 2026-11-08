# 实现细节

这份文档梳理了项目的核心数据流向、模块职责以及关键实现细节，方便后续维护或二次开发时快速定位。

---

## 1. 推荐阅读顺序

建议顺着数据处理的链路来看：
1. `examples/generate_mock_data.py`：数据生成逻辑，重点查看倾斜分布和冷启动边界如何构造；
2. `spark_jobs/common.py`：跨平台运行环境与 SparkSession 配置；
3. `ods_ingest.py` → `dwd_build.py` → `dws_build.py`：核心的三层数仓构建流程；
4. `skew_aggregation.py`：两阶段聚合的具体实现（`none / all / hot_only` 三种 salt scope）；
5. `pipelines/benchmark_skew.py`：三种方案的交错性能测试、实际 group 基数与指标采集；
6. `pipelines/benchmark_salt_sweep.py`：复用同一套测量代码，做 2 / 5 / 20 盐桶敏感性扫描；
7. `evaluation/features.py`：行为序列特征计算；
8. `evaluation/evaluate_als.py`：时间切分、模型训练与排序指标评估；
9. `dashboards/app.py`：前端看板展示。

---

## 2. 数据生成：`examples/generate_mock_data.py`

这个脚本仅使用 Python 标准库，不依赖 Spark。这样设计是为了让“数据生成”和“计算任务”解耦，运行数仓代码前随时可以单独重新造一份数据。

主要逻辑：
- 时间窗口固定在 2025-01-01 起的 90 天；
- 默认生成 100 万行，包含 5 万用户和 2 万商品；
- 通过 `hot_ratio=0.38` 强制让热门商品（ID 9999）占据 38% 的数据量，以此模拟数据倾斜；
- 商品和类目是多对一关系，类目 ID 直接通过公式 `(item_id * 37 + 11) % 100` 映射；
- 为了评估冷启动，特意让 10% 的用户只在 80% 时间边界之后才产生行为；
- 时间戳精确到微秒，并用 `used_timestamps` 集合记录已生成的组合，避免主键重复；
- 运行完成后会输出包含热门占比、行为分布等指标的 JSON 文件，方便复现日志直接引用真实数字。

---

## 3. 环境配置：`spark_jobs/common.py`

`configure_runtime()` 必须在 import PySpark 之前执行。它主要解决了 Windows 下的环境兼容问题：
1. 优先加载 `.bootstrap` 里的便携版 JDK 17；也支持通过 `SPARKINSIGHT_USE_SYSTEM_JAVA=1` 强制使用系统 JDK；
2. 检测并配置 `winutils.exe` 及 `HADOOP_HOME`；
3. 统一设置 `PYSPARK_PYTHON`，确保 Driver 和 Worker 用的是同一个解释器环境；
4. 锁定 `SPARK_LOCAL_IP=127.0.0.1`，避免 Windows 下因主机名解析导致启动异常。

`build_spark_session()` 里的参数是为了运行基准测试（Benchmark）专门固定的：
- 固定使用 `local[8]` 和 16 个 shuffle partitions；
- 关闭了 AQE 和 Skew Join。这是因为我们要测的是代码本身的性能基线，不能让 Catalyst 优化器在两次运行中产生不一样的执行计划；
- 开启 `partitionOverwriteMode=static`，保证重新运行时只覆盖指定分区，实现幂等。

*(注：这些参数是为了测试环境设计的，生产环境需要按实际数据量重新调优。)*

---

## 4. ODS 层：`spark_jobs/ods_ingest.py`

- 读 CSV 时不解析类型，全部作为 String 保留；
- 增加 `_ingest_ts`（入库时间）和 `_source_file`（来源文件）元数据；
- 采用全量覆盖的方式写入 Parquet；
- 写入后读回并校验数据量非空。

**设计考量**：ODS 层故意不做类型转换，是为了保留最原始的数据快照。如果后续发现 DWD 层的清洗逻辑有误，可以直接从 ODS 重放，无需追溯原始 CSV 文件。

---

## 5. DWD 层：`spark_jobs/dwd_build.py`

`build_dwd()` 做了四件事：
1. 读取 ODS 数据；
2. 将 ID 字段转为 Int，时间转为 Timestamp；
3. 过滤掉关键字段为 Null 以及非法 `behavior_type` 的脏数据；
4. 按 `(user_id, item_id, event_time)` 进行 `dropDuplicates` 去重。

去重完成后，`run()` 会执行 4 条数据质量断言（非空、主键唯一、非空率、持久化行数），校验通过后按 `dt` 分区覆盖写入。

*注意*：`dropDuplicates` 这一步必然会触发 Shuffle，这是为了保证最终数据的唯一性和幂等性，属于正常开销。

---

## 6. DWS 层与加盐：`skew_aggregation.py` / `dws_build.py`

核心实现在 `build_item_stats()`，它通过 `salt_scope` 控制三种路径：
- `none`：不加盐，走常规的 `groupBy("item_id")` 聚合；
- `all`：所有记录都加上 `_salt`，Stage-1 按 `(item_id, _salt)` 聚合，Stage-2 去掉 `_salt` 后按 `item_id` 汇总；
- `hot_only`：只有 `hot_item_id` 对应的记录计算 `_salt`，其余记录 `_salt=0`；Stage-1 仍按 `(item_id, _salt)` 聚合，但普通 item 不会被拆开。

两阶段复用了 `_partial_metrics()` 逻辑，确保统计指标一致。盐值算法使用 `pmod(xxhash64(...), N)`，必须保证结果是确定性的，否则每次重新运行的数据分组都会变化（详见 `DESIGN_DECISIONS` 第 7 条）。

`dws_build.py` 默认调用 `salt_scope="none"`；`--salted` 显式切换为 `salt_scope="all"`，演示时使用 `SALT_BUCKETS = 10`。计算完成后先调用 `cache()` 和 `count()` 强制物化，再落盘。随后执行 4 条对账断言，确保 DWS 的聚合结果与 DWD 的明细数据能对齐。默认不加盐是为了让主链路服从 benchmark 的负结果（详见 `DESIGN_DECISIONS` 第 16 条）。summary 里的 `aggregation`、`salt_scope`、`salt_buckets` 会明确记录本次走的是哪条路径。

---

## 7. 性能测试：`pipelines/benchmark_skew.py` / `benchmark_salt_sweep.py`

`benchmark_skew.py` 的流程：
1. 自动检查并编译 JVM Listener (`TaskTimingCollector.java`)；
2. 将 Jar 包注入到 `PYSPARK_SUBMIT_ARGS` 中；
3. 读取 DWD 数据，提取需要的四列，使用 `persist(MEMORY_AND_DISK)` 缓存并触发物化；
4. 严格按照 U1（不加盐）→ F1（全量加盐）→ H1（仅热点键加盐）→ U2 → F2 → H2 的交错顺序执行，取每种方案第 2 次结果；
5. 在 `measure_once()` 中挂载 Listener，执行聚合和 `collect()`，记录耗时和 Task 指标；
6. 校验三种方案的输出行数都等于 distinct item 数，再用 `distinct(item_id, _salt)` 测量实际 group 基数，输出 JSON。

`benchmark_salt_sweep.py` 不复制测量逻辑：它 import `benchmark_skew.py` 的 `load_benchmark_source()`、`run_approach_set()` 等函数，复用同一份缓存 DWD、同一个 Listener 和同一套两轮协议，只在外层循环替换 `salt_buckets`。默认扫描 2 / 5 / 20，10 桶基准点保留在 `benchmark_results.json`，避免两个文件对同一个 10 桶结果各说各话。

**关于最长 Task 的采集**：
代码里只统计 `successful` 为 true 的任务。如果采集到的 `duration` 为 0，会使用 finish 和 launch 时间做兜底计算。同时还会输出按 Stage 维度的聚合统计，方便定位具体是哪个 Stage 成为性能瓶颈。

**关于口径**：三种方案都写成逻辑两阶段，但加盐方案会多一次 shuffle。benchmark 不把这次 shuffle 当成“误差”藏起来，而是把 stage 数、shuffle 字节和记录数一起报告。`group_cardinality` 同时给出 `actual` 与 `max_possible`，前者是现场 `distinct` 出来的，后者只是理论上限。

---

## 8. 特征工程：`evaluation/features.py`

**用户侧特征**：
- 按用户和事件时间排序，使用 `lag` 提取前置行为；
- 计算两次行为之间的时间差（`gap_sec`）；
- 聚合出行为总数、商品/类目多样性、活跃天数、转化率、间隔均值/最大值等统计指标；
- 统计行为转移频次（如 pv->cart, pv->buy 等）；
- 最终 17 个维度的特征通过 `VectorAssembler` 拼接成一个向量。

**商品侧特征**：
- 按 `(item_id, category_id)` 聚合；
- 类目使用 `StringIndexer` 进行编码；
- 最终拼接 10 个维度的特征向量。

`run()` 阶段会校验行数、主键唯一性，并验证特征向量的维度是否符合预期。

---

## 9. 模型评估：`evaluation/evaluate_als.py`

评估流程：
1. 读取 DWD，缓存相关列；
2. `time_split()` 按 80% 分位点切分训练集和测试集；
3. 构建隐式评分矩阵 `(user_id, item_id, rating)`；
4. 训练 ALS 模型；
5. 使用 `recommendForUserSubset` 生成推荐候选；
6. 过滤推荐结果：保留原始排序，通过 anti join 剔除训练集中已经看过的商品，截取前 20 个；
7. 构建实际结果集，同样剔除已见商品；
8. 计算 `RankingMetrics`（NDCG, MAP 等）；
9. 对于冷启动用户，单独使用热门商品进行 fallback 评估。

**几个不能破的规矩**：
- 评估用户必须同时存在于训练集和测试集；
- 训练集见过的商品不能算作成功推荐（防泄漏）；
- 隐式反馈没有负样本，不适用 RMSE 指标。

---

## 10. 看板展示：`dashboards/app.py`

- 使用 `st.cache_data(ttl=5)` 读取离线生成的 JSON 结果；
- 页面分为 4 个 Tab，数据全部来源于静态 JSON，如果文件缺失会提示生成命令；
- Benchmark tab 展示不加盐 / 全量加盐 / 仅热点键加盐三方案、stage 明细、实际 group 基数，以及可选的盐桶 sweep 折线图；
- 图表使用 Altair 渲染，三色分类板已用 dataviz 校验脚本验证 CVD 分离度；
- 已通过 `AppTest` 无头测试，确保页面逻辑不会报错。

---

## 11. 常见参数调整

### 场景 A：修改 Salt 桶数（10 -> 50）
需要同步改动三个地方：
1. `spark_jobs/skew_aggregation.py` 中 `build_item_stats()` 的默认参数 `salt_buckets`；
2. `spark_jobs/dws_build.py` 的 `SALT_BUCKETS` 常量（仅 `--salted` 演示路径使用）；
3. `pipelines/benchmark_skew.py` 的 `SALT_BUCKETS`，或运行时用 `--salt-buckets` 覆盖。

调整桶数会直接影响 Stage-1 的 Group 基数，进而改变 Shuffle Write 的数据量和执行时间。并非桶数越多性能越好；实测 2 / 5 / 20 桶下，全量加盐和仅热点键加盐都不是单调变快，必须重新看 sweep 数据。若只想测敏感性、不改主基准，运行：
```bash
python pipelines/benchmark_salt_sweep.py --salt-buckets 2 5 10 20
```

### 场景 A2：把“仅热点键加盐”接到 DWS
当前 `dws_build.py` 只暴露 `--salted`（等价于 `salt_scope="all"`）。如果要让 DWS 走 `hot_only`，需要给 CLI 增加 scope 参数并传入 `hot_item_id`；不要只改 `SALT_BUCKETS`，否则仍然会拆所有 item。

### 场景 B：主指标从 @10 换成 @20
因为代码里已经同时算了两套指标，所以可以选择：
- **仅修改展示**：修改 `dashboards/app.py` 对应行数的读取键名（把 `ndcg@10` 换成 `ndcg@20` 等），无需重训模型。
- **仅计算 @20**：修改 `evaluation/evaluate_als.py:58-59`，将 `DEFAULT_K_VALUES` 改为 `(20,)`。底层的打印逻辑已经做了动态适配。

### 场景 C：切分比例从 80% 换成 70%
- **仅调整评估**：修改 `evaluation/evaluate_als.py:61` 的 `SPLIT_QUANTILE`。切分点变动后，测试集也会变，新的指标无法与旧结果直接对比。
- **重新生成数据集**：如果希望冷启动用户的生成边界也同步对齐，需要修改 `examples/generate_mock_data.py:111` 里的切分系数（0.8 -> 0.7）。重新生成数据后，需要重新运行整条链路，并更新文档中所有引用的指标数字。

---

## 12. 调试与维护

- 快速生成小规模数据：`python examples/generate_mock_data.py --rows 200000 --hot-ratio 0.38`
- 单独执行 DWS 层：`python spark_jobs/dws_build.py`
- 调整压测参数：`python pipelines/benchmark_skew.py --salt-buckets 20 --shuffle-partitions 32`
- 跑盐桶敏感性：`python pipelines/benchmark_salt_sweep.py --salt-buckets 2 5 20`
- 校验看板逻辑：
  ```python
  from streamlit.testing.v1 import AppTest
  at = AppTest.from_file("dashboards/app.py", default_timeout=120).run()
  assert not at.exception
  ```
- **已知报错**：在 Windows 下 Spark 任务结束时，大概率会报 `ShutdownHookManager: Exception while deleting Spark temp dir...`。这是因为系统清理临时目录时存在竞争条件，进程退出码仍为 0，不影响任何数据结果，可忽略。
