# SparkSkew-Benchmark 底层原理大全

本文档梳理了项目背后的核心运行机制与设计考量，跳出具体的 API 使用，重点探讨以下几个问题。这些内容可作为技术评审与团队沟通的参考：

- Spark 的 Job/Stage/Task 如何切分？为什么 `groupBy` 必然触发 Shuffle？
- Shuffle 的 map 端 Combine 为什么能让 `count/sum` 变快？为什么 `collect_list` 不行？
- 加盐两阶段聚合的成本模型是什么？什么条件下它才真正管用？
- 隐式 ALS 的目标函数、置信度、冷启动和排序指标分别是什么？
- 为什么时间切分比随机切分可信？隐式反馈为什么不能算 RMSE？
- Parquet、分区覆盖、断言和对账，如何保证任务可复现与幂等？

*注：文中出现的所有“本机数字”均来自实际运行结果，并非理论估算。*

---

## 1. Spark 执行模型

### 1.1 核心角色
- **Driver**：负责解析代码、构建 DAG、划分 Stage、生成并调度 Task，最后收集结果。`main()`、`SparkSession` 以及 `collect()` 都在这里执行。
- **Executor**：实际干活的 JVM 进程。拥有若干 task slot 和统一内存，负责执行 Task、缓存数据以及写 shuffle 文件。
- **Task**：Spark 调度最小单位，一个 partition 对应一个 Task。
- **Stage**：以 Shuffle 为边界划分。窄依赖可以在同一个 Stage 内 pipeline 执行；遇到宽依赖必须等上游写完 shuffle 数据。

项目配置的 `local[8]` 并非指 8 台机器，而是在单个 JVM 中用 8 个线程模拟 Executor slot。因此，本项目中的分布式行为（Shuffle、Task 划分、Spill）是真实发生的，只是不存在跨节点网络和真实多 Executor 的故障域。这也划定了本地 Benchmark 的边界。

### 1.2 Job 与 Stage 的触发
Spark 采用惰性执行：`read`、`filter`、`groupBy` 仅构建逻辑计划，直到遇到 action 操作（`count`、`collect`、`write`）才真正提交 Job。

```text
action
  -> Job
     -> DAG Scheduler
        -> Stage（按 shuffle 边界切）
           -> TaskSet
              -> Task（按 partition）
```

映射到本项目：
- `run_pipeline.py` 的 `write` 触发 ODS/DWD/DWS 的 Job；
- `benchmark_skew.py` 的 `collect()` 触发两阶段聚合的 Job；
- `evaluate_als.py` 的 `fit`、`transform` 及指标计算则各自触发不同 Job。

### 1.3 窄依赖与宽依赖
| 类型 | 典型算子 | 是否触发 Shuffle |
|---|---|---|
| Narrow | `select`、`filter`、`withColumn`、`map` | 否，可 pipeline |
| Wide | `groupBy`、`distinct`、`dropDuplicates`、`join`（非 broadcast）、`repartition` | 是 |

`dropDuplicates` 在 Spark SQL 底层通常实现为聚合/去重，需要按 key 重分区，绝非“本地删重”这么简单。因此，DWD 中的 `dropDuplicates(PRIMARY_KEY)` 是一道明确的 Shuffle 边界。

### 1.4 本项目的 Stage 视角
```text
CSV read (narrow)
  -> cast/filter/withColumn (narrow)
  -> dropDuplicates(user_id,item_id,event_time) (wide, shuffle)
  -> write partitionBy(dt) (wide-ish, shuffle/sort)
```

DWS 侧（不加盐）：
```text
DWD read (narrow)
  -> groupBy(item_id) partial (wide, shuffle)
  -> groupBy(item_id) final
  -> write (wide-ish)
```
两次 `groupBy` 使用同一个 key，Catalyst 可以把它们合并成一次 shuffle。

加盐聚合（`all` 或 `hot_only`；DWS 仅在 `--salted` 演示路径启用 `all`）：
```text
DWD read (narrow)
  -> groupBy(item_id, _salt) (wide, shuffle #1)
  -> groupBy(item_id) (wide, shuffle #2)
  -> write (wide-ish)
```
加盐版本在本写法下通常比 baseline 多一次 Shuffle：stage-1 的 key 变成 `(item_id, _salt)`，原本可被 Catalyst 合并的同 Key 两阶段聚合被阻断。这笔开销是否值得，取决于它能否消除长尾。不能盲目认定“加盐就是优化”，必须结合数据分布和聚合是否可合并来评判。

---

## 2. Shuffle 机制

### 2.1 Shuffle 做了什么
以 `groupBy(key).agg(count/sum)` 为例：
1. **Map 端**：每个 map task 按 key 的 hash 进行分区，写入 shuffle 文件。期间可能先做局部聚合，再按 partition 排序/溢写。
2. **Reduce 端**：每个 reduce task 拉取自己负责的 partition 的所有 map 输出，执行 merge、反序列化，完成最终聚合。
3. **结果**：每个 key 的所有记录最终落到同一个 reduce partition 中。

Shuffle 的成本主要消耗在：磁盘读写（spill/shuffle 文件）、跨 Executor 网络传输、序列化/反序列化、排序/合并，以及内存不足导致的 Spill 和 GC。

### 2.2 Map 端预聚合（Combine）
如果聚合满足**结合律和交换律**，Spark 会在 map 端先做局部聚合，只把部分结果发给 reduce 端，大幅减少网络 IO。典型如：`count`、`sum`、`min`、`max`、`avg`。

以下情况无法提前合并：
- `collect_list`、`collect_set`：必须保留所有值；
- 精确去重（`count(distinct)`）：必须看到全量 key；
- 某些 percentile 或自定义聚合：中间状态不可合并；
- RDD 的 `groupByKey`：默认 `mapSideCombine=false`，需要保留完整迭代器（`reduceByKey` 默认开启 combine）。

本项目实测：100 万行输入，在不加盐时，stage-1 的 shuffle write 仅有 **440,303** 条记录，说明 `count/sum` 已在 map 端合并掉大量行。全量加盐后，stage-1 的 key 变为 `(item_id, _salt)`，实际 group 基数为 **190,924**（理论上限 200,000），stage-1 shuffle write 增至 **598,054** 条；仅热点键加盐只把 item 9999 拆开，group 基数为 **20,009**，stage-1 shuffle write 为 **440,708** 条，但总 Shuffle 仍因额外一次 shuffle 增加到 11.522MB。两种加盐方式在这个可 Combine 负载上都是净开销为正。

### 2.3 分区数与数据分布
`spark.sql.shuffle.partitions=16` 表示 Shuffle 后有 16 个 reduce partition。如果 key 的 hash 分布均匀，各 partition 数据量接近；但若某 key 占据 38%，它所在的 partition 就会显著大于其他 partition。

**分区数并不等于 key 数**。2 万个 item 分到 16 个 partition，平均每个 partition 有 1250 个 key；热门 key 会让某个 partition 的行数远超平均值。倾斜看的是“partition 内部的数据量/计算量”，而不仅仅是“key 的数量”。

### 2.4 倾斜（Skew）的机制
假设执行 `groupBy(item_id)`，热门 item 9999 有 380,421 行，其他 item 平均仅 31 行。按 hash 分区后：
- 9999 所在的 reduce task 要处理近 38 万行；
- 其他 reduce task 可能只有几万行；
- 整个 Stage 的完成时间取决于最慢的 task（straggler）；
- 该 task 极易发生磁盘 Spill，甚至 OOM。

`最长 Task` 比“总 wall clock”更能直接暴露倾斜问题：总时间会被并行度稀释，而最长 Task 则直观展示了长尾。本项目借助 JVM `SparkListener` 采集 `TaskInfo.duration()`，且仅统计成功的 task。

### 2.5 AQE 的适用边界
Spark 3.x 的 AQE 可以在运行时合并小分区、处理 Skew Join（主要针对 join）、在特定场景下避免重复拉取 shuffle 数据。

但 AQE 不是万能的：
- 它对 `groupBy` 聚合引起的倾斜没有直接的 Skew Join 优化；
- 需要收集运行时统计信息，可能引入额外的 shuffle 或阶段；
- 开启后会改变“原始 shuffle 行为”，不适合作为倾斜 Benchmark 的对照。

本项目在 benchmark 中固定 `spark.sql.adaptive.enabled=false`，是为了观察原始两阶段聚合的纯成本。生产环境通常应开启 AQE，并单独评估其收益。

---

## 3. 加盐两阶段聚合

### 3.1 盐函数
项目采用：
```python
pmod(
    xxhash64("user_id", "behavior_type", col("event_time").cast("long")),
    lit(salt_buckets),
).cast("int").alias("_salt")
```
- **确定性**：相同输入每次得到相同的盐，确保重新运行、对账和复现可比；
- **非负**：`pmod` 保证返回非负余数；
- **高基数**：将 `user_id`、`behavior_type`、`event_time` 一起 hash，避免单一用户反复访问热门 item 时全落到同一个桶；
- **摒弃 `rand()`**：随机盐每次分组不同，无法做两次运行对比，也无法复现 DWS 对账。

### 3.2 两阶段流程
**Stage 1**：
```text
groupBy(item_id, _salt)
  -> partial_count = count(*)
  -> partial_min_ts = min(event_time)
  -> partial_max_ts = max(event_time)
```
**Stage 2**：
```text
groupBy(item_id)
  -> event_count = sum(partial_count)
  -> min_ts = min(partial_min_ts)
  -> max_ts = max(partial_max_ts)
```
在 Stage 2 中，每个 item 最多只需处理 N 行部分结果（N = 盐桶数），而非 38 万条原始数据。这正是“打散热 key”的核心。

选择性加盐（`hot_only`）只对 `hot_item_id` 做上面的事，其余 item 的 `_salt` 固定为 0：
```text
hot item 9999 -> _salt = hash(...) % N -> N 个 (9999, salt) 小组
其他 item   -> _salt = 0             -> 仍是原来的 (item_id, 0) 小组
```
这样 stage-1 的 group 数只从 20,000 增加到 20,009（10 桶），而不是全量加盐的 190,924。代价是仍需在 stage-2 去掉 `_salt`，所以额外 shuffle 的次数没有消失。

### 3.3 成本模型
假设热 key 行数为 `H`，盐桶数为 `N`，原始 group 数为 `G`。
Stage 1 的 group 数上限约为 `G × N`。热 key 在每个桶预期处理 `H / N` 行，最长 task 的理论上限从 `H` 降至 `H / N`。但代价是：
- shuffle 记录数增加（部分结果变大）；
- 多一次 shuffle；
- 序列化、调度和任务数开销增加；
- 如果聚合可通过 map-side combine 加速，baseline 已经很快；在本项目这份负载和实现里，加盐会带来净损失。

本项目 `G=20,000`、`N=10`、`H=380,421`。全量加盐时 stage-1 group 的理论上限是 200,000，实测 190,924；baseline 已 combine 到 440,303 条 stage-1 shuffle 记录，全量加盐后是 598,054 条。2026-11-08 的 measured run 中，全量加盐 wall clock 从 2.354s 升至 3.408s（+44.77%），最长 Task 从 456ms 升至 846ms（+85.53%）；仅热点键加盐 wall clock 为 2.806s（+19.20%），最长 Task 552ms（+21.05%），负担更轻但仍未转为收益。这些数字直接印证了成本模型的推演。

这些数字来自 1M 行、`local[8]` 的小规模实验；不加盐方案最长 Task 只有 456ms，长尾并不严重。因此“本实验里加盐为负”不能外推到单 Task 数千万行、已经发生 Spill/OOM 的 Join 或 groupByKey 长尾场景，也不能外推到开启 AQE 的生产环境。

### 3.4 什么时候值得优先验证加盐
- 聚合不可 Combine：如 `collect_list`/`collect_set`、精确去重、复杂 percentile；
- 热 key join：事实表关联维表时，维表某个 key 数据量巨大；
- 单分区数据量超出 executor 内存，必须在 reduce 端打散；
- 分区数少、单 key 占比极高，且 AQE skew join 不适用。

**“不可 Combine”是加盐可能有效的必要条件，不是充分条件。** baseline 无法在 map 端压小热 key，只说明常规路径没有自动解决倾斜；加盐是否真的划算，还取决于额外 shuffle、group 基数膨胀、第二阶段是否仍倾斜。下面这些场景只是“优先做对照实验”的候选，不是“加了就一定快”的保证。

如果只有少数热点 key，优先考虑 `hot_only`，不要为了一个热点把全部 item 拆开。本项目的 10 桶对照显示：全量加盐的 stage-1 group 是 190,924，仅热点键加盐是 20,009，后者的 wall clock 额外开销从 +44.77% 降到 +19.20%——负担更轻，但在本负载上仍是负收益。如果聚合可 Combine，先让 Spark 自己做 map-side combine；即使做了选择性加盐，也要用实测确认净收益。

桶数也不是越多越好。2 / 5 / 20 桶 sweep 中，全量加盐的 group 基数分别实测 40,000 / 99,799 / 315,405，wall clock 变化为 +14.25% / +37.14% / +34.79%，并不单调；更多桶会增加序列化、调度和最终合并的记录数。选择 N 应基于热点行数、目标单 task 行数和实测，而不是拍脑袋。

---

## 4. Catalyst 与 Tungsten

### 4.1 Catalyst 优化器
Spark SQL 的计划流程：
```text
SQL / DataFrame API
  -> Unresolved Logical Plan
  -> Analyzed Logical Plan
  -> Optimized Logical Plan
  -> Physical Plans
  -> Selected Physical Plan
  -> RDD / WholeStageCodegen
```
常见优化规则包括：谓词下推、列裁剪、常量折叠、Join 重排序、聚合优化等。

### 4.2 为什么 `.count()` 可能“看不到”完整两阶段
`count()` 只关心行数，Catalyst 可能选择更廉价的计划，跳过部分投影/聚合，甚至折叠某些阶段。这样 benchmark 测到的并非完整的两阶段聚合。
因此项目改用 `aggregate.collect()`，强制物化全部 2 万行 item 指标，确保 stage-1 和 stage-2 都真实执行。代价是 Driver 需要收集结果；2 万行尚可，千万级数据则应改为写 Parquet 并单独 count 校验。

### 4.3 Whole-stage codegen 与 Tungsten
- **Whole-stage codegen**：将多个 operator 融合成一个 Java 方法，减少虚函数调用和中间对象。
- **Tungsten**：提供堆外内存管理、二进制格式、缓存友好算法和代码生成。
这些优化让小规模 benchmark 的绝对时间极短，也意味着 JVM 预热和 JIT 对第一次运行影响巨大。因此项目运行两次并取第二次结果，交错执行。

---

## 5. 内存、序列化与 GC

### 5.1 统一内存管理
Executor 内存大致分为：Execution（shuffle、join、sort、aggregation 的临时内存）、Storage（cache、broadcast）、User（用户数据结构）、Reserved（系统保留）。UnifiedMemoryManager 允许 Execution 和 Storage 互相借用；当 Execution 内存不足时，会 Spill 到磁盘。倾斜的长尾 task 通常最先触发 Spill，进而拖慢整个 Stage。

### 5.2 缓存
Benchmark 读取 DWD 后执行 `cache()`（`MEMORY_AND_DISK`），避免每次测量都重复读 Parquet。缓存本身也会占用内存，在 100 万行规模下尚可接受，但生产环境需评估缓存收益与 eviction。

### 5.3 序列化
Spark 默认使用 Java Serializer，Kryo 通常更快且更紧凑，但需注册类。Shuffle 的 record 序列化/反序列化是主要成本之一，加盐增加了记录数，会放大这部分开销。此外，UDF/闭包也要序列化到 Executor，应避免捕获大对象。

### 5.4 GC
G1 是 Spark 常用的 GC 算法；长尾 task 或大对象容易触发 Full GC。监控时不能只看 wall clock，还要关注 GC time、Spill 以及 peak execution memory。本地 `local[8]` 的 GC 行为与多 Executor 集群不同，结论需带上运行环境。

---

## 6. 存储：Parquet、分区与幂等

### 6.1 Parquet 的列式优势
- **列裁剪**：只读查询需要的列；
- **谓词下推**：利用 row group 的 min/max 统计跳过不相关数据；
- **压缩**：列内类型一致，压缩率通常高于行式存储；
- **编码**：支持 dictionary、RLE、bit-packing 等。

DWD/DWS 采用 Parquet，Dashboard/评估只读所需列，有效降低了 I/O 压力。

### 6.2 分区
`partitionBy("dt")` 会生成 `dt=YYYY-MM-DD/` 的目录结构。查询时带上 `dt` 过滤条件即可进行分区裁剪。分区并非越多越好：小文件过多会增大 NameNode/文件系统压力；每个分区至少一个 task，过多小分区会浪费调度资源，后续需考虑 compaction 或小文件合并。

### 6.3 静态与动态覆盖
- **Static overwrite**（项目当前）：`partitionOverwriteMode=static`，`write.mode("overwrite")` 会覆盖整个表或整个分区集合，重新运行不会残留旧 `dt` 分区。
- **Dynamic overwrite**：只覆盖本次写出的分区；源数据删掉某天时，旧分区会残留。

全量批处理 Demo 用静态覆盖更幂等；生产环境在增量场景下多用“有界重处理窗口 + 动态分区 + merge”策略。

### 6.4 幂等性
幂等指同一输入重新运行多次，结果一致。项目保证：
- DWD `dropDuplicates(PRIMARY_KEY)`；
- 静态覆盖，不追加；
- 确定性盐（不用 `rand()`）；
- 生成器 `seed=42`；
- 固定依赖版本。

需注意：`percentile_approx` 为近似分位，跨并行度可能存在细微差异；时间切分边界值以实际 JSON 为准。

---

## 7. 数据质量：断言与对账

### 7.1 断言（Assertion）
断言是任务内部的质量门禁：不满足则 fail-fast，不发布结果。项目 DWD 包含：
1. 行数 > 0；
2. 主键 `(user_id,item_id,event_time)` 去重后数量等于总数，duplicates=0；
3. 关键字段非空率 ≥ 0.999；
4. 持久化行数 = 已验证行数。

### 7.2 对账（Reconciliation）
对账是跨层口径一致性检查：DWS 行数必须等于 DWD distinct item 数；DWS 的 `sum(event_count)` 必须等于 DWD 总行数。断言关注单表约束，对账关注跨表/跨层是否丢数据或重复，两者缺一不可。

### 7.3 为什么必须 fail-fast
如果 DWS 少了几万行却仍被写出，下游 Dashboard 或模型就会在不知情的情况下使用错误数据。通过抛出 `AssertionError` 使任务以非零状态退出，调度系统才能触发重试或告警，避免污染下游。

### 7.4 阈值 0.999 的取舍
在真实脏数据环境中，要求 1.0 的非空率过于脆弱；0.999 允许极少量异常，但能敏锐捕捉系统性空值。阈值显式定义为常量，避免魔法数，生产环境应结合业务 SLA 调整，并监控趋势而非仅看单次是否过线。

---

## 8. 隐式 ALS 的数学

### 8.1 隐式反馈 vs 显式反馈
显式反馈指用户给出 1-5 星评分，未评分可视作缺失或 0。隐式反馈（如 pv/cart/fav/buy）只有正行为，缺乏可靠负样本——“没点击”可能代表没曝光、没兴趣，或已购买。

### 8.2 偏好与置信度
对于用户 `u` 和商品 `i`：
```text
p_ui = 1  如果 r_ui > 0（观测到交互）
p_ui = 0  如果 r_ui = 0（未观测）
c_ui = 1 + alpha * r_ui
```
`r_ui` 是行为权重的聚合值（项目设定 pv=1, fav=2, cart=3, buy=5，按 user-item 求和）。`alpha` 用于放大置信度；观测越多或行为越重，`c_ui` 越大。

### 8.3 目标函数
隐式 ALS（Hu et al., 2008）旨在最小化：
```text
min_{X,Y}  Σ_{u,i} c_ui (p_ui - x_u^T y_i)^2
           + λ (Σ_u ||x_u||^2 + Σ_i ||y_i||^2)
```
`X` 是用户因子矩阵，`Y` 是商品因子矩阵，`λ` 是正则化系数。

### 8.4 交替最小二乘
固定 `Y`，对每个用户 `u` 求解线性系统获取 `x_u`；固定 `X`，对每个商品求解 `y_i`；交替迭代。Spark 的 ALS 采用分块/分布式最小二乘实现，`rank` 为因子维度，`maxIter` 为交替轮数，`regParam` 对应 `λ`，`alpha` 为置信度系数。

项目参数：`rank=32`、`maxIter=10`、`regParam=0.08`、`alpha=40.0`、`implicitPrefs=True`、`seed=42`、`coldStartStrategy="drop"`。

### 8.5 冷启动
- **用户冷启动**：训练期未出现的用户没有因子，ALS 无法打分。项目从主评估中过滤此类用户，单独报告 5,000 / 45,054 = 11.0978% 的占比。
- **item 冷启动**：训练期未出现的 item 同样缺乏因子；本项目测试集中“训练未见 item 的 pair 数 = 0”，因为 2 万个 item 全在训练期出现过。
- **fallback**：采用训练期热门 item 进行非个性化推荐，单独评估，不与 ALS 指标混用。

### 8.6 已见 item 过滤
推荐训练期已见过的 item 实质是“记住训练集”，而非泛化。项目对 `seen` 执行 left anti join，仅对未见候选排序。该过滤会改变候选集和指标口径，必须在文档中说明。

### 8.7 候选生成与评估
项目使用 `posexplode` 展开预测，排除 seen，`row_number` 取 Top-K，并通过 `CANDIDATE_BUFFER=50` 预留缓冲。评估采用 `RankingMetrics`，输入为 `(predicted_items, actual_items)` 的 RDD。

生产环境中，全量 item 打分的复杂度为 `O(users × items)`，无法扩展；应改用 ANN/向量检索召回候选，再进行重排。

---

## 9. 排序指标

### 9.1 NDCG@K
```text
DCG@K  = Σ_{i=1..K} (2^{rel_i} - 1) / log2(i + 1)
NDCG@K = DCG@K / IDCG@K
```
`rel_i` 表示第 i 个位置的相关性；隐式评估通常为二值（测试集有交互=1）。`IDCG@K` 是理想排序的 DCG，NDCG 越接近 1 越好。

### 9.2 MAP@K
Average Precision 计算每个相关位置的 precision 并取平均；MAP 则是所有用户 AP 的平均值。它同时兼顾了“是否相关”和“相关项排得是否足够靠前”。

### 9.3 Precision@K / Recall@K
```text
Precision@K = top-K 中 relevant 数 / K
Recall@K    = top-K 中 relevant 数 / 测试集 relevant 总数
```
Precision 关注推荐结果的准确性，Recall 关注相关商品是否被遗漏。两者随 K 变化，必须带 K 报告。

### 9.4 为什么不用 RMSE
RMSE 需要真实评分和“未交互=低分”的假设。隐式反馈没有显式评分，负样本不可靠；将 pv/cart/fav/buy 映射成 1-5 分再算 RMSE 是人为构造，无法反映排序质量。**隐式反馈不适用 RMSE**；要做显式对照，必须另建显式评分数据集。

### 9.5 冷启动 baseline 的 cohort 问题
项目报告冷启动 popularity 的 NDCG@10=0.225296，而 ALS 的 NDCG@10=0.000963。这两个数字不可直接比较：
- ALS 仅在“训练期出现过、且测试期有未见 item”的用户上评估；
- Popularity 仅在“训练期未出现”的冷启动用户上评估；
- 两者的候选集、用户分布、相关性定义完全不同。

正确说法是“分别报告两个 cohort 的指标”，而非“popularity 比 ALS 好 200 倍”。

---

## 10. 时间切分与泄漏

### 10.1 为什么随机切分不可信
随机按行切分会把“未来行为”放进训练集。例如用户周三的行为在训练集，周二的行为在测试集；模型可以利用未来信息，导致离线指标虚高，线上无法复现。

### 10.2 时间切分
项目用：
```python
split_ts = percentile_approx(event_time.cast("long"), 0.8, 10000)
train = events.filter(event_time <= split_ts)
test  = events.filter(event_time >  split_ts)
```
实际训练 799,907 行（79.9907%），测试 200,093 行（20.0093%）。这样模拟“用历史预测未来”。

### 10.3 时间切分的代价
- 测试期可能出现训练期没见过的用户（冷启动），必须单独处理；
- 时间漂移/概念漂移会让指标低于随机切分；
- 分位点是近似值，跨并行度可能有细微差异；
- 不能用测试期信息做特征/调参，否则仍然泄漏。

### 10.4 特征泄漏
项目特征表由全量 DWD 构建，但**不作为 ALS 的输入**；ALS 只读 DWD 行为。这样避免“用未来行为构造的特征”进入训练。若以后接监督式冷启动模型，必须在训练时间窗内重建特征。

---

## 11. Benchmark 方法论

### 11.1 固定变量
一个可信的 benchmark 至少固定：
- 数据与输入路径；
- Spark 版本、JVM、Python；
- master/并行度；
- shuffle partitions；
- AQE 开关；
- action 类型（`collect` vs `count`）；
- 运行顺序与测量轮次。

项目把这些写入 `benchmark_results.json` 的 `environment`，避免“配置漂移”。

### 11.2 预热与交错
第一次运行包含 JVM 启动、JIT、类加载、代码生成、缓存预热；第二次更接近稳态。项目：
```text
U1 (warmup) -> F1 (warmup) -> H1 (warmup) -> U2 (measured) -> F2 (measured) -> H2 (measured)
```
交错执行让三种方案都经历相似预热，避免“某个方案连续运行两次”获得额外优势。

### 11.3 Wall clock vs Task duration
- **Wall clock**：端到端，包含提交、调度、shuffle、collect、Driver 开销；
- **最长 Task**：暴露长尾/倾斜，但只代表一个 task；
- **Shuffle read/write**：解释成本来源；
- **Task 数/失败数**：判断是否有重试/ speculative execution。

只报 wall clock 可能掩盖倾斜；只报最长 Task 可能忽略总吞吐。项目两者都报。

### 11.4 Listener 采集
PySpark 3.5.1 没有 Python 侧 `SparkListener` 基类。项目写 Java `TaskTimingCollector extends SparkListener`：
- `onTaskEnd` 只统计 successful task；
- `TaskInfo.duration()` 毫秒；
- `TaskMetrics` 的 shuffle read/write bytes/records；
- 通过 `PYSPARK_SUBMIT_ARGS --driver-class-path` 挂到 Driver。

listener 编译失败时，benchmark 仍会记录 wall clock，但 Task 字段为 null，`collection_method=fallback_total_time_only`。不伪造 Task 数字。

### 11.5 方差与结论边界
本地 1M 行、`local[8]` 的 benchmark 方差可能很大；单次运行偏快或偏慢不能证明全局。项目运行两次并取第二次结果，但这仍然只是 **1 个进入统计的 measured sample**：没有重复采样、没有置信区间、也没有做显著性检验。表中 wall clock 和最长 Task 的百分比应理解为该次运行的方向性证据，不是精确效应量；Shuffle records、group 基数、Task 数是确定性结构指标，方向更稳。

结论必须带条件：

> 在这份可 map-side combine 的 `count/sum` 负载上，全量加盐和仅热点键加盐都更慢；这不等于加盐在所有场景都无用，也不能外推成“加盐对所有可 Combine 负载都是负优化”。选择性加盐能降低额外开销，但在本写法下仍多一次 shuffle。
>
> 三种方案的对比是真实的；但归因边界只到“本实现”：不加盐可能享受 Catalyst 对同 Key 两阶段聚合的合并，加盐方案则不能。这个公平性差异是加盐成本的一部分，不是被隐藏的变量。

反过来也要防止把结论用过头：不可 Combine 只是加盐可能有效的**必要条件，不是充分条件**。本项目没有对不可 Combine 场景做实证，不能拿这份负结果替那些场景背书；迁移到新负载时必须重跑同一套 skew probe 和指标对照。

---

## 12. 幂等、可复现与可审计

### 12.1 幂等
- DWD 主键去重；
- 静态 overwrite；
- 确定性盐；
- 生成器固定 seed；
- 固定依赖版本。

### 12.2 可复现
- `requirements.txt` 固定五个包；
- 项目内 JDK 17/winutils，不依赖系统 Java 21；
- benchmark 配置写入 JSON；
- 评估参数（rank/maxIter/alpha/split quantile/K）写入 JSON；
- 原始报错和修复记录在 `REPRODUCE_LOG.md`。

### 12.3 不可复现的部分
- `percentile_approx` 近似分位；
- 浮点/并行调度/文件系统顺序；
- 本机后台负载、JVM/JIT、磁盘缓存；
- 网络/代理。

因此文档只承诺“同一环境、同一数据、同一配置下可重复”，不承诺跨机器数字完全一致。

### 12.4 可审计
Dashboard 不实时计算指标，只读命令行产物 JSON；缺失时显示生成命令，不显示假 0。这样每个页面数字都能追溯到一次真实运行。

---

## 13. 原理 → 项目代码映射

| 原理 | 项目位置 |
|---|---|
| SparkSession/本地运行时 | `spark_jobs/common.py` |
| 惰性执行/Job/Stage | `run_pipeline.py`、`benchmark_skew.py`、`evaluate_als.py` |
| 窄/宽依赖、去重 shuffle | `spark_jobs/dwd_build.py` |
| 两阶段聚合/确定性盐/选择性加盐 | `spark_jobs/skew_aggregation.py`、`spark_jobs/dws_build.py` |
| 三方案对照/实际 group 基数 | `pipelines/benchmark_skew.py` 的 `run_approach_set()` |
| 盐桶敏感性 | `pipelines/benchmark_salt_sweep.py` |
| Catalyst/count 优化 | `pipelines/benchmark_skew.py` 的 `.collect()` |
| JVM SparkListener | `spark_jobs/jvm/TaskTimingCollector.java` |
| 窗口/ lag 特征 | `evaluation/features.py` |
| 时间切分/冷启动 | `evaluation/evaluate_als.py` |
| RankingMetrics | `evaluation/evaluate_als.py` |
| Parquet/分区/overwrite | `ods_ingest.py`、`dwd_build.py`、`dws_build.py` |
| 断言/对账 | `spark_jobs/quality.py`、`dwd_build.py`、`dws_build.py` |
| 只读产物 Dashboard | `dashboards/app.py` |

---

## 14. 延伸阅读

- Apache Spark 3.5.1 Tuning Guide：<https://spark.apache.org/docs/3.5.1/tuning.html>
- Apache Spark 3.5.1 SQL Performance Tuning：<https://spark.apache.org/docs/3.5.1/sql-performance-tuning.html>
- Apache Spark 3.5.1 Collaborative Filtering：<https://spark.apache.org/docs/3.5.1/ml-collaborative-filtering.html>
- RankingMetrics API：<https://spark.apache.org/docs/3.5.1/api/python/reference/api/pyspark.mllib.evaluation.RankingMetrics.html>
- Hu, Koren, Volinsky. *Collaborative Filtering for Implicit Feedback Datasets*. WWW 2008.
- Parquet 文件格式：<https://parquet.apache.org/docs/file-format/>

---

## 15. 总结

Spark 的性能表现，是执行计划、Shuffle 成本、数据分布与内存机制综合作用的结果；推荐模型的评估质量，则取决于时间切分、冷启动处理与候选口径的严谨性。本项目将这些底层逻辑落实到具体的代码、断言与可复现日志中，完成了一次完整的工程实践。
