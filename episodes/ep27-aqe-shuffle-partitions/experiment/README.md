# Ep27 experiment

本集比較同一個 aggregation 在三種設定下如何使用 shuffle partitions。核心問題是：AQE 開啟後，
`spark.sql.shuffle.partitions` 是否還有意義？答案是它仍提供初始切分；AQE 取得 map-side shuffle
統計後，才有機會將連續的小 partitions 合併成較少的下游 tasks。

## 實驗地圖

```text
1,200,000 筆均勻分布 records
        ↓ groupBy(group_key) + sum + max(payload)
16 個 map tasks 寫出初始 32 份 shuffle blocks
        ↓
Phase 1：AQE 關閉
32 個下游 shuffle-read tasks

Phase 2：AQE 開啟，但 coalescing 關閉
仍是 32 個下游 shuffle-read tasks

Phase 3：AQE 與 coalescing 開啟
Spark 依 map output statistics 與 16 MiB advisory size
合併連續的小 partitions，使用較少的下游 tasks
```

每筆資料帶有依 `id` 改變的 96-character payload，並以 `max(payload)` 保留在 aggregation state。
這讓 map-side partial aggregate 有足夠資料可寫入 shuffle。keys 均勻分布，實驗不探討 skew；AQE
skew join 留給需要 sort-merge join 的專屬情境。

## 固定環境與設定

- 一個 Master、兩個 Workers；每個 Worker 提供 2 cores、3 GiB memory。
- 一個 client-mode application；Driver UI 位於 `http://localhost:4040`。
- 兩個 Executors；每個 Executor 使用 2 cores、512 MiB heap；合計 4 個 task slots。
- 三個 phase 都設定 `spark.sql.shuffle.partitions=32`，作為初始 shuffle partitions。
- Phase 1 使用 `spark.sql.adaptive.enabled=false`。
- Phase 2 使用 `spark.sql.adaptive.enabled=true`，但
  `spark.sql.adaptive.coalescePartitions.enabled=false`。
- Phase 3 開啟 AQE 與 post-shuffle coalescing，並設定：

  ```text
  spark.sql.adaptive.coalescePartitions.initialPartitionNum=32
  spark.sql.adaptive.advisoryPartitionSizeInBytes=16m
  spark.sql.adaptive.coalescePartitions.parallelismFirst=false
  spark.sql.adaptive.coalescePartitions.minPartitionSize=1m
  ```

`parallelismFirst=false` 讓本次實驗尊重 16 MiB advisory size。Spark 3.4.4 官方文件說明預設的
`parallelismFirst=true` 會優先維持平行度，並可能忽略 advisory size；官方建議想以 advisory size
作為 tuning 目標時將它設為 `false`。

實驗另固定 `spark.shuffle.compress=false`，讓各 phase 的 shuffle bytes 較容易比較。它只是觀察條件，
不構成一般 production 的建議。

## 檔案與執行順序

1. `prepare_workers.sh`：重建兩個 Ep27 Workers。它會移除 Ep15、Ep17、Ep22–Ep27 的已知 Worker
   containers；只能在沒有 application 使用它們時執行。
2. `submit_aqe_partition_observer.sh`：提交 application。三個 phase 完成後，Driver 會停留 420 秒，
   讓 completed Jobs、SQL plan 與 Stage metrics 保留在 UI 中。

## 觀察清單

三個 phase 全部完成後再開始截圖，不需要在 phase 間停下：

1. **submitter log**：保留 records、source partitions、initial shuffle partitions、advisory partition
   size，以及每個 phase 的 AQE／coalescing 設定與 result partitions。
2. **Jobs 頁**：保留三個 phase 的 Job description、Stage 數與 task 數。先確認每個 action 對應哪一組設定。
3. **Phase 1 Job details**：截 Job DAG 與 Completed Stages，記錄初始 32 partitions 對應的下游 task 數。
4. **Phase 2 Job details**：截同樣資訊，確認單獨開啟 AQE、但關閉 coalescing 時不應把 partitions 合併。
5. **submitter log + Phase 3 Job details**：從 submitter log 截 final plan 中的
   `AQEShuffleRead`／`coalesced` 訊號，並從 Job details 截 Completed Stages。記錄下游實際 task 數和
   Phase 1、2 的差異。
6. **Phase 3 downstream Stage details**：截 task timeline、Shuffle Read 與 task duration；它用來確認合併後
   的 tasks 仍能在兩個 Executors、四個 task slots 上正常分批執行。

## 預期與限制

- Phase 1、2 的下游 task 數應維持初始 partition 數附近；Phase 3 應較少。實際合併後的 task 數由
  map output statistics、advisory size、最小 partition size 與 Spark 版本共同決定，不應事先假定為某個固定數字。
- AQE 只能合併已經偏小的 post-shuffle partitions；它不能將一個過大的 partition 自動切成更多一般
  partitions。本集不把 AQE skew join 混入討論。
- 本次在筆電上的 phase duration 只用來理解執行形狀，不用來宣稱某個設定在所有 production job 都更快。
- 真實設定前先看 task data size、shuffle bytes、cluster cores 與 query latency；以足夠大的 initial
  partition count 提供 AQE 合併空間，再以 production metrics 驗證。
