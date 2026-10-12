# Ep17 experiment

本集沿用 Ep15、Ep16 的 Podman Spark Standalone cluster，追蹤資料在 storage、Executor memory 與 Worker local disk 之間的位置。

## 實驗地圖

```text
共享的 Parquet input
  ↓ read
Executor scan task
  ↓ repartition(key)
各 Executor 的 local disk：shuffle intermediate files
  ↓ cache() + count()
各 Executor 的 Block Manager memory：cache blocks
```

## 固定條件

- 兩個 Workers；每個 Worker 提供 2 cores、3 GiB memory。
- 兩個 Executors；每個 Executor 使用 2 cores、1024 MiB memory。
- 可由兩個 Workers 讀取的共享 Parquet input。
- 一次明確的 `repartition("key")`，只引入一個 shuffle 邊界。
- shuffle 後的 DataFrame 呼叫 `cache()` 並以 action materialize。
- input 資料量保持小於 Executor memory，這一集不混入 disk spill。

## 預計驗證的證據

1. input Parquet files 保留在共享 storage 路徑，不會因為被讀取而搬到 Master。
2. shuffle 的中間 blocks 位於實際執行 task 的 Executor local disk。
3. `cache()` materialize 後，Storage tab 顯示 cache blocks 位於 Executors 的 memory。
4. 對同一份 cached DataFrame 再次 action 時，Spark 重用 cache blocks；它不需重新完成 upstream 的 scan 與 shuffle。

input scan 使用 Spark 預設的 file partition planning。小型 input files 可能被合併成較少的 scan tasks；本集不以 scan task 數驗證平行度。後續的 `repartition("key")` 會明確建立 shuffle partitions，作為兩個 Executors 資料分布的觀察基準。

## 檔案與執行順序

先確認 Ep15 的 Master 已啟動，且沒有其他 Spark application 正在使用 Workers。接著依序執行：

1. `prepare_shared_workers.sh`：建立 `ep17-shared-data` volume，並以掛載該 volume 的兩個
   Workers 重建 Ep17 實驗環境。這個腳本會移除同名的 idle Worker containers，因此不能在
   application 執行時使用。
2. `prepare_input.sh`：以短暫的 local-mode writer 在 shared volume 建立 80,000 rows、8 個
   Parquet files。它只建立 input，不會向 Standalone Master 提交 application；其資料生成邏輯
   位於 `prepare_input.py`。
3. `submit_shuffle_cache_observer.sh`：提交 client-mode application，申請兩個 2-core
   Executors。application 的觀察邏輯位於 `shuffle_cache_observer.py`，並有兩個可觀察階段：

   - `repartition(8, "key")` 建立 shuffle，`cache().count()` 實際寫入八個 cached
     partitions，然後保留 300 秒。
   - 對同一個 cached DataFrame 執行 `filter("key = 7").count()`，再保留 300 秒。
4. `inspect_worker_local_storage.sh`：在 application 的暫停期間唯讀列出兩個 Worker 上的
   `shuffle_*` 檔案。

程式設定 `spark.sql.adaptive.enabled=false`。這個設定只為了讓本實驗中明確指定的
八個 shuffle partitions 不被 AQE 合併，讓 UI 中的 partition 數可直接對照
`repartition(8, "key")`；它只服務本實驗的穩定對照，不作為一般工作負載的預設建議。

Worker-managed 的 `/tmp/spark-.../executor-.../blockmgr-.../` 是本次 Standalone Worker
為 Executor 建立的 temporary directory；其中的 `shuffle_*` 檔案確認中間資料留在
Executor 所在的 Worker local disk，不在 Master 或共享 input volume。

## 觀察清單

### Phase 1：第一次 `count()` 完成後

- Master UI：兩個 Executors 共用 4 cores、2 GiB。
- Driver UI 的 Jobs / Stages：確認 scan 上游 stage 與 `repartition(8, "key")` 形成的
  shuffle 邊界；Stage 詳情應有 Shuffle Read / Write metrics。
- Driver UI 的 Storage：同一個 cached RDD 有八個 partitions，blocks 分布在兩個
  Executors 的 memory。
- 兩個 Worker UI：各有一個 Executor。
- `inspect_worker_local_storage.sh` 的輸出：兩個 Worker-managed local directory 中的
  `shuffle_*` 檔案。

### Phase 2：第二次 action 完成後

- Driver UI 的 Jobs / Stages：出現新的 Job；比較其 DAG 與第一個 materialization Job，
  確認它從 cached blocks 開始，沒有重新執行 upstream scan 和 shuffle。
- Storage：cached RDD 與 blocks 仍存在，直到程式最後呼叫 `unpersist()`。

程式結束後 Executor 退出、local shuffle files 被清理、cache blocks 也被釋放。因此所有
截圖與 local-file 觀察都要在對應的 300 秒暫停期間完成。
