# Ep25 experiment

本集讀取兩份內容相同、總 rows 相同的 Parquet input，只改輸出檔案數量，觀察小檔案如何改變
scan partitions、task 數與 task launch／排程的形狀。

## 實驗地圖

```text
同樣 960,000 rows、同樣欄位、同樣 query、同樣 Executors
        ↓
large-files input：8 個 Parquet files
        ↓ scan
少量 scan tasks，每個 task 處理較多資料

small-files input：160 個 Parquet files
        ↓ scan
大量短 scan tasks，4 個 task slots 要分許多批處理
```

每個 phase 都只讀取 `value` 欄位並做 `sum(value)`。最後的全域 aggregation 會建立一個小型彙總
Stage；判讀時要將它和主要的 Parquet scan Stage 分開，避免把尾端的結果合併誤認成小檔案成本。

## 固定條件與單一變因

- 一個 Master、兩個 Workers；每個 Worker 提供 2 cores、3 GiB memory。
- 一個 client-mode application；提交端保有 Driver，Driver UI 位於 `http://localhost:4040`。
- 兩個 Executors；每個 Executor 使用 2 cores、1024 MiB memory；合計 4 個 task slots。
- 兩份 input 都有 960,000 rows、相同 schema 與相同 deterministic values。
- `large-files` 以 8 個 output partitions 寫出；`small-files` 以 160 個 output partitions 寫出。
- 讀取時固定 `spark.sql.files.maxPartitionBytes=1m` 與
  `spark.sql.files.openCostInBytes=4m`。因為單一 Parquet file 的估算開檔成本已高於 1 MiB，
  planner 不會將多個小檔合併成同一個 scan partition，讓 160-file input 的 task 數可直接觀察。
- `spark.sql.adaptive.enabled=false` 只用來維持後續小型 aggregate Stage 的形狀穩定。
- 每個 action 完成後 application 都停留 420 秒，方便查看 completed UI。

`maxPartitionBytes=1m` 是特意縮小的觀察條件，**不是**一般 production 的設定建議。真實環境通常
由檔案大小、預設 128 MiB 上限、open cost、儲存系統與可用 cores 共同決定一個 scan partition
會包含幾個 files。

## 檔案與執行順序

1. `prepare_experiment.sh`：建立 `ep25-shared-data` volume，重建兩個 Ep25 Workers，並用短暫的
   local-mode writer 生成兩份 Parquet input。它會移除 Ep15、Ep17、Ep22、Ep23、Ep24、Ep25 的已知
   Worker containers；只能在沒有 application 使用它們時執行。
2. `submit_small_file_observer.sh`：提交 client-mode application。主要程式位於
   `small_file_observer.py`；它依序讀取 large-files、等待、讀取 small-files、再等待。

## 收集證據後怎麼判讀

| 證據 | 這次可以確認什麼 | 還不能直接下的結論 |
| --- | --- | --- |
| files 和 planned scan partitions 同時增加 | input layout 直接讓 Spark 產生更多 scan tasks。 | 每個 file 必然對應一個 task；實際 planner 會受 max partition bytes 與 open cost 影響。 |
| 很多短 task、分許多批取得 task slots | task launch 與排程開銷相對於單一 task 的資料處理變得重要。 | 每個短 task 都是壞事；互動查詢或足夠大的 cluster 可能有不同取捨。 |
| 總 Input 接近，但 task 數差很多 | 問題來自 file layout，而非總資料量本身。 | 只靠調整 read-side setting 就能永久解決；通常還要回到上游輸出與 compaction。 |

本集先建立證據與判讀順序。輸出檔案 partition 設計、compaction 與 table format 的實務策略，會在
確認小檔案症狀後再回到實際 pipeline 討論。
