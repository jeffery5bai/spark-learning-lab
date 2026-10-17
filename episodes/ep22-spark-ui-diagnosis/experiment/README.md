# Ep22 experiment

本集沿用 Ep15 的 Podman Spark Standalone cluster，建立一個可重跑的 scan-and-shuffle
application，練習從 Spark UI 為「job 變慢」收集第一輪證據。

## 實驗地圖

```text
共享 Parquet input
  ↓ read + filter
scan stage
  ↓ repartition(8, "category")
shuffle stage
  ↓ groupBy + collect action
completed Job、Stages、task timeline、Executor metrics
```

這是診斷基準，不刻意製造 skew、OOM、spill 或小檔案。它的任務是讓我熟悉每個 UI 指標
回答的問題；Ep23 到 Ep26 再分別以單一變因製造明確症狀。

## 固定條件

- 一個 Master、兩個 Workers；每個 Worker 提供 2 cores、3 GiB memory。
- 一個 client-mode application；提交端保有 Driver，Driver UI 位於 `http://localhost:4040`。
- 兩個 Executors；每個 Executor 使用 2 cores、1024 MiB memory。
- 共享 volume 上有 800,000 rows 的 Parquet input；application 讀取後以
  `repartition(8, "category")` 建立一個明確的 shuffle 邊界。
- `spark.sql.adaptive.enabled=false` 只用來維持明確要求的八個 shuffle partitions，讓 UI
  的數字能直接對照程式；它不是一般工作負載的預設建議。
- action 完成後 Driver 暫停 420 秒。這段時間不會再建立新工作，適合查看 completed UI。

小型 Parquet files 可能被 Spark 合併成較少的 scan tasks，因為 file scan partition planning
取決於檔案大小與設定。本集只將 scan stage 的 Input 當成診斷訊號，不用 scan task 數證明
平行度；八個 post-shuffle tasks 才是本實驗穩定的對照基準。

## 檔案與執行順序

1. `prepare_shared_workers.sh`：建立 `ep22-shared-data` volume，並重建兩個 Ep22 Workers。
   這個腳本會移除 Ep15、Ep17、Ep22 的已知 Worker containers；只能在沒有 application 使用
   那些 Workers 時執行。
2. `prepare_input.sh`：以短暫的 local-mode writer 建立共享 Parquet input。它不會向
   Standalone Master 提交 application；資料生成邏輯位於 `prepare_input.py`。
3. `submit_diagnosis_observer.sh`：提交 client-mode application。application 邏輯位於
   `diagnosis_observer.py`，在 `collect()` 完成後保留 420 秒供 UI 觀察。

## 觀察清單

application 完成 action 並進入 420 秒等待後，依序收集：

1. **Master UI**：`Alive Workers` 為 2，application 使用兩個 Executors、4 cores、2 GiB
   memory。這是資源配置的背景，不用它判斷 Spark task 的計算瓶頸。
2. **Driver UI → Jobs**：截一張完整 Job 與 DAG。先記錄 duration，並辨識 scan 與 shuffle
   對應的 stages。
3. **Driver UI → Stages**：分別開啟 scan stage 與包含 `repartition` 的 stage，截 Summary
   Metrics。記錄 Input、Shuffle Read、Shuffle Write 與 task 數。
4. **Driver UI → Stage details**：在 post-shuffle stage 截 Tasks table 或 Event Timeline，記錄
   task duration 是否平均、是否出現明顯落後 task。這次不預期出現 skew；它是後續比較基準。
5. **Driver UI → Executors**：截每個 Executor 的 task 數、Input、Shuffle Read／Write、GC time、
   memory／disk spill。這次不預期 spill；零或很低的數值就是基準的一部分。

截圖時保留頁面標題、application 名稱與關鍵數字，避免只截局部表格而失去判讀上下文。

## 收集證據後怎麼判讀

| UI 證據 | 現階段能先問的問題 | 深入驗證的課程 |
| --- | --- | --- |
| 少數 task 特別慢 | 是否存在 key 或 partition 的資料偏斜？ | Ep23 |
| shuffle read／write 很高 | 哪個 transformation 或 join 建立了資料交換？ | Ep24 |
| task 很多且單個 task 很短 | 是否被大量小檔案與 task launch overhead 主導？ | Ep25 |
| GC time、memory spill 或 disk spill 高 | Executor memory、partition 與資料量如何互動？ | Ep26 |

本集只建立問題與證據的連結。看到某個訊號後，下一步是驗證對應假設，不立刻套用通用設定。
