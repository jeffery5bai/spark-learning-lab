# Ep18 experiment

本集用最小的 Spark Standalone cluster 觀察 application 在失去一個 Executor 時的恢復路徑。
實驗以 4 個可由 lineage 重建的 `spark.range` partitions 建立長時間 tasks，並在 task 執行時
停止一個 Worker。剩餘 Executor 可重新執行未完成 partitions，讓 Driver 的 retry 路徑清楚可見。

## 實驗地圖

```text
Driver 提交一個有 4 個長時間 task 的 stage
        ↓
兩個 Executors 分別執行部分 tasks
        ↓
Worker 1 / Executor 1 消失
        ↓
Driver 偵測 Executor lost，標記未完成 task 失敗
        ↓
剩餘 Executor 重試失敗 task 的同一個 partition
        ↓
stage 與 application 完成
```

## 預計驗證的證據

1. Master UI：Worker 1 與其 Executor 消失後，可用 cores／memory 改變。
2. Driver UI：受影響 task 有失敗 attempt，stage 仍能完成。
3. Driver 或 Executor log：`ExecutorLostFailure`、task retry 與重新排程的記錄。
4. 剩餘 Executor：接手原先由消失 Executor 執行的 task。

## 本集範圍

- 使用兩個 Workers、每個 Worker 一個 2-core Executor。
- 使用可由 `spark.range` lineage 重建的 partitions，將失敗恢復聚焦在 task retry。
- 以受控方式停止一個 Worker，並在實驗後重新啟動它。
- 對照 Worker lost、Executor lost 與 task retry 的關係。

本集不驗證 Master HA、external shuffle service、dynamic allocation、block replication、cache
block 遺失後的 recomputation、memory spill 或 rack-level failure。

## 檔案與執行順序

先確認 Ep15 的 Master 已存在；接著依序使用：

1. `prepare_two_workers.sh`：確認 Worker 1 已啟動，並建立或啟動 Worker 2。
2. `submit_failure_observer.sh`：提交 client-mode application，讓兩個 Executors 同時執行
   4 個長時間 tasks。
3. `stop_worker_1.sh`：只在確認兩個 Executors 都已有 running tasks 後使用，停止 Worker 1
   來觸發受控 failure。
4. `restore_worker_1.sh`：application 完成觀察後，恢復 Worker 1，讓後續課程回到兩個
   Workers 的基準環境。

`failure_observer.py` 是 application 本體；每個初始 task attempt 會暫停 240 秒，提供足夠時間
截取 running stage。Worker 1 消失後的 retry 只暫停 15 秒，讓恢復結果能較快落到 Worker 2。

## 觀察順序

1. 啟動 application，確認兩個 Executors 都已取得 task。
2. 截 Master UI 與 Driver UI 的 running stage，確認兩個 Executors 已取得四個 tasks。
3. 停止 Worker 1。
4. 在 Driver UI 觀察 task attempt、stage 狀態與 Event Timeline。
5. 截 Master UI 與剩餘 Worker UI，確認可用資源和 Executor 數量變化。
6. 等待重試完成，截 completed stage 的 task table 與 logs。

只有 application 處於 running stage 時才停止 Worker。完成觀察後必須恢復 Worker，讓後續課程保有
兩個 Workers 的基準環境。
