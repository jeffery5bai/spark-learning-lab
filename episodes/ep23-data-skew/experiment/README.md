# Ep23 experiment

本集用同一份 synthetic data 跑兩個 action，只改 shuffle key 的分布，觀察 data skew 如何讓
一個 Stage 等待少數 task 完成。

## 實驗地圖

```text
同一批 600,000 rows、同一組 Executors、同樣 8 個 shuffle partitions
        ↓
baseline：64 個 key 平均分布
        ↓ repartition(8, "key")
8 個下游 tasks 收到相近 rows，完成時間接近

skew：大約 90% rows 使用同一個 hot key
        ↓ repartition(8, "key")
hot key 落入一個 partition；對應 task 收到遠多於其他 task 的 rows
        ↓
其他 task 已完成，Stage 仍等待這個 straggler
```

程式在 shuffle 後對每筆資料執行相同、固定的 Python CPU loop。這是讓小型筆電實驗中，
task duration 能隨收到的 rows 明顯拉開的**可見化裝置**；它不模擬某個特定商業運算，也不應
被帶進實際 production job。

## 固定條件與單一變因

- 一個 Master、兩個 Workers；每個 Worker 提供 2 cores、3 GiB memory。
- 一個 client-mode application；提交端保有 Driver，Driver UI 位於 `http://localhost:4040`。
- 兩個 Executors；每個 Executor 使用 2 cores、1024 MiB memory；合計 4 個 task slots。
- 每一個 phase 都有 600,000 rows、8 個來源 partitions、8 個 shuffle partitions，並對每列
  執行相同 CPU work。
- 唯一改變的是 `key` 的分布：baseline 用均勻 key；skew phase 讓約 90% rows 使用 key `0`。
- `spark.sql.adaptive.enabled=false` 只用來保留固定八個 post-shuffle partitions，使 UI 能直接
  對照資料分布；它不是一般工作負載的預設建議。
- 每個 action 完成後 application 都停留 420 秒，方便在 Driver UI 檢查 completed Job。

沒有獨立 input prep 步驟。資料由 `spark.range` 在 application 中生成，避免 input files、scan
partition planning 等額外變因混入這次比較。

## 檔案與執行順序

1. `prepare_workers.sh`：重建兩個 Ep23 Workers。它會移除 Ep15、Ep17、Ep22、Ep23 的已知
   Worker containers；只能在沒有 application 使用那些 Workers 時執行。
2. `submit_skew_observer.sh`：提交 client-mode application。實驗邏輯位於
   `skew_observer.py`；它依序執行 baseline、等待、skew、再等待。

## 觀察清單

### Baseline 完成後的第一次 420 秒等待

1. **Driver UI → Jobs**：找到描述為 `baseline: redistribute by uniform_key` 的 completed Job，記錄它的 Stage 數與 duration。
2. **Driver UI → post-shuffle Stage details**：截 Event Timeline 與 Tasks table。保留
   Duration、Shuffle Read Size、Records 欄位，確認八個 tasks 的資料量與時間相近。
3. **Driver UI → Executors**：截兩個 Executors 的 task 數與基本指標，作為資源分布背景。

### Skew 完成後的第二次 420 秒等待

1. **Driver UI → Jobs**：找到描述為 `skew: redistribute by skew_key` 的 completed Job。
2. **Driver UI → post-shuffle Stage details**：截 Event Timeline 與 Tasks table。依
   Duration、Shuffle Read Size 或 Records 排序，保留能看到一個 task 仍在執行、其他 task 已
   完成的畫面。
3. **Driver UI → Executors**：截 Executors 頁，確認哪個 Executor 執行了較多工作；再回到
   task table 對照，避免只用 Executor 彙總數字推論原因。

截圖時保留頁面標題、application 名稱、Stage description 與欄位名稱。最重要的對照是同一套
資源下，baseline 與 skew 的 post-shuffle Tasks table／timeline。

## 收集證據後的判讀範圍

若 skew phase 出現單一 task 同時有較多 records、較大的 shuffle read 與較長 duration，這次
實驗能支持的因果鏈是：**key 分布集中 → shuffle partition 資料不均 → task 工作量不均 →
Stage 等待 straggler**。

它還不能直接回答如何修任何真實 job。真實資料還要先確認 skew 來自哪一個 join、aggregation
或 partition key；AQE skew join、salting 與 join strategy 會留在後續合適的實驗處理。
