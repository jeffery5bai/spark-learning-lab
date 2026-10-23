# Ep26 experiment

本集建立一個 sort-based shuffle，刻意讓 map-side shuffle sorter 在受控記憶體壓力下 spill，觀察
Spark UI 中的 memory spill、disk spill、GC time 與 shuffle 指標要如何一起讀。

## 實驗地圖

```text
2,400,000 個均勻分布、帶有 128-byte payload 的 key-value records
        ↓ sortByKey(8)
8 個 map tasks：建立 sorter、寫出 shuffle data
        ↓ 受控的低 execution memory
sorter 的 working set 超過 execution memory 後寫入 local disk（spill）
        ↓
8 個 reduce tasks：讀取、合併並排序 shuffle data
        ↓
每個 partition 回傳一個小型 summary
```

本實驗刻意不製造 data skew；每個 key 只出現一次，讓單一 task 的資料量大致均勻。我要隔離的是
shuffle sorting 的 spill 路徑，而不是重複 Ep23 的 straggler。

## 固定條件與觀察用設定

- 一個 Master、兩個 Workers；每個 Worker 提供 2 cores、3 GiB memory。
- 一個 client-mode application；提交端保有 Driver，Driver UI 位於 `http://localhost:4040`。
- 兩個 Executors；每個 Executor 使用 2 cores、512 MiB heap；合計 4 個 task slots。
- `sortByKey(8)` 將 2,400,000 records 重新分配並排序，形成一個清楚的 shuffle 邊界。
- 每筆 value 帶有 128 bytes、隨 record 改變的 binary payload；它避免重複字串被 shuffle compression 大幅壓縮，讓 sorter 必須處理足夠的中間資料。
- `spark.shuffle.sort.bypassMergeThreshold=0` 強制這個少量 reducer 的 shuffle 走 sort-based writer，
  而非 bypass merge writer。
- `spark.memory.fraction=0.1` 將 512 MiB Executor heap 中可交給 Spark unified memory 的比例刻意調低，
  讓 shuffle sorter 的 working set 在可控資料量下超過可用 execution memory，留下可觀察的 spill 指標。
- `spark.shuffle.compress=false` 與 `spark.shuffle.spill.compress=false` 讓實驗中的中間資料不被壓縮，
  使 disk spill 的大小與資料移動更容易從 UI 對照；兩者都不是 production 的通用建議。
- `spark.sql.adaptive.enabled=false` 只為維持 task 數與 UI 形狀穩定。
- action 完成後 Driver 停留 420 秒，方便查看 completed UI。

512 MiB executor heap、低 memory fraction 與受控 payload 的目的，是讓 laptop 實驗在可控範圍內留下 spill 指標。
**這不是刻意製造 OOM 的實驗**：看到 spill 只能確認中間資料有被寫入 local disk，不能單獨證明
Executor heap 已經耗盡；真實 OOM 還要看 Executor lost、container／process exit 與 logs。

## 檔案與執行順序

1. `prepare_workers.sh`：重建兩個 Ep26 Workers。它會移除 Ep15、Ep17、Ep22–Ep26 的已知 Worker
   containers；只能在沒有 application 使用它們時執行。
2. `submit_spill_observer.sh`：提交 client-mode application。主要程式位於 `spill_observer.py`；
   action 完成後會保留 420 秒供 UI 觀察。

## 收集證據後怎麼判讀

| 證據 | 這次可以確認什麼 | 還不能直接下的結論 |
| --- | --- | --- |
| Memory／Disk Spill 大於 0 | shuffle sorter 或 downstream merge 曾將中間資料寫到 local disk。 | 單靠 spill 就能證明 JVM heap 不夠，或應立刻加大 Executor memory。 |
| GC time 高且 task duration 拉長 | JVM 記憶體管理可能佔據工作時間，值得與資料量、spill、cache 一起看。 | GC 是唯一原因；也可能有 shuffle、storage、CPU 或 skew。 |
| Executor lost、exit code、OOM log | process 或平台層的記憶體限制可能被觸發。 | Spark 一定能靠 retry 完整恢復；還要看 shuffle files、重試與可用資源。 |
| 少數 task 的 spill／duration 特別高 | 可能回到 skew 或 partition 不均。 | 所有 memory 問題都是平均型資源不足。 |

這次強制 threshold 讓 spill 容易觀察；真實診斷時要先移除這個實驗條件，改用 production job 的
task data size、GC、spill、Executor logs 與平台資源限制形成假設。
