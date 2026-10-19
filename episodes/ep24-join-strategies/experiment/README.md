# Ep24 experiment

本集用同一份 fact table 與 dimension table 跑兩次 equi-join，只切換 Spark 採用的 join strategy，
觀察 broadcast join 和 shuffle join 在 physical plan 與 Spark UI 中留下哪些不同證據。

## 實驗地圖

```text
1,000,000-row fact table                2,000-row dimension table
product_id、event value                 product_id、segment
        │                                         │
        ├──────────── join on product_id ─────────┤
        │
        ├── broadcast phase：傳送小 dimension table 到 Executors
        │       BroadcastHashJoin；fact side 不為 join key shuffle
        │
        └── shuffle phase：兩側依 product_id 重分配並排序
                SortMergeJoin；兩側在 join 前都有 Exchange
```

兩個 phase 都使用相同的資料、相同的 join key、相同的 Executors 與相同的 action。這個實驗不追求
用很小的筆電資料證明「哪個策略快幾倍」；它要建立可重複檢查的證據鏈：physical plan 選了什麼、
資料在哪裡移動、Spark UI 的 Stage metrics 如何對應這個選擇。

## 固定條件與單一變因

- 一個 Master、兩個 Workers；每個 Worker 提供 2 cores、3 GiB memory。
- 一個 client-mode application；提交端保有 Driver，Driver UI 位於 `http://localhost:4040`。
- 兩個 Executors；每個 Executor 使用 2 cores、1024 MiB memory；合計 4 個 task slots。
- fact table 有 1,000,000 rows、8 個 source partitions；dimension table 有 2,000 rows、8 個
  source partitions；`product_id` 均勻分布，刻意不製造 skew。
- 每個 phase 都是同一個 inner equi-join，最後執行 `count()`。
- broadcast phase 用 `F.broadcast(dimension)` 明確指定小表可 broadcast。
- shuffle phase 將 `spark.sql.autoBroadcastJoinThreshold=-1`，並保留 Sort-Merge join 偏好，避免
  小表被自動 broadcast。
- `spark.sql.adaptive.enabled=false` 與 `spark.sql.shuffle.partitions=8` 只為讓 plan 與 task
  數穩定對照；它們不是 production 的通用設定建議。
- 每個 action 完成後 application 都停留 420 秒，方便查看 completed UI。

資料由 `spark.range` 生成，沒有 input files、cache 或額外 Python UDF；這些條件用來讓觀察焦點
停留在 join strategy 與 shuffle。

## 檔案與執行順序

1. `prepare_workers.sh`：重建兩個 Ep24 Workers。它會移除 Ep15、Ep17、Ep22、Ep23、Ep24 的
   已知 Worker containers；只能在沒有 application 使用那些 Workers 時執行。
2. `submit_join_observer.sh`：提交 client-mode application。主要程式位於
   `join_observer.py`；它依序執行 broadcast phase、等待、shuffle phase、再等待。

## 觀察清單

### Broadcast phase 完成後的第一次 420 秒等待

1. **submitter log 的 formatted plan**：找 `BroadcastHashJoin` 與 `BroadcastExchange`。確認小的
   dimension side 被 broadcast；不要只從 job duration 判斷策略。
2. **Driver UI → SQL / DataFrame**：開啟這次 execution，截能同時看到 `BroadcastHashJoin`、
   `BroadcastExchange` 與 join 條件 `product_id` 的 physical plan。
3. **Driver UI → Jobs → Job details**：截 DAG 與 Completed Stages。找 broadcast exchange，以及
   `count()` 最後彙總造成的 Stage；記錄 fact-side join 前是否有為 `product_id` 建立的 Exchange。
4. **Driver UI → Stages**：截主 join Stage 的 Summary Metrics 或 Tasks table，保留 task 數和
   Input／Shuffle Read／Shuffle Write 欄位。

### Shuffle phase 完成後的第二次 420 秒等待

1. **submitter log 的 formatted plan**：找 `SortMergeJoin`，並找 join 左右兩側各自的
   `Exchange hashpartitioning(product_id, 8)` 與 `Sort`。
2. **Driver UI → SQL / DataFrame**：截相同範圍的 physical plan，和 broadcast phase 對照。
3. **Driver UI → Jobs → Job details**：截 DAG 與 Completed Stages。找 join 前的 Exchanges，並
   比較相關 Stages 的 Shuffle Read／Write。
4. **Driver UI → Stage details**：截 shuffle read 的 Tasks table 或 Event Timeline。這次 key
   均勻分布，不預期有 Ep23 那種單一 straggler。

截圖時保留 application 名稱、phase description、operator 名稱與關鍵 metrics。最重要的是兩張
physical plan，以及兩個 Job DAG／Completed Stages 的對照。

## 收集證據後怎麼判讀

| 證據 | 這次可以先確認什麼 | 還不能直接下的結論 |
| --- | --- | --- |
| `BroadcastHashJoin` + `BroadcastExchange` | dimension side 被複製到各 Executors；fact side 不需為 join key 重分配。 | 小表永遠可安全 broadcast。還要看實際 serialized size 與每個 Executor 的 memory。 |
| `SortMergeJoin` + 兩側 `Exchange` | join 前兩側資料都按 key 重分配，shuffle 是這個策略的明確成本。 | 所有 shuffle join 都很慢，或換任何設定都能避免它。 |
| shuffle read／write metrics | 哪些 Stage 真正在讀寫 join 的 intermediate data。 | metrics 高一定代表 join 是唯一瓶頸；仍須看資料量、task 分布與其他 operators。 |

完成觀察後，先用 plan 和 UI 證據說明 Spark 選了什麼策略、資料如何移動；修正策略或設定要等回到真實
table size、統計資訊與 Executor memory 後再討論。
