# Ep12｜Iceberg 為什麼重新定義一張表？從 Hive-style table 到 modern table format

上一集我追著 Hive Metastore 看 table name、schema 與 location 如何跨 SparkSession 被重新找到，也看見 `DROP TABLE` 後，managed table 與 external table 的實體檔案可能有不同的生命週期。那次實驗讓我理解「從名稱找到檔案」的路徑；但當一張表每天持續寫入、欄位持續增加、partition 規則也必須調整時，誰來保存「此刻這張表究竟由哪些檔案組成」？

Iceberg 是一種業界常用的 **table format**。它的核心理念是讓 table metadata 可以被版本化：每次 table state 改變時，將 schema、partition spec、snapshot 與檔案清單的關係提交成新的 metadata state。它沿用 catalog、storage 與 Parquet 等 data file format，並在 table root 增加這套設計。這次我用最小的本機實驗，依序新增資料、欄位與 partition field，觀察 snapshot、schema evolution、partition evolution 與 time travel 如何連在一起。

## 讀完這篇後你會更了解……

- Iceberg table 在 catalog、`metadata/` 與 `data/` 三層各自保存什麼資訊。
- snapshot、manifest list、manifest 與 data file 如何讓一次查詢找到正確的一組檔案。
- 為什麼新增欄位後，舊 Parquet file 不需要重寫，仍能以同一張 table 被讀取。
- `spec_id` 如何讓新舊 partition rules 下寫出的 files 共存。
- Iceberg 與 Hive-style table 面對 schema 與檔案版本管理時，分別提供什麼能力。

## 先畫出全貌：Iceberg 多加的是 table metadata layer

先從 Ep10 的資料儲存地圖出發。Iceberg 沒有移除 catalog，也沒有取代底層的 object storage、HDFS 或 Parquet。它增加的是每張 table 自己的 metadata layer：catalog 先從 table name 找到 table root；table root 裡的 metadata 再告訴 query engine 應讀取哪一批 data files。

```text
Spark SQL / DataFrame API
  ↓
catalog
  └─ ice.analytics.events → table root
       ↓
warehouse/analytics/events/
├── metadata/  # table metadata、snapshot 與 file inventory
└── data/      # 真正的 Parquet data files
```

這次實驗使用 Hadoop catalog，所以沒有另外啟動 Hive Metastore service；`ice` catalog 直接以 warehouse 路徑保存 table。正式環境也可以使用 Hive catalog、REST catalog 或其他 catalog 實作。此時 catalog 的角色仍相同：由 `ice.analytics.events` 找到這張 Iceberg table 的位置。Iceberg 的 `metadata/` 目錄不等於 metastore；它保存的是**這一張表**自己的進階 metadata。

> *catalog 負責由名稱找到 table；Iceberg metadata 負責由 table state 找到正確的 files。*

## `metadata/` 與 `data/` 是同層目錄，靠路徑彼此連結

建立 table、寫入資料並做幾次演進後，這次本機實驗的 table root 會長得像這樣：

```text
warehouse/
└── analytics/
    └── events/
        ├── data/
        │   ├── 00000-....parquet
        │   ├── 00001-....parquet
        │   └── 00002-....parquet
        └── metadata/
            ├── v1.metadata.json
            ├── v2.metadata.json
            ├── ...
            ├── version-hint.text
            ├── snap-....avro
            └── ....-m0.avro
```

`data/` 裡的 Parquet files 保存真正的 event rows。

`metadata/` 裡則有不同類型的索引檔。`vN.metadata.json` 是一份可讀的 JSON table metadata；`snap-....avro` 和 manifest files 通常是 Avro 二進位 metadata，不保存大量 rows。這些檔案之間的箭頭代表「內容記錄了下一個檔案的路徑」，不是目錄巢狀關係：

```text
metadata/vN.metadata.json
  └─ 記錄 snapshot S3 使用的 manifest list 路徑
       ↓
metadata/snap-S3.avro
  └─ 記錄 manifest A、manifest B 的路徑
       ↓
metadata/manifest-A.avro
  └─ 記錄 data file 的路徑、partition、筆數、欄位統計
       ↓
data/00000-....parquet
data/00001-....parquet
```

第一次 append 後，我直接列出本機 table root。以下省略每次執行都不同的 UUID 與 macOS 產生的 `.crc` 檔案；第一個 Parquet file 位在 `data/`，metadata files 則位在同層的 `metadata/`：

```bash
Files below: .../warehouse/analytics/events
 - data/event_ts_day=2026-10-01/00000-2-....parquet
 - metadata/....-m0.avro
 - metadata/snap-3856788153838660393-1-....avro
 - metadata/v1.metadata.json
 - metadata/v2.metadata.json
 - metadata/version-hint.text
```

可以把 manifest 想成一批貨物的明細單。它列出哪些 data files 屬於這批資料，以及每個 file 的 record count、partition 值、欄位 min/max 等統計。query engine 能先閱讀較小的 metadata，排除不符合條件的 files，再讀真正的 Parquet rows。

| 元件 | 它保存的內容 |
| --- | --- |
| snapshot | 某次 commit 後，table 應看見的一組檔案狀態。 |
| manifest list | 該 snapshot 使用哪些 manifests。 |
| manifest | 一批 data files 的位置、partition、筆數與統計。 |
| data file | 真正的 rows，例如 Parquet、ORC 或 Avro。 |

## `vN.metadata.json` 的版本，和 schema、snapshot 是不同維度

一開始看到 `v1.metadata.json`、`v2.metadata.json`，我很容易把它當成 schema v1、schema v2。實際上，`vN` 表示的是**整張 table metadata 的版本**。每次成功提交 table-level change，Iceberg 會寫出一份新的 metadata JSON，保存當時完整的 table state。

概念上，這次實驗的變化順序如下：

```text
建立 table
  → v1.metadata.json

初次 append
  → 新 snapshot，新的 metadata version

ADD COLUMN source
  → schema history 增加 schema-id，新的 metadata version

append 一筆含 source 的資料
  → 新 snapshot 與新 data file，新的 metadata version

ADD PARTITION FIELD bucket(4, event_id)
  → partition spec history 增加 spec-id，新的 metadata version

再次 append
  → 新 snapshot、新 data file，新的 metadata version
```

因此，一份較新的 metadata JSON 會同時持有 schema history、partition spec history、snapshots 與 `main` 等 refs。它可抽象成這樣：

```text
vN.metadata.json
├── schemas
│   ├── schema-id: 0：event_id, event_ts, region
│   └── schema-id: 1：event_id, event_ts, region, source
│
├── partition-specs
│   ├── spec-id: 0：days(event_ts)
│   └── spec-id: 1：days(event_ts), bucket(4, event_id)
│
├── snapshots
│   ├── snapshot A ──→ manifest list A
│   └── snapshot B ──→ manifest list B
│
└── refs
    └── main ──→ 目前 snapshot
```

這份完整 metadata 的寫入成本，換來的是一次 commit 的原子切換：讀者會看到完整的舊 table state 或完整的新 table state。高頻寫入的 table 確實可能累積很多 metadata、manifest 與過期 snapshots，因此正式環境需要設定 metadata retention，並定期執行 snapshot expiration 和 orphan-file cleanup。time travel 可查詢的時間範圍也會受這些 retention policy 限制；它適合版本追查與資料驗證，不能取代長期備份策略。

## 第一個實驗：snapshot 讓我回到當時的一組資料

實驗先建立以 `days(event_ts)` 分區的 Iceberg table，寫入兩筆初始事件：

```python
TABLE_NAME = "ice.analytics.events"

spark.sql(
    f"CREATE TABLE {TABLE_NAME} ("
    "event_id BIGINT, event_ts TIMESTAMP, region STRING"
    ") USING iceberg PARTITIONED BY (days(event_ts))"
)

append_rows(
    [
        (1, datetime(2026, 10, 1, 9), "TW"),
        (2, datetime(2026, 10, 1, 10), "JP"),
    ],
    ["event_id", "event_ts", "region"],
)
```

每次 append 成功後，Iceberg 會有新的 snapshot。這個 metadata table 讓我查看 snapshot ID、提交時間與操作：

```python
snapshots = spark.sql(
    f"SELECT snapshot_id, committed_at, operation "
    f"FROM {TABLE_NAME}.snapshots ORDER BY committed_at"
)
snapshots.show(truncate=False)
```

後面即使繼續 append 資料，仍可指定第一個 `snapshot_id` 讀取當時的 table state：

```python
original_rows = (
    spark.read
    .option("snapshot-id", first_snapshot_id)
    .table(TABLE_NAME)
    .orderBy("event_id")
    .collect()
)
```

實際輸出中，目前 table 有 4 筆 rows；指定第一個 snapshot ID 後，只讀到最初的 2 筆：

```bash
current rows: [
  Row(event_id=1, ..., region='TW', source=None),
  Row(event_id=2, ..., region='JP', source=None),
  Row(event_id=3, ..., region='US', source='mobile'),
  Row(event_id=4, ..., region='TW', source='web')
]
rows at first snapshot 3856788153838660393: [
  Row(event_id=1, ..., region='TW', source=None),
  Row(event_id=2, ..., region='JP', source=None)
]
```

snapshot ID 會隨每次執行而不同。關鍵在於 snapshot 決定**這次讀取要納入哪些 files**；它不是把 rows 複製到 JSON 裡，也不靠 data file 名稱判斷版本。

如果只想用 SQL 查詢舊版本，也能使用：

```sql
SELECT *
FROM ice.analytics.events VERSION AS OF 1234567890123456789;
```

這是查詢歷史資料的方式。若要讓預設讀取的 table 正式回到某個 snapshot，應使用 Iceberg 的 rollback procedure；不要手動修改 `version-hint.text`。後者是 Hadoop catalog 找尋目前 metadata 的內部提示，不是使用者管理版本的介面。

## 第二個實驗：schema evolution 用 field ID 連起新舊檔案

接著我新增 `source`：

```python
spark.sql(f"ALTER TABLE {TABLE_NAME} ADD COLUMN source STRING")

append_rows(
    [(3, datetime(2026, 10, 2, 9), "US", "mobile")],
    ["event_id", "event_ts", "region", "source"],
)

spark.table(TABLE_NAME).orderBy("event_id").show()
```

實際輸出中，初始兩筆資料的 `source` 是 `NULL`，新寫入的第三筆資料是 `mobile`。這個結果代表先前的 Parquet data files 沒有為了新增欄位而重寫。

```bash
+--------+-------------------+------+------+
|event_id|event_ts           |region|source|
+--------+-------------------+------+------+
|1       |2026-10-01 09:00:00|TW    |null  |
|2       |2026-10-01 10:00:00|JP    |null  |
|3       |2026-10-02 09:00:00|US    |mobile|
+--------+-------------------+------+------+
```

Iceberg 不會把每個 data file 綁定到一個單一的 schema version。它以穩定的 **field ID** 辨識欄位身份，schema history 則保存這些 ID 在各次演進中的定義：

```text
schema history
├── field id 1：event_id
├── field id 2：event_ts
├── field id 3：region
└── field id 4：source

舊 data file：field id 1、2、3
新 data file：field id 1、2、3、4
```

查詢目前 schema 時，舊 file 缺少 field ID 4，Iceberg 將 `source` 投影成 `NULL`；新 file 則讀取實際值。這是 schema evolution 的核心：**snapshot 決定讀哪些 files，schema 與 field ID 決定如何解讀那些 files 的欄位。**

## 第三個實驗：partition evolution 讓舊、新分區規則共存

最後我加入第二個 partition field，再寫入第四筆資料：

```python
spark.sql(
    f"ALTER TABLE {TABLE_NAME} "
    "ADD PARTITION FIELD bucket(4, event_id)"
)

append_rows(
    [(4, datetime(2026, 10, 3, 9), "TW", "web")],
    ["event_id", "event_ts", "region", "source"],
)
```

這次我直接查看 Iceberg 的 `files` metadata table：

```python
spark.sql(
    f"SELECT spec_id, partition, record_count, file_path "
    f"FROM {TABLE_NAME}.files ORDER BY spec_id, file_path"
).show(truncate=False)
```

實際輸出顯示，先前的 files 使用 `spec_id = 0`：只有日期 partition，bucket 位置為 `null`；最後寫入的 file 使用 `spec_id = 1`，同時帶有日期與 `event_id` bucket：

```bash
+-------+------------------+------------+-------------------------------------------------------+
|spec_id|partition         |record_count|file_path                                              |
+-------+------------------+------------+-------------------------------------------------------+
|0      |{2026-10-01, null}|2           |.../data/event_ts_day=2026-10-01/00000-2-....parquet |
|0      |{2026-10-02, null}|1           |.../data/event_ts_day=2026-10-02/00000-7-....parquet |
|1      |{2026-10-03, 2}   |1           |.../data/event_ts_day=2026-10-03/event_id_bucket_4=2/|
|       |                  |            |00000-13-....parquet                                  |
+-------+------------------+------------+-------------------------------------------------------+
```

Iceberg 因此能知道每個 file 的 partition 值該採用哪一份 partition spec 解讀。

```text
舊 data files ── spec-id 0 ──→ days(event_ts)
新 data files ── spec-id 1 ──→ days(event_ts), bucket(4, event_id)
```

這裡的 `spec_id` 管的是分區規則，不能把它當成 schema version。兩種演進分別處理不同問題：field ID 讓欄位身份跨 schema 留下來；spec ID 讓 partition 值跨規則正確解讀。

## 回到實務：同一張 table 可以演進，仍需要 schema contract

這讓我想到團隊處理 feature engineering 實驗時遇過的 schema update 問題。在 Hive-style table 的使用方式中，新增欄位常讓我們傾向建立全新的 table，並將 schema version 編進 table name。下游使用者需要知道應該改讀哪一張表，查詢程式與文件也跟著增加維護成本。

另一個使用 Iceberg 的後端資料來源則有不同的使用經驗。tracking events 持續增加欄位，下游仍引用同一張 table。Iceberg 將欄位演進放在 table metadata，舊 data files 保留，未寫入的新欄位讀出時為 `NULL`；新資料則能寫入該欄位。

| 面向 | Hive-style table 的常見做法 | Iceberg table format |
| --- | --- | --- |
| table metadata | catalog / metastore 保存 schema、provider、location 等基本資訊。 | catalog 找到 table root；table metadata 另外保存 schema history、snapshot、partition specs 與 manifests。 |
| 新增欄位 | Hive 與 Spark 可支援部分 schema 變更；實務上仍常受資料契約、目錄式分區與跨 job 相容性影響。 | 以 field ID 管欄位演進，新增欄位通常無須重寫舊 data files。 |
| 改變分區規則 | 常需要規劃新路徑、重寫資料或建立新 table。 | 以 `spec_id` 讓使用不同 partition specs 的 files 共存。 |
| 一致的 table view | 依 engine 與寫入流程管理檔案與 metastore 更新。 | snapshot 將一次提交的 table state 明確記錄下來。 |
| 查詢舊版本 | 需由外部備份、路徑或自訂流程管理。 | 在 retention 範圍內可用 snapshot ID 或 timestamp 做 time travel。 |

這不代表下游可以忽略 schema contract。新欄位的語意、nullability、資料品質與何時開始有值，仍然需要和使用者溝通；Iceberg 管理的是 table metadata 與檔案相容性，不會替團隊決定欄位的業務意義。

> *Iceberg 讓 table name 可以持續代表同一份資料實體；schema、partition 與資料版本的演進，則交給 table metadata 留下可追蹤的軌跡。*

下一篇會走到真正承載 rows 的 data files。Parquet、ORC、CSV、JSON 都能放在 storage 上，但它們如何保存 schema、如何被讀取，以及適合什麼工作情境，會直接影響 Spark 的讀取成本。

---

## 完整程式碼與參考資料

🚀 GitHub：\
[Ep12｜Iceberg 為什麼重新定義一張表？從 Hive-style table 到 modern table format](https://github.com/jeffery5bai/spark-learning-lab/tree/main/episodes/ep12-iceberg-table-format)

🚀 spark-learning-lab 系列文章與實作：\
[https://github.com/jeffery5bai/spark-learning-lab](https://github.com/jeffery5bai/spark-learning-lab)

Apache Iceberg 1.5.2 — Evolution：\
[https://iceberg.apache.org/docs/1.5.2/evolution/](https://iceberg.apache.org/docs/1.5.2/evolution/)

Apache Iceberg 1.5.2 — Spark Writes：\
[https://iceberg.apache.org/docs/1.5.2/spark-writes/](https://iceberg.apache.org/docs/1.5.2/spark-writes/)

Apache Iceberg 1.5.2 — Maintenance：\
[https://iceberg.apache.org/docs/1.5.2/maintenance/](https://iceberg.apache.org/docs/1.5.2/maintenance/)
