# Ep13｜Parquet、ORC、CSV、JSON：資料檔格式怎麼影響 Spark？

Ep12 讓我理解 Iceberg 如何管理一張 table 的 schema、snapshot 與 data files；這次我想再往下一層追問：同一筆 event 寫進某個 data file 後，到底保留了什麼？如果 event 帶著 app context、任意 attributes 與 tags，CSV、JSON、Parquet、ORC 還能不能把它讀回原來的樣子？

CSV、JSON、Parquet、ORC 都能放進 storage，也都能由 Spark 讀取，但是它們保存型別、nested structure 與欄位資訊的方式卻差很多，我們也會討論這些結構上不同的設計如何影響實務上的選擇。

## 讀完這篇後你會更了解……

- table format 與 data file format 分別解決什麼問題。
- CSV、JSON、Parquet、ORC 面對 `struct`、`map`、`array` 時，各自能保存到什麼程度。
- CSV 的扁平化與 JSON string 化為什麼是一個資料契約轉換。
- Parquet file 裡的 row group、column chunk、page、footer 如何對應一次讀取。
- 在新資料湖與既有 Hive-heavy 環境中，如何初步選擇 Parquet 或 ORC。

## 先定位這一層：Iceberg 管 table，Parquet 等格式管 file

先把 Ep12 的地圖接回來：Iceberg 這類 table format 管理「一張表目前有哪些 files、schema 如何演進、哪個 snapshot 是目前版本」；CSV、JSON、Parquet、ORC 則管理「單一 file 裡的 rows 與欄位如何編碼」。

```text
Iceberg table
├── metadata/                  # snapshot、schema history、file inventory
└── data/
    ├── part-00000.parquet     # 每個 file 的內容由 file format 定義
    ├── part-00001.parquet
    └── ...
```

因此，Iceberg table 可以使用 Parquet，也可以使用 ORC。這兩層可以分開思考：table format 解決 table state；file format 解決 file representation。

> *table format 告訴 Spark 該讀哪些 files；data file format 告訴 Spark 如何解讀每個 file 裡的資料。*

## 這次的資料：一筆 event 通常不只有 flat columns

若只比較 `id`、`region`、`amount` 這種 flat columns，很容易低估格式選擇的差異。實驗因此建立一份含有 primitive 與 nested types 的 event DataFrame：

```text
id: long
event_ts: timestamp
region: string
event_type: string
amount: double
context: struct<device: string, app_version: string>
attributes: map<string, string>
tags: array<string>
```

其中 `context` 表示固定結構的 app context；`attributes` 表示 keys 可能隨事件而增加的 mapping；`tags` 則是一串標籤。這些欄位在 tracking event、feature data 與 API response 裡都很常見。

原始 schema 讓我確認這三種 nested type 的差別：

```bash
|-- context: struct
|    |-- device: string
|    |-- app_version: string
|-- attributes: map
|    |-- key: string
|    |-- value: string
|-- tags: array
|    |-- element: string
```

## 第一個觀察：CSV 沒有 nested type 的原生表示

我先嘗試直接把原始 DataFrame 寫成 CSV：

```python
events.write.mode("overwrite").option("header", True).csv(csv_path)
```

Spark 直接回報：

```bash
Column `context` has a data type of struct<device:string,app_version:string>, which is not supported by CSV.
```

CSV 的 model 是「一列由幾個文字欄位組成」，沒有能表示 nested object、array 或 map 的原生結構。若真的要提供 CSV，必須由資料生產者定義轉換規則。我在實驗裡將 `context` 攤成兩欄，將 `attributes` 和 `tags` 轉成 JSON string：

```python
csv_events = events.select(
    "id",
    "event_ts",
    "region",
    "event_type",
    "amount",
    C("context.device").alias("context_device"),
    C("context.app_version").alias("context_app_version"),
    F.to_json(C("attributes")).alias("attributes_json"),
    F.to_json(C("tags")).alias("tags_json"),
)
```

這已經是一種資料契約轉換：下游需要知道 `attributes_json` 是 JSON string，並自行解析；原本的 `map` 型別與 element types 不再由 CSV file 保存。

## 第二個觀察：JSON 能表達巢狀內容，schema 仍需要推論或約定

JSON 可以自然寫出 object 與 array，因此原始 nested DataFrame 可以直接寫入 JSON。不過重新讀取時，Spark 需要由 JSON records 推論 schema；這份資料得到的結果有幾個值得注意的地方：

```bash
json schema
|-- amount: double
|-- attributes: struct
|    |-- campaign: string
|    |-- channel: string
|-- context: struct
|    |-- app_version: string
|    |-- device: string
|-- event_ts: string
|-- tags: array
|    |-- element: string
```

`amount` 被推論為 `double`，但 `event_ts` 讀回來成了 `string`。更有意思的是，原本的 `attributes: map<string, string>` 讀回來成了固定欄位的 `struct`。JSON object 能描述 key-value pairs，卻無法在檔案中區分「這是一個固定欄位集合」或「這是一個任意 keys 的 map」；Spark 只能根據當前看見的 keys 推論。

JSON 很適合 API、事件交換與 raw landing；若下游需要穩定的 typed schema，讀取端應提供明確 schema 或再轉換成 table／columnar format。

## 第三個觀察：Parquet 與 ORC 將 nested schema 留在 file 裡

Parquet 和 ORC 都能直接寫入原始 DataFrame。重新讀回時，`event_ts` 仍是 `timestamp`，`attributes` 仍是 `map`，`context` 與 `tags` 也保留原本的 nested structure：

```bash
parquet schema
|-- id: long
|-- event_ts: timestamp
|-- context: struct
|    |-- device: string
|    |-- app_version: string
|-- attributes: map
|    |-- key: string
|    |-- value: string
|-- tags: array
|    |-- element: string
```

ORC 在這份實驗的 schema 結果相同。兩者都屬於 columnar binary format，除了 rows 以外也保存 schema 與型別資訊，因此 Spark 不必猜測每個欄位應如何還原。

| 資料型別或語意 | CSV | JSON | Parquet／ORC |
| --- | --- | --- | --- |
| `string` | 保留文字 | 保留 | 保留 |
| 數字、`boolean` | 以文字保存；預設讀回為 `string` | 可推論 JSON number／boolean | 保留 typed value |
| `timestamp`、`date` | 以文字保存 | 常以字串表示；需要 schema 或格式約定 | 保留 logical type |
| `decimal(p, s)` | precision、scale 不在 file 中 | number 的 precision、scale 沒有固定 schema 語意 | 保留 precision、scale |
| `struct`、`array`、`map` | 無原生表示 | 可表示 object、array；`map` 與 `struct` 意圖可能混合 | 保留 nested schema 與 type |
| `null` | 空字串、缺值與 null 需要額外約定 | 可寫 JSON `null` | 保留 null 資訊 |

即使讀 CSV、JSON 時由讀取端額外提供 schema，也能將文字或 JSON value 轉回 Spark 型別；這是由讀取端重新解讀資料，原始檔案本身並未完整保存型別語意。

## 同樣的 rows，檔案大小為什麼差很多？

這份 2,400 rows 的小型實驗固定寫出兩個 data files；Parquet 與 ORC 使用 Spark 預設的 Snappy compression，得到以下總大小：

```bash
csv:      354,574 bytes
json:     589,578 bytes
parquet:   18,507 bytes
orc:        6,734 bytes
```

CSV、JSON 將欄位名稱、重複字串與日期等內容反覆寫進文字；Parquet、ORC 則以 columnar layout、encoding 與 compression 處理相同型別的值，因此這份重複度高的小資料出現很大的差距。

這不是「ORC 永遠比 Parquet 小」的證明。欄位值分布、檔案大小、compression codec、writer settings 與 Spark 版本都會改變結果；真實選擇要以代表性的 schema 與 query workload 驗證。

## 看進一個 Parquet file：rows 的邏輯視圖與 columns 的實體布局

Parquet 邏輯上仍是一張有 rows 的表，物理上則以 columns 保存。單一 file 的結構可以先這樣理解：

```text
part-00000.parquet
│
├── Row group 0                 # 一批 logical rows
│   ├── id column chunk
│   │   ├── page 0
│   │   └── page 1
│   ├── region column chunk
│   ├── amount column chunk
│   ├── context.device column chunk
│   └── context.app_version column chunk
│
├── Row group 1
│   └── 同樣依 column 保存另一批 rows
│
└── Footer
    ├── schema
    ├── row group 與 column chunk 的位置
    └── statistics：min、max、null count ...
```

同一個 row group 中，各 column chunk 的第 0 個值仍屬於同一筆 logical row；只是 `id`、`region`、`amount` 沒有連續放在一起。column chunk 內再切成 pages，讓它們能個別 encoding 與 compression。

例如查詢只需要 `context.device`，實驗的 Physical Plan 顯示 Parquet reader 要求的 schema 是：

```bash
ReadSchema: struct<context:struct<device:string>>
```

`context.app_version` 因而不在這次 scan 要求的 schema 中。JSON 的對照結果則是：

```bash
ReadSchema: struct<context:struct<app_version:string,device:string>>
```

JSON reader 需要解析整段 `context` object，才能在後面的 `Project` 取出 `device`；Parquet 能在 file 結構上定位 nested child column。footer 的統計資訊也能協助 reader 判斷某一批 rows 是否可能符合條件。

這裡先建立「columnar file 有機會只讀必要部分」的直覺。什麼條件讓 Spark 實際少讀、如何從 `ReadSchema`、`PushedFilters`、`PartitionFilters` 與 Spark UI 確認，會在 Ep14 用可重跑的查詢分開驗證。

## 實務上怎麼在 Parquet 與 ORC 之間選？

Parquet 與 ORC 都能保存 nested schema，也都能在 Spark 中使用 columnar read。ORC 並非只能在 Hive 使用；Spark、Trino、Iceberg 和常見雲端資料湖服務都能使用它。選擇重點通常是生態系與既有資料資產：

| 面向 | Parquet | ORC |
| --- | --- | --- |
| 常見定位 | 跨引擎、跨雲端資料湖的通用選擇 | Hive／HDFS 歷史較深的 columnar format，也被多種引擎支援 |
| 資料能力 | typed schema、nested types、columnar layout | typed schema、nested types、columnar layout |
| 新 table 的常見起點 | 多引擎共享、Iceberg 或雲端資料湖時常優先考慮 | 已有 ORC table、Hive workflow 與成熟治理工具時常延續使用 |
| 選擇依據 | 互通性、團隊工具鏈與實測 workload | 既有資產、平台支援與實測 workload |

如果要建立新的資料湖 table，而資料可能由 Spark、Trino 與其他服務共同讀取，Parquet 往往是阻力較低的預設選擇。若團隊已有大量 ORC tables，並且平台的查詢、治理與維運都已圍繞 ORC，選 ORC 同樣合理。檔案大小或單次 benchmark 都只能作為訊號，最後仍要用真實資料與常見 query 驗證。

## 這次我真正學到的是什麼？

CSV 與 JSON 的可讀性、交換便利性讓它們很適合資料入口與系統邊界；當資料成為供下游持續查詢的 typed table，schema、nested structure 與 null 語意是否能穩定保存就變得很重要。Parquet 與 ORC 提供這個基礎，而 Iceberg 再往上管理一張表的 versions、schema evolution 與 files。

我原本只把 Parquet 想成「壓縮得比較小的檔案」，這次才理解它的 row group、column chunk 與 footer 是為了讓 query engine 能定位資料結構。這份結構提供了少讀資料的條件；下一篇會從真正的 query plan 出發，驗證 Spark 在什麼情況下會用到它。

---

## 完整程式碼與參考資料

🚀 GitHub：\
[Ep13｜Parquet、ORC、CSV、JSON：資料檔格式怎麼影響 Spark？](https://github.com/jeffery5bai/spark-learning-lab/tree/main/episodes/ep13-data-file-formats)

🚀 spark-learning-lab 系列文章與實作：\
[https://github.com/jeffery5bai/spark-learning-lab](https://github.com/jeffery5bai/spark-learning-lab)

Apache Spark 3.4.4 — CSV Files：\
[https://archive.apache.org/dist/spark/docs/3.4.4/sql-data-sources-csv.html](https://archive.apache.org/dist/spark/docs/3.4.4/sql-data-sources-csv.html)

Apache Spark 3.4.4 — JSON Files：\
[https://archive.apache.org/dist/spark/docs/3.4.4/sql-data-sources-json.html](https://archive.apache.org/dist/spark/docs/3.4.4/sql-data-sources-json.html)

Apache Spark 3.4.4 — Parquet Files：\
[https://archive.apache.org/dist/spark/docs/3.4.4/sql-data-sources-parquet.html](https://archive.apache.org/dist/spark/docs/3.4.4/sql-data-sources-parquet.html)

Apache Spark 3.4.4 — ORC Files：\
[https://archive.apache.org/dist/spark/docs/3.4.4/sql-data-sources-orc.html](https://archive.apache.org/dist/spark/docs/3.4.4/sql-data-sources-orc.html)

Apache Parquet — File Format：\
[https://parquet.apache.org/docs/file-format/](https://parquet.apache.org/docs/file-format/)
