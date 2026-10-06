# Ep11｜Hive Metastore、warehouse 與 Hive-style table：metadata 到底管理了什麼？

上一集，我用一個簡單的 table 走過整個資料存儲的生態系，了解整體架構的全局樣貌。今天我想要深入了解 Metastore 管理資料表的生命週期，當 SparkSession 停掉後，下一個 job 為什麼還找得到這張表？`DROP TABLE` 時，為什麼有些 HDFS 路徑會跟著消失，有些檔案卻繼續佔著空間？

這次我固定使用 Hive-compatible catalog，讓兩個 SparkSession 共用同一份 Derby metastore 與 warehouse。透過 managed table、external table 和 `DROP TABLE`，確認 metadata 與實體資料各自的生命週期。

## 讀完這篇後你會更了解……

- Hive Catalog、Hive Metastore、metadata backend 與 warehouse 在這個實驗裡如何合作。
- SparkSession 結束後，table metadata 為什麼仍能被下一個 session 找到。
- managed table 與 external table 在 `DROP TABLE` 後，為什麼留下不同的實體資料結果。
- orphan files、dangling table 和 `refreshTable()` 對資料清理代表什麼。

## 先 recap：誰保存 table name？誰保存資料？

先把 Ep10 的地圖縮小成這次討論的範圍：

```text
Spark SQL
  ↓
Hive-compatible catalog / metastore
  ↓
metadata backend：Derby、MySQL、PostgreSQL ...
  ↓ metadata 中的 Location
warehouse 或 external location
  ↓
Parquet data files
```

catalog 的責任是解析 table identifier，例如 `spark_catalog.analytics.managed_events`；Hive Metastore 則是 Hive 生態裡常見的 catalog 與 metadata service 實作，它保存 table 的 schema、type、provider、location 等資訊。實驗中的 Derby 是這份 metadata 的 backend database。

`analytics` 在 Hive SQL 的語境中稱為 **database**；在 Spark Catalog API 等較通用的語境中稱為 **namespace**。這個實驗裡兩者指向同一個單層名稱空間：

```text
spark_catalog.analytics.managed_events
      │             │
   catalog      database / namespace
```

warehouse 是受管理 table 常用的預設 storage 根路徑。external table 則會登記一條自行指定的資料路徑；兩者都會將自己的 `Location` 記錄在 metadata 裡。

> *Hive Metastore 保存「如何找到一張表」的資訊；warehouse 與 external location 保存「這張表實際讀寫哪些檔案」的資料。*

## 這次的實驗：兩個 session，共用一份 metastore

實驗每次都建立乾淨的暫時 workspace，裡面有三個重要位置：

```text
workspace/
├── metastore_db/       # embedded Derby：保存 Hive metadata
├── warehouse/          # managed table 的預設根路徑
└── external-events/    # 這次明確指定的 external location
```

兩個 SparkSession 都指定同一組 `metastore_path` 與 `warehouse_path`：

```python
from pathlib import Path
from tempfile import mkdtemp

from pyspark.sql import SparkSession

workspace = Path(mkdtemp(prefix="ep11-hive-metastore-warehouse-"))
warehouse_path = workspace / "warehouse"
metastore_path = workspace / "metastore_db"
external_events_path = workspace / "external-events"

def create_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.warehouse.dir", str(warehouse_path))
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:{metastore_path};create=true",
        )
        .enableHiveSupport()
        .getOrCreate()
    )


spark = create_spark("ep11-session-a")
```

`enableHiveSupport()` 讓 SparkSession 使用 Hive-compatible catalog。`ConnectionURL` 則讓 Hive metastore 使用 workspace 裡的 embedded Derby。正式環境通常會改由共用的 Hive Metastore service 搭配 MySQL 或 PostgreSQL；這裡用 Derby 是為了讓所有元件都能在本機觀察與清理。

## 第一個觀察：database 與 table metadata 寫到哪裡？

Session A 先建立 `analytics` database，再建立兩張內容相同、type 不同的 table：

```python
spark.sql("CREATE DATABASE analytics")

events.write.mode("overwrite").saveAsTable("analytics.managed_events")

events.write.mode("overwrite").parquet(str(external_events_path))
spark.sql(
    "CREATE TABLE analytics.external_events "
    "USING PARQUET "
    f"LOCATION '{external_events_path}'"
)
```

`DESCRIBE DATABASE EXTENDED` 先讓我看到 database 也有 metadata：

```bash
+--------------+------------------------------------------------+
|info_name     |info_value                                      |
+--------------+------------------------------------------------+
|Catalog Name  |spark_catalog                                   |
|Namespace Name|analytics                                       |
|Location      |file:/.../warehouse/analytics.db                |
|Owner         |...                                             |
+--------------+------------------------------------------------+
```

接著看兩張 table：

```bash
+--------+-------------------------------------------------------------+
|col_name|data_type                                                    |
+--------+-------------------------------------------------------------+
|Type    |MANAGED                                                      |
|Provider|parquet                                                      |
|Location|file:/.../warehouse/analytics.db/managed_events             |
+--------+-------------------------------------------------------------+

+--------+-------------------------------------------------------------+
|col_name|data_type                                                    |
+--------+-------------------------------------------------------------+
|Type    |EXTERNAL                                                     |
|Provider|PARQUET                                                      |
|Location|file:/.../external-events                                    |
+--------+-------------------------------------------------------------+
```

這些輸出讓我把幾個角色對上：database metadata 指向 `analytics.db`；managed table 由 Spark 在 warehouse 下安排 location；external table 的 metadata 則記住明確指定的路徑。兩張表的 provider 都是 Parquet。

## 第二個觀察：SparkSession 停掉，metadata 還在嗎？

Session A 建表後立刻 `stop()`。接著 Session B 用完全相同的 Derby 與 warehouse 設定啟動，沒有再執行任何 `CREATE DATABASE` 或 `CREATE TABLE`：

```python
spark = create_spark("ep11-session-b")

spark.sql("SHOW DATABASES").show(truncate=False)
spark.sql("SHOW TABLES IN analytics").show(truncate=False)
print(spark.table("analytics.managed_events").count())
print(spark.table("analytics.external_events").count())
```

輸出仍然列出 `analytics`、`managed_events`、`external_events`，而且兩張表都讀到 12 筆資料：

```bash
+---------+---------------+-----------+
|namespace|tableName      |isTemporary|
+---------+---------------+-----------+
|analytics|external_events|false      |
|analytics|managed_events |false      |
+---------+---------------+-----------+

managed count: 12
external count: 12
```

這裡的重點不在 SparkSession 自己記住前一段程式；Session B 從 Derby 裡的 metastore metadata 找到 table name、schema、provider 與 location，再回到對應路徑讀取 Parquet files。

## 第三個觀察：`DROP TABLE` 為什麼留下不同結果？

接著我分別 drop 兩張表，再直接檢查原本的 location 是否存在：

```python
spark.sql("DROP TABLE analytics.managed_events")
print(managed_location.exists())

spark.sql("DROP TABLE analytics.external_events")
print(external_events_path.exists())
```

結果是：

```bash
managed location exists after DROP TABLE: False
external location exists after DROP TABLE: True
```

兩種 `DROP TABLE` 都會移除 Hive metastore 裡的 table metadata。接下來的**差異來自 table type 的資料 ownership semantics**：
- managed table 的 location 由 Spark/Hive convention 管理，drop 時會連同該 table location 的 data files 清除；
- external table 的 location 由建表者明確指定，預設保留實體資料，避免 catalog 在不清楚其他使用者或 job 是否共用這個路徑時直接刪除檔案。

這個行為也能立刻驗證。external path 還在時，只要重新登記同一條路徑：

```python
spark.sql(
    "CREATE TABLE analytics.external_events_recreated "
    "USING PARQUET "
    f"LOCATION '{external_events_path}'"
)

spark.table("analytics.external_events_recreated").count()
```

結果再次得到 `12`。data files 一直都在，消失的是原先的 table metadata entry。

| 操作 | metastore metadata | data files |
| --- | --- | --- |
| `DROP TABLE` managed table | 移除 | 預設一起清除 table location。 |
| `DROP TABLE` external table | 移除 | 預設保留 external location。 |
| 將 external path 重新 `CREATE TABLE ... LOCATION` | 新增一筆 metadata | 沿用原本檔案。 |

正式環境仍要以平台設定為準。某些 Hive 環境能開啟 external table purge 行為，讓外部資料也在 drop 時清除；這類設定會擴大刪除範圍，執行 retention 前需要先確認 table type、location 與平台 policy。

## metadata 也會過期：orphan files、dangling table 與 refresh

metadata 與 storage 有各自的生命週期，兩邊一旦不同步，就可能出現兩種問題：

```text
DROP external table
  ↓
metadata 消失，data files 留下
  ↓
orphan files

直接刪除 external location
  ↓
metadata 留下，data files 消失
  ↓
dangling table
```

另一種常見情境是其他 job 直接改動 external location 的檔案。Spark application 可能仍保有已讀取的 table 或檔案 listing 視圖；因此當需要重新讀取時，可以明確 refresh：

```python
spark.catalog.refreshTable("analytics.external_events")
```

它會刷新這張 table 相關的 cached data 與 metadata。

這讓我想到團隊曾經做過的每月 Data Retention 清理，我們會盤點不再使用的 table，透過 `DROP TABLE IF EXISTS ...` 移除它們；有次做完刪除的幾個月後才發現有一批 HDFS 路徑仍在佔空間（儲存空間也在花錢），追查後才知道，部分 table 建立時使用 external location，drop 時雖然移除了 metadata，實體檔案仍留在 HDFS。後來我們先確認路徑沒有被其他 table 或 job 使用，再以 HDFS 指令清除過時檔案。

這次實驗完全呼應了今天討論的內容，retention workflow 需要同時盤點 metastore 的 table metadata 與 storage 上的實體路徑，才能避免 orphan files，也避免誤刪仍被使用的 external data。

## 這次我真正學到的是什麼？

Hive Metastore 讓 table name、schema、provider 與 location 可以跨 SparkSession 被重新找到；它提供資料表的控制面 metadata。warehouse 與 external location 則位於儲存面，放著真正由 Spark 讀寫的 data files。

managed table 和 external table 都能被同一個 catalog 查詢，差異在於 location 的 ownership semantics。看到 `DROP TABLE` 成功後，我會再確認它清掉的是 metadata、檔案，或兩者；這也是資料清理與 retention 最容易留下盲點的地方。

下一篇會沿著同一條路徑介紹 Iceberg 的設計。它保留 catalog、storage 和 data files 這些元件，並用另一套 table metadata 與 snapshot 設計處理大型資料表的版本與提交。

---

## 完整程式碼與參考資料

🚀 GitHub：\
[Ep11｜Hive Metastore、warehouse 與 Hive-style table：metadata 到底管理了什麼？](https://github.com/jeffery5bai/spark-learning-lab/tree/main/episodes/ep11-hive-metastore-warehouse)

🚀 spark-learning-lab 系列文章與實作：\
[https://github.com/jeffery5bai/spark-learning-lab](https://github.com/jeffery5bai/spark-learning-lab)

Apache Spark 3.4.4 — Hive Tables：\
[https://archive.apache.org/dist/spark/docs/3.4.4/sql-data-sources-hive-tables.html](https://archive.apache.org/dist/spark/docs/3.4.4/sql-data-sources-hive-tables.html)

Apache Spark 3.4.4 — DROP TABLE：\
[https://archive.apache.org/dist/spark/docs/3.4.4/sql-ref-syntax-ddl-drop-table.html](https://archive.apache.org/dist/spark/docs/3.4.4/sql-ref-syntax-ddl-drop-table.html)

Apache Spark 3.4.4 — Catalog.refreshTable：\
[https://archive.apache.org/dist/spark/docs/3.4.4/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.refreshTable.html](https://archive.apache.org/dist/spark/docs/3.4.4/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.refreshTable.html)
