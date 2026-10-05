# spark-learning-lab

這是一個從「我每天都在用 Spark，但其實沒有完全看懂它」開始的學習 lab。

我是一名 MLE，日常工作中寫 PySpark 建置 ETL pipeline，維運推薦系統做優化與實驗，雖然天天都在跑 PySpark job，但是當 job 變慢、資源不夠、或是看到 Driver、Executor、YARN 這些名詞時，我總是感覺自己一知半解，常常只能猜測它們的關係，卻說不清楚底層到底是如何運作。

我想從日常工作裡真的會遇到的問題出發，自己做小實驗、看 Spark UI、讀 log，嘗試在小規模的環境中自己建置運算叢集，更深入了解底層的基礎建設和運算過程。
我希望透過一系列的學習與實作，自己不僅是一名能「把 Spark 跑起來」的使用者，也能更全面地理解原理和架構。

## 這個 lab 想達成什麼？

每一集圍繞一個 Spark 問題，保留文章、可重跑實驗與必要的觀察證據。課程依資料與程式實際走過的路徑分段；目錄記錄目前已完成的內容。

### 第一段：在 local mode 建立 Spark 的執行現場（Ep01–Ep04）

從一段能在筆電執行的 PySpark 程式開始，依序看見 lazy execution、Spark UI 與 local mode 的程序拓樸，先建立可觀察、可驗證的本機執行模型。

| 集數 | 主題 | 文章 | 實驗 |
| --- | --- | --- | --- |
| Ep01 | 在筆電上第一次跑起 PySpark | [文章](episodes/ep01-local-pyspark-first-run/article.md) | [程式](episodes/ep01-local-pyspark-first-run/experiment/first_run.py) |
| Ep02 | transformation、action 與 lazy execution | [文章](episodes/ep02-local-mode-program-lifecycle/article.md) | [程式](episodes/ep02-local-mode-program-lifecycle/experiment/lifecycle.py) |
| Ep03 | 第一次打開 Spark UI | [文章](episodes/ep03-local-spark-ui/article.md) | [程式](episodes/ep03-local-spark-ui/experiment/spark_ui.py) |
| Ep04 | local mode 的 Driver、JVM、task thread 與 Python worker | [文章](episodes/ep04-local-mode-platform-map/article.md) | [程式](episodes/ep04-local-mode-platform-map/experiment/local_topology.py) |

### 第二段：從資料流到平行度與結果重用（Ep05–Ep09）

沿著 DataFrame lineage，理解 transformation、shuffle 與 partition 如何改變工作；最後再回答同一份結果被多個 action 使用時，Spark 是否重新計算，以及何時該 materialize 或切斷 lineage。

| 集數 | 主題 | 文章 | 實驗 |
| --- | --- | --- | --- |
| Ep05 | Narrow、Wide 與 Shuffle 如何切出 Stage | [文章](episodes/ep05-dag-lazy-execution/article.md) | [程式](episodes/ep05-dag-lazy-execution/experiment/dag_lineage.py) |
| Ep06 | Partition 如何決定 Task 數與平行度 | [文章](episodes/ep06-partition-parallelism/article.md) | [程式](episodes/ep06-partition-parallelism/experiment/partition_parallelism.py) |
| Ep07 | 同一段 DataFrame pipeline，Task 數為什麼會變？從 Shuffle 到 AQE | [文章](episodes/ep07-shuffle-aqe/article.md) | [程式](episodes/ep07-shuffle-aqe/experiment/shuffle_and_aqe.py) |
| Ep08 | 資料寫出前怎麼安排 partition？repartition、coalesce、partitionBy | [文章](episodes/ep08-output-partitions/article.md) | [程式](episodes/ep08-output-partitions/experiment/output_partitions.py) |
| Ep09 | 同一份 DataFrame 為什麼又跑一次？Cache、persist 與 checkpoint | [文章](episodes/ep09-cache-checkpoint/article.md) | [程式](episodes/ep09-cache-checkpoint/experiment/cache_checkpoint.py) |

### 第三段：從一張表走到資料檔：Catalog、Table Format 與資料格式（Ep10–Ep13）

從 `spark.table("analytics.orders")` 這個日常入口往下追。先用 Hive 生態系畫出查詢提交、query engine、catalog／metastore、warehouse 與 data files 的全貌；接著深入 Hive-style table 的 metadata 管理，再看 Iceberg 如何改變 table metadata 與版本管理，最後走到 Parquet、ORC、CSV、JSON 等實體資料檔。

| 集數 | 主題 | 會回答什麼問題？ |
| --- | --- | --- |
| Ep10 | 一張表從名字到檔案：Spark 資料儲存全貌 | 一次查表會經過哪些元件？名稱、metadata 與實體檔案如何連起來？ |
| Ep11 | Hive Metastore、warehouse 與 Hive-style table：metadata 到底管理了什麼？ | managed/external table、location 與 metadata backend 各自負責什麼？ |
| Ep12 | Iceberg 為什麼重新定義一張表？ | table format 如何用 metadata tree、snapshot 與 commit 管理資料檔？ |
| Ep13 | Parquet、ORC、CSV、JSON：資料檔格式怎麼影響 Spark？ | 檔案格式如何影響 schema、儲存與讀取？ |

### 第四段：讀取策略與效能排查（Ep14 起，依實驗調整）

前一段已經看過 table metadata 與 data files；接著回到一次實際查詢，理解 Spark 如何減少不必要的讀取，再逐步處理 join、skew 與整體效能問題。

| 集數 | 主題 | 會回答什麼問題？ |
| --- | --- | --- |
| Ep14 | 同一份表，Spark 為什麼能少讀那麼多資料？ | column pruning、predicate pushdown 與 partition pruning 分別跳過什麼？ |

## 開始前需要什麼？

目前的本機環境已用下面這組版本驗證過：

| 項目 | 版本 |
| --- | --- |
| Python | 3.11 |
| PySpark | 3.4.4 |
| Java | 17 |
| 套件管理 | uv |

Java 可以和電腦裡其他版本並存。這個專案在 macOS 上使用 Eclipse Temurin 17；
它是常見的 OpenJDK 發行版，Spark 不要求特定 Java 廠商，只要是相容的 Java 17 即可。

### macOS 安裝方式

先準備 Git、[uv](https://docs.astral.sh/uv/)，以及 Java 17：

```bash
brew install --cask temurin@17
```

接著 clone repo，並讓 uv 建立 Python 環境與安裝鎖定的套件：

```bash
git clone https://github.com/jeffery5bai/spark-learning-lab.git
cd spark-learning-lab
uv python install 3.11
uv sync
```

執行 Spark 前，在目前這個 terminal 選擇 Java 17：

```bash
export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
export PATH="$JAVA_HOME/bin:$PATH"
```

想確認環境有沒有通，可以跑這個最小測試：

```bash
uv run python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.master('local[2]').appName('smoke-test').getOrCreate(); print(f'Spark {spark.version} | count={spark.range(3).count()}'); spark.stop()"
```

若看到 `Spark 3.4.4 | count=3`，代表 PySpark、Java 與本機 Spark 都能正常合作。
在 macOS 上看到 native Hadoop library 的 warning 是 local mode 常見訊息，可以先不用緊張。

## Repository 結構

```text
.
├── episodes/
│   └── epNN-topic/
│       ├── article.md       # 當集文章
│       ├── experiment/      # 可重跑的 PySpark 實驗
│       └── screenshots/     # 文章使用的截圖（如有）
├── README.md                # 系列進度與環境設定
├── pyproject.toml           # Python 與 PySpark 相依設定
└── uv.lock                  # 鎖定的套件版本
```
