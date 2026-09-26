# Ep01｜我每天都在跑 Spark，但先讓它在我的筆電上跑一次吧！


> ### 前言
> *大家好，我是 Jeffery，我是一名 Junior MLE！我的日常工作總是離不開應用 PySpark 建立與維運 Data ETL pipeline、確認資料品質並執行模型的實驗。*
>
> *這是我系列文章的第一篇，希望透過簡單的動手實作過程，深入了解過去每一個「理所當然」和「應該是這樣」背後的底層邏輯，也希望能夠幫助和我同樣困惑的人們！*

在日常工作中，我總是理所當然地連上公司的開發環境，在大型叢集中執行資料處理的任務，還真的從沒想過在筆電上要怎麼把 Spark 跑起來。

為了從頭開始理解這一切到底是怎麼運作的，這次先不急著碰 YARN 或 cluster，我決定先在我的筆電上開啟一個小小的實驗，看懂我到底啟動了什麼，順便釐清所有被視為「理所當然」的小細節。


## 從一個最小的 Spark application 開始
平常寫 PySpark 時，建立 session 往往是最不需要思考的一段：複製貼上一段熟悉的
`SparkSession.builder`，調整一下名字和參數，就直接開始處理 DataFrame。這次我想好好地跟他熟悉一下。

實驗的核心程式很短：

```python
from pyspark.sql import SparkSession

spark: SparkSession = (
    SparkSession.builder
    .appName("ep01-first-run")
    .master("local[2]")
    .getOrCreate()
)

numbers = spark.range(1, 6)
numbers.show()
print(f"Row count: {numbers.count()}")

spark.stop()
```

執行後，得到預期的輸出：

```text
Spark version: 3.4.4
Master: local[2]
Application ID: local-...

+---+
| id|
+---+
|  1|
|  2|
|  3|
|  4|
|  5|
+---+

Row count: 5
```

一段非常簡單的 code，卻讓我感覺一知半解：我寫的是 Python，那 Spark 到底在哪裡跑呢？

## PySpark 不只是在跑 Python

**Spark 的核心主要由 Scala 與 Java 實作，運作在 JVM（Java Virtual Machine）裡**。JVM 是執行 Java／Scala 程式的 runtime；Spark 的 scheduler、SQL engine、記憶體管理等核心元件，都在這裡工作。PySpark 則透過 **Py4J** 與 JVM 溝通。

### 一個 PySpark 指令的旅程

當我寫下：

```python
numbers = spark.range(1, 6)
```

它大致會走過這條路：

```text
PySpark 指令
  ↓
Py4J 把方法呼叫傳到 JVM
  ↓
JVM 裡的 SparkSession 建立 range DataFrame
  ↓
Py4J 回傳 JVM 物件的參照
  ↓
PySpark 將它包成可繼續操作的 Python DataFrame
```

`range()` 多半先回傳 Python wrapper，而不是立刻把所有資料搬回來；等到 `show()`、`collect()` 等需要結果的操作出現，資料才會再回到 Python 端。

三者的分工可以先這樣記：

- **PySpark**：我直接使用的 Python API，負責 DataFrame、SQL 與資料處理邏輯。
- **Py4J**：Python 與 JVM 的橋樑／傳聲筒，負責傳遞方法呼叫、物件參照與結果；它不是把 Python 轉成 Java 的編譯器。
- **JVM**：實際執行 Java／Scala 程式的環境；Spark engine、SparkContext、scheduler 與 Spark UI 都在這裡。

這也解釋了為什麼 PySpark 專案仍需要 Java。安裝 `pyspark==3.4.4` 時，`py4j` 也會作為相依套件一併安裝。

## `builder`：先描述 application，再真正啟動它

`SparkSession.builder` 是一個設定組裝器。呼叫 `appName()`、`master()` 或 `config()` 時，只是在
收集設定；直到 `getOrCreate()`，Spark 才會真的建立或取得 session。

```text
SparkSession.builder  → 設定組裝器
.appName(...)         → 仍是設定組裝器
.master(...)          → 仍是設定組裝器
.getOrCreate()        → 建立或取得 SparkSession
```

設定大致可分成幾類：application 身分、執行位置、資源、SQL 效能、資料系統整合，以及觀測性。
例如 executor memory、shuffle partition 數、Hive catalog、YARN queue 都是常見設定。

設定少時，直接接在 builder 後面很清楚；設定多時，也可以先用 `SparkConf` 集中整理：

```python
from pyspark import SparkConf

conf = (
    SparkConf()
    .setAppName("my-job")
    .setMaster("yarn")
    .set("spark.sql.catalogImplementation", "hive")
)

spark = SparkSession.builder.config(conf=conf).getOrCreate()
```

兩種寫法本質相同，都是先描述 application 要怎麼跑，再交給 Spark 建立。

`getOrCreate()` 裡的「or」也值得留意：它會先找目前 process 裡是否已有可用的 session；如果有，
可能會重用它。像 master、executor memory 這種底層啟動設定，不能期待 session 已存在後重新設定就真的換了執行環境。

## 名稱、識別碼和執行位置，分別回答不同問題

我把幾個看似相近的欄位，先記成三個不同問題：

| 設定或欄位 | 回答的問題 |
| --- | --- |
| `appName` | 這個 application 叫什麼？ |
| `master` | 它要在哪裡、以什麼方式執行？ |
| application ID | 這一次實際執行的系統識別碼是什麼？ |

`appName("ep01-first-run")` 是給人辨識用的名稱。未來在 Spark UI、driver log、YARN 或 Spark History
Server 裡找 job 時，它會是容易閱讀的標籤；它可以重複，並不是唯一 ID。

我這次看到的 application ID 是 `local-...`。在 local mode 中，它通常由 `local-` 加上建立
SparkContext 時的時間戳記（Unix timestamp 毫秒）組成，不是 UUID。每次重新啟動 application，ID 都會不同，方便系統追蹤某一次具體執行。

## `local[2]` 是什麼意思？

`local[2]` 是一種 Spark master URL 的寫法，完整拆開來看：

| 部分 | 意思 |
| --- | --- |
| `local` | 使用 local mode，也就是在啟動 application 的這台機器上執行 Spark。 |
| `[2]` | 最多同時用兩個本機 worker thread 執行 Spark task。 |

它不是兩台機器，也不是兩個遠端 executor。`local` 不加中括號時可視為單一 thread；`local[*]` 則使用 JVM 看得到的所有可用 logical CPU。

這個意義不會因為電腦身處公司環境或有 cluster 可用而改變：只要設定 `local[2]`，Spark 就是要求 local mode。真正提交到 cluster 時，`master` 會改成 `yarn`、`spark://...` 或 Kubernetes 的 URL；同時能跑多少 task，則由 executor 數量、executor core 等設定決定。

在真正的 cluster 裡，Driver 會把 task 排到不同 executor process；但 local mode 中，Driver、scheduler
與 task execution 都在同一個 JVM 裡，不會啟動獨立、遠端的 executor process。

這裡最容易混淆的幾個詞，可以放在一起看：

| 名詞 | 它是什麼？ | 在這次實驗中的角色 |
| --- | --- | --- |
| **Spark task** | Spark 切出的資料運算工作單位 | 被 Spark scheduler 排程執行 |
| **thread** | JVM／作業系統實際執行 task 的軟體單位 | `local[2]` 最多同時使用兩個 task thread |
| **logical CPU** | 作業系統可排程的運算單位 | OS 將 thread 排到可用的 logical CPU 執行 |
| **physical CPU core** | 真正的硬體運算單元 | 不會被 `local[2]` 固定綁定 |

`local[2]` 的 `2` 指的是最多兩個 Spark task thread，不是固定綁定兩顆 physical core；OS 仍會決定 thread
實際在哪個 logical CPU 上執行。

## 沒有指定 master 會怎樣？

我把 `.master("local[2]")` 註解掉後重新執行，看到的是：

```text
Master: local[*]
```

在這次直接以 `uv run python` 執行 PySpark 3.4.4 的環境裡，PySpark 的預設啟動方式會落到 `local[*]`：
使用 JVM 看得到的所有可用 logical CPU。

這不代表 Spark 會自動尋找 YARN 或 Kubernetes，找不到才退回本機。它根本不會主動猜測 cluster；要跑 YARN、
Kubernetes 或 Spark standalone，都應該由 `spark-submit`、平台設定或程式中的 `spark.master` 明確指定。

對實驗來說，我仍會保留 `local[2]`。它讓每次執行使用固定的平行度，不會因為換了一台 CPU core 數不同的電腦而改變行為。

## 這次我真正建立起來的是什麼？

這次沒有建立 cluster，也沒有碰到 YARN；我建立的是一個 local Spark application。Python 透過 Py4J 啟動並連到
JVM 裡的 Spark engine，使用兩個本機 task thread 執行一個很小的 DataFrame 工作。

它讓我看見後面會反覆出現的基本結構：

```text
Python API
  ↓ Py4J
JVM Spark engine
  ↓ scheduler
task
  ↓
本機 thread；未來則會是 cluster 裡的 executor
```

不過，這次實驗還留下了一個很熟悉的疑問：我明明已經寫了 `spark.range()`，也把 DataFrame 一路接下去了，為什麼常常要等到 `show()`、`count()`，甚至最後 `write()` 時，才感覺 Spark 真的開始忙起來？

下一篇，我想沿著這段程式繼續追：一段 local mode 下的 PySpark 程式，從「我寫下 DataFrame 操作」到「畫面出現結果」之間，到底經歷了什麼？

## 完整程式碼與參考資料

🚀 GitHub：\
[Ep01｜我每天都在跑 Spark，但先讓它在我的筆電上跑一次吧！](https://github.com/jeffery5bai/spark-learning-lab/tree/main/episodes/ep01-local-pyspark-first-run)

🚀 spark-learning-lab 系列文章與實作：\
[https://github.com/jeffery5bai/spark-learning-lab](https://github.com/jeffery5bai/spark-learning-lab)

Apache Spark 3.4.4 — Configuration：\
[https://spark.apache.org/docs/3.4.4/configuration.html](https://spark.apache.org/docs/3.4.4/configuration.html)

PySpark 3.4.4 — SparkSession API：\
[https://spark.apache.org/docs/3.4.4/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html](https://spark.apache.org/docs/3.4.4/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html)

Py4J Documentation：\
[https://www.py4j.org/](https://www.py4j.org/)
