# Ep04｜資料在哪裡、程式在哪裡跑：從 local mode 畫出資料平台地圖

前幾集我已經在筆電上跑起 Spark，也在 Spark UI 裡看過 Job、Stage 和 Task。不過我還是常常把幾個名詞混在一起：Driver、JVM、task thread、Python worker，到底誰在哪裡？資料又是怎麼來回的？

這次我先來觀察 process ID（PID）和 hostname，把 local mode 的位置圖畫出來。

## 讀完這篇後你會更了解......

- 在 local mode 下，Python driver、JVM 與 Spark task 各自位於哪裡。
- 原生 DataFrame API 與 Python UDF 在執行位置上的差異。
- process、thread、partition、task 和 `local[2]` 之間的關係。

## 先畫出 local mode 的整體位置圖

這次實驗的 Spark 設定很單純：

```python
spark = (
    SparkSession.builder
    .appName("ep04-local-mode-platform-map")
    .master("local[2]")
    .getOrCreate()
)
```

在 local mode 下，所有元件都在同一台筆電，但不代表它們都在同一個 process：

```text
Python driver process
  ↓ Py4J
JVM process
  ├─ Spark Driver：SparkContext、scheduler、Spark UI
  └─ JVM task threads：執行 Spark task
       ↓ 只有 task 需要執行 Python 邏輯時
       Python worker process
```

這裡最容易混淆的幾個詞，可以先放在一起看：

| 名詞 | 它是什麼？ | 在 local mode 中的角色 |
| --- | --- | --- |
| **process** | 作業系統中的獨立執行單位，有自己的 PID 與記憶體空間 | Python driver、JVM、Python worker 都可能是不同 process。 |
| **thread** | process 內的執行單位，與同 process 的其他 thread 共用記憶體 | JVM 內的 task thread 實際執行 Spark task。 |
| **partition** | Spark 將資料切分後的單位 | 一個 stage 通常會讓一個 task 處理一個 partition。 |
| **task** | Spark 對一個 partition 發出的運算工作 | 被排到可用的 task thread 執行。 |

`local[2]` 的 `2` 也不是兩台機器、兩個 JVM process 或兩個 Python worker。它指的是：**這個 application 最多同時用兩條本機 JVM task thread 執行 task。**

## 先看原生 DataFrame API：只有 Python driver 和 JVM

我先用 `spark.range()` 建立兩個 partition 的 DataFrame，接著只使用 built-in DataFrame API：

```python
numbers = spark.range(1, 9, numPartitions=2)

native_result = (
    numbers.withColumn("double", C("id") * F.lit(2))
    .withColumn("partition_id", F.spark_partition_id())
)

native_result.show()
```

程式先印出兩個 PID：

```bash
Python driver host: AL02297568
Python driver PID: 28258
JVM host: AL02297568
JVM PID: 28259
Spark master: local[2]
```

接著的結果顯示，資料被切成兩個 partition：

```bash
+---+------+------------+
| id|double|partition_id|
+---+------+------------+
|  1|     2|           0|
|  2|     4|           0|
| ...                         |
|  5|    10|           1|
|  6|    12|           1|
| ...                         |
+---+------+------------+
```

這裡看不到「每個 task 一個新 PID」，因為 task thread 不是 process。`where()`、`select()`、`withColumn()`、`groupBy()` 這類 built-in DataFrame API，主要由 JVM 裡的 Spark engine 執行；兩個 task thread 都在同一個 JVM PID 裡。

```text
Python driver process
  ↓ Py4J：描述 DataFrame 操作、呼叫 action
JVM process
  ├─ task thread → partition 0
  └─ task thread → partition 1
```

> ***原生 DataFrame API 並不是 Python driver 自己逐列算資料；Python 主要負責描述工作，真正的 Spark task 主要在 JVM 裡執行。***

## 加上 Python UDF 後，多了什麼？

接著我對同一份資料加上一個很刻意的 Python UDF。它不做複雜計算，只回傳自己執行所在的 hostname 和 PID：

```python
@F.udf(returnType=T.StringType())
def python_worker_identity(_: int) -> str:
    return f"host={socket.gethostname()}, pid={os.getpid()}"


udf_result = native_result.withColumn(
    "python_worker",
    python_worker_identity(C("id")),
)

udf_result.show(truncate=False)
```

這次結果多出兩個新的 PID：

```bash
| id|partition_id|python_worker             |
|  1|           0|host=AL02297568, pid=28309|
|  2|           0|host=AL02297568, pid=28309|
| ...                                      |
|  5|           1|host=AL02297568, pid=28308|
|  6|           1|host=AL02297568, pid=28308|
| ...                                      |
```

兩個 worker 仍在同一台筆電，卻有不同 PID，代表它們是額外的 Python worker process。這次每個 partition 都由一個 worker 處理；實際 worker 是否重用、數量是否剛好等於 task 數，會隨設定與執行狀況改變，但核心差異不變：**task 一旦需要執行自訂 Python 邏輯，就需要跨出 JVM。**

```text
Python driver process
  ↓ Py4J
JVM process
  └─ JVM task thread
       ↓ 序列化資料
       Python worker process
       ↓ 執行 Python UDF
       JVM task thread
```

| 情境 | task 的主要執行位置 | 額外 Python worker |
| --- | --- | --- |
| built-in DataFrame API | JVM task thread | 通常不需要 |
| `RDD.map()` 等自訂 Python 函式 | JVM task thread 與 Python worker | 需要 |
| Python UDF | JVM task thread 與 Python worker | 需要 |

這也是 Python UDF 在效能排查時值得注意的原因：除了函式本身的成本，資料還需要在 JVM 與 Python worker 之間傳遞。這不代表 UDF 不能用，而是當 built-in function 能完成同一件事時，通常更容易讓 Spark 最佳化與執行。

## 回到 `local[2]`：它和 process 有什麼關係？

這次剛好有兩個 partition，也設定了 `local[2]`，因此兩個 task 最多可以同時執行。若同一個 stage 有十個 task，`local[2]` 並不會建立十個 process；Spark 會一次最多執行兩個 task，其餘 task 等待可用的 thread。

```text
10 個 task
  ↓
local[2]
  ↓
同時最多 2 個 JVM task thread 執行
  ↓
其餘 task 排隊
```

到了真正的 cluster mode，這張圖會擴展成 Driver 與多個 executor process 分布在不同節點；但 task、partition 與 thread 的基本關係仍會保留。這就是我先在 local mode 建立位置感的原因。

## 這次我真正釐清了什麼？

這次我確認了 local mode 也能把完整架構濃縮在一台筆電上：Python driver 和 JVM 已經是不同 process；task 在 JVM thread 中執行；只有需要自訂 Python 邏輯時，才會看到額外的 Python worker process。

我也弄清楚 `local[2]` 不是兩台機器或兩個 process。它真正限制的是同時可執行的 JVM task thread 數量。

下一篇，我想把今天的 process、thread 與 partition 先放在一邊，回到另一個根本問題：Spark 明明知道我接下來要做很多 transformation，它是怎麼把這串操作組成 DAG，又為什麼不立刻執行？

---
## 完整程式碼

🚀 GitHub：\
[Ep04｜資料在哪裡、程式在哪裡跑：從 local mode 畫出資料平台地圖](https://github.com/jeffery5bai/spark-learning-lab/tree/main/episodes/ep04-local-mode-platform-map)

🚀 spark-learning-lab 系列文章與實作：\
[https://github.com/jeffery5bai/spark-learning-lab](https://github.com/jeffery5bai/spark-learning-lab)
