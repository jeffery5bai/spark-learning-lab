# Ep05｜Narrow、Wide 與 Shuffle：Spark 怎麼切出 Stage？

前幾集裡，我已經知道 DataFrame 的 transformation 會先累積，等 action 出現才執行；也在 Spark UI 看過 Job、Stage 與 Task。不過當我看到 `groupBy()`、`join()` 這類操作時，還是會有一個更根本的問題：Spark 到底根據什麼把一串程式切成不同 stage？

這次我從資料的 partition 開始看，理解 narrow transformation、wide transformation 與 shuffle 的關係。它們正是 DAG 和 stage 邊界的起點。

## 讀完這篇後你會更了解……

- partition 裡的資料如何流動，以及 narrow、wide transformation 分別代表什麼。
- `groupBy()` 為什麼需要 shuffle，以及 shuffle 如何讓 DAG 出現 stage 邊界。
- RDD 是什麼；`toDebugString()` 能提供哪些 lineage 資訊，以及何時值得使用它。

## 先從整體看：資料怎麼流過一段 Spark 程式？

Spark 會把一份資料切成多個 **partition**；每個 task 主要處理其中一個 partition。這次實驗從兩個 partition 的來源資料開始，接著篩選、計算服務費，最後依地區彙總金額：

```text
來源資料：2 個 partitions
  ↓
where() / withColumn() / select()
  ↓
groupBy("region") / sum()
  ↓
show()
```

前面三個操作可以讓各 partition 各自完成；`groupBy("region")` 則需要先把同一個 `region` 的資料集中到一起。這個差異，正好把 transformation 分成 narrow 與 wide 兩類。

| 類型 | 資料怎麼處理？ | 是否需要跨 partition 搬資料？ | 常見操作 |
| --- | --- | --- | --- |
| **Narrow transformation** | 每個 partition 可以自己處理完。 | 不需要。 | `where()`、`select()`、`withColumn()`、`map()` |
| **Wide transformation** | 結果需要其他 partition 的資料。 | 需要。 | `groupBy()`、`distinct()`、`repartition()`、多數 `join()` |

「每列各自計算」通常是 narrow transformation，例如 `withColumn()`；更完整的判斷方式是：**只要一個 partition 能自行完成工作，不必向其他 partition 取得資料，就是 narrow transformation。**

> *要不要跨 partition 取得資料，是 narrow 和 wide 最核心的差別。*

## `groupBy()` 為什麼會觸發 shuffle？

假設同一個 `region` 的訂單一開始散在不同 partition：

```text
Partition 0：TW、US、TW
Partition 1：JP、US、JP
```

要計算每個地區的總金額時，所有 `TW`、所有 `US`、所有 `JP` 的資料都要各自集中，Spark 才能完成最終彙總。它會根據 `region` 這個 key，決定每筆資料要送往哪個 partition；這個重新分配資料的過程就是 **shuffle**。

```text
Stage 0
Partition 0 ─┐
             ├─ 依 region shuffle ─┐
Partition 1 ─┘                      │
                                    ↓
Stage 1                       groupBy + sum
```

因此，wide transformation 會形成 stage 邊界：前一個 stage 先處理原始 partition 並寫出 shuffle 資料；下一個 stage 再讀取重組後的資料，繼續運算。

在 cluster 裡，shuffle 可能涉及跨機器傳輸；在 local mode 中，資料仍在同一台電腦，但 Spark 還是要寫出、讀回並重新分配資料。這也是為什麼 `groupBy()`、`join()`、`distinct()` 等操作經常是效能分析時優先觀察的地方。

API 名稱只能作為初步線索。例如多數 `join()` 需要 shuffle，但小表可以使用 broadcast join；此時 Spark 會採用不同策略。真正要問的是：**下一步運算需要改變資料在 partition 間的分布嗎？**

## 用一個小實驗看見 shuffle 邊界

這次的程式把前段的 narrow transformations 和最後的 wide transformation 放在一起：

```python
from pyspark.sql import functions as F

C = F.col

cleaned_orders = (
    orders.where(C("amount") >= 100)
    .withColumn("amount_with_fee", C("amount") * F.lit(1.05))
    .select("region", "amount_with_fee")
)

regional_summary = cleaned_orders.groupBy("region").agg(
    F.sum("amount_with_fee").alias("total_amount")
)
```

`regional_summary` 建立完成時，Spark 已經知道整份處理流程和資料相依關係，但尚未執行資料處理。呼叫 `show()` 後，Spark 才依據這份 DAG 切出 stage、建立 task 並開始運算。

```python
regional_summary.show()
```

Spark UI 中可以看到這個 Job 的兩個 stage：第一個 stage 處理來源資料並寫出 shuffle 資料，第二個 stage 讀取 shuffle 資料後完成依地區彙總。這也把程式碼裡的 `groupBy("region")` 和 UI 裡的 stage 邊界連了起來。

## RDD 是什麼？為什麼可以用它看 lineage？

RDD（Resilient Distributed Dataset）是 Spark 較底層的分散式資料抽象。可以先把它理解成：**一組 partition，加上它們如何從上游資料得到結果的相依關係。**

現在日常的 PySpark ETL 大多使用 DataFrame，因為它有 schema，也能交給 Catalyst 進行最佳化。RDD API 則提供像 `map()`、`filter()`、`reduceByKey()` 這類較底層的操作。

| 面向 | RDD | DataFrame |
| --- | --- | --- |
| 資料表示 | 分散在 partition 中的物件集合 | 有 schema 的表格資料 |
| 常見 API | `map()`、`filter()`、`reduceByKey()` | `where()`、`select()`、`groupBy()`、`join()` |
| 日常 ETL 開發 | 較少直接使用 | 通常優先使用 |

DataFrame 的執行最終仍要面對 partition、task 與資料相依關係。因此在學習或除錯時，可以暫時從 RDD 視角觀察 lineage：

```python
print(regional_summary.rdd.toDebugString().decode("utf-8"))
```

這次輸出很長，我只保留和資料流動最有關的部分：

```bash
(2) MapPartitionsRDD[...]
 |  ShuffledRowRDD[...]
 +-(2) MapPartitionsRDD[...]
    |  ParallelCollectionRDD[...]
```

這份輸出由下往上讀：

| 節點 | 目前可以怎麼理解 |
| --- | --- |
| `ParallelCollectionRDD` | 來源資料；這次由 `spark.range()` 建立。 |
| `MapPartitionsRDD` | 每個 partition 各自處理的內部節點；它不一定一對一對應某個 DataFrame API。 |
| `ShuffledRowRDD` | Spark SQL 完成 shuffle 後的資料，是 `groupBy()` 形成 wide dependency 的證據。 |
| `(2)` | 這個 RDD 的 partition 數量。 |

輸出中也會出現 `SQLExecutionRDD`、`javaToPython` 等 Spark SQL 和 PySpark 的內部包裝。它們有助於 Spark 運作，這次先不用逐一背起來；辨認來源、partition 數量與 `ShuffledRowRDD` 已足夠回答我們的問題。

## 什麼時候該用 `toDebugString()`？

`toDebugString()` 適合快速確認 RDD lineage 與 shuffle 邊界，例如：

- 想確認某段處理是否出現 shuffle。
- 想從 partition 相依關係判斷某個 RDD transformation 是 narrow 還是 wide。
- 想追查 partition 數量在哪個步驟開始改變。

日常的 DataFrame 除錯仍可依問題搭配不同觀察方式：

| 想確認的問題 | 優先使用的工具 |
| --- | --- |
| Catalyst 如何整理查詢、選擇何種 join 策略 | `df.explain(mode="extended")` |
| 實際執行的 Job、Stage、Task 與 shuffle read / write 成本 | Spark UI |
| partition lineage 與 shuffle 相依關係 | `df.rdd.toDebugString()` |

`toDebugString()` 會讓 Spark 產生可檢視的 lineage，但不會因為這行就掃描資料或執行 task；真正觸發資料處理的仍是 `show()`、`count()`、`write()` 等 action。

## 回到整體：shuffle 如何把 DAG 切成 stage？

`where()`、`withColumn()`、`select()` 都是 narrow transformation，因此可以留在同一個 stage 中連續處理。`groupBy()` 需要依 `region` 重分配資料，形成 shuffle，也在 DAG 中切出下一個 stage。Action 出現後，Spark 便依這些相依關係開始執行整份工作。

> *Shuffle 讓一段資料處理出現 stage 邊界：前段先重新分配資料，後段再讀取重組後的資料繼續計算。*

下一篇，我想把這條路再拆細一點：一個 action 建立的 Job、被 shuffle 切開的 Stage，以及實際處理 partition 的 Task，三者究竟各自負責什麼？

---
## 完整程式碼與參考資料

🚀 GitHub：\
[Ep05｜Narrow、Wide 與 Shuffle：Spark 怎麼切出 Stage？](https://github.com/jeffery5bai/spark-learning-lab/tree/main/episodes/ep05-dag-lazy-execution)

🚀 spark-learning-lab 系列文章與實作：\
[https://github.com/jeffery5bai/spark-learning-lab](https://github.com/jeffery5bai/spark-learning-lab)

Apache Spark 3.4.4 — RDD Programming Guide：\
[https://archive.apache.org/dist/spark/docs/3.4.4/rdd-programming-guide.html](https://archive.apache.org/dist/spark/docs/3.4.4/rdd-programming-guide.html)
