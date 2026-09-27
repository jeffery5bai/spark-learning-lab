# Ep02｜local mode 下的一段 PySpark 程式，到底經歷了什麼？

上一集，我終於在自己的筆電上啟動了 Spark。不過接著又有一個更貼近日常開發的問題：明明我已經連續寫了好幾個 DataFrame 操作，為什麼總是到 `show()`、`count()` 或 `write()` 時，Spark 才看起來真的開始忙？

這次我用一小份訂單資料，在 local mode 下追一段 PySpark 程式從「描述資料處理」到「真正執行」的過程。

## 讀完這篇後你會更了解......

- 操作 DataFrame 的 **transformation**、**action** 與 **lazy execution**。
- 透過 `explain(mode="extended")`，讀懂 logical plan 與 physical plan 個別在回答什麼問題。
- 觀察 Catalyst Optimizer 如何整理 transformation；並瞭解 DAG Scheduler 與 Task Scheduler 對於執行 action 的責任和角色。

## 先從整體看：一段 PySpark 程式怎麼跑？

我先把 DataFrame API 分成兩種操作：

| 類型 | 例子 | 當下做的事 |
| --- | --- | --- |
| **Transformation** | `where()`、`select()`、`withColumn()`、`join()` | 描述新的資料處理步驟，回傳新的 DataFrame。 |
| **Action** | `count()`、`show()`、`collect()`、`write()` | 要求實際結果，觸發 Spark 執行資料處理。 |

整段旅程如下：

```text
Transformation：描述想做什麼
  ↓
Logical Plan：累積一連串操作
  ↓
Action：要求結果，觸發查詢規劃與執行
  ↓ Catalyst Optimizer
Physical Plan：決定實際怎麼執行
  ↓
DAG Scheduler → Task Scheduler → task
```

接下來，就沿著這條路由上往下看吧！

## Transformation：先描述資料要怎麼變

這次的資料是一小份訂單明細。我先計算含服務費的金額，再保留金額至少 100 的訂單：

```python
from pyspark.sql import functions as F

C = F.col

large_orders_with_fee = (
    transactions
    .withColumn("amount_with_fee", C("amount") * F.lit(1.05))
    .where(C("amount") >= 100)
    .select("order_id", "name", "amount", "amount_with_fee")
)
```

這時 Spark 還沒有真正開始處理資料。`withColumn()`、`where()`、`select()` 只是逐步描述「我想得到什麼樣的 DataFrame」，並累積成 logical plan。

### Logical Plan 和 Physical Plan 分別在想什麼？

我在 action 前先加上一段：

```python
large_orders_with_fee.explain(mode="extended")
```

它會要求 Spark 產生計畫並印到 console，但**不會因為這行就開始掃描資料、執行 task**。`extended` 模式會依序顯示四個階段：

| 階段 | 它回答的問題 |
| --- | --- |
| Parsed Logical Plan | 我寫下的 DataFrame 操作大致長什麼樣子？ |
| Analyzed Logical Plan | 欄位名稱、來源與型別是否合法？ |
| Optimized Logical Plan | 在不改變結果的前提下，Catalyst 怎麼整理這份邏輯？ |
| Physical Plan | Spark 最後選擇怎麼執行它？ |

前面三個都還是在處理「要做什麼」，屬於 logical plan 的過程；最後的 Physical Plan 才開始回答「要怎麼做」。

## Catalyst 如何重新編排 transformation？

完整 output 很長，這裡只看最重要的一組對照。Parsed Logical Plan 先忠實保留我寫程式的順序：先建立 `amount_with_fee`，再篩選 `amount >= 100`。

> ***NOTE**: 印出來的 Plan 順序要「由下往上」閱讀（這很反直覺我知道==）*

```bash
Filter (amount >= 100)
  +- Project [..., amount * 1.05 AS amount_with_fee]
     +- LogicalRDD [...]
```

到了 Optimized Logical Plan，順序變成：

```bash
Project [order_id, name, amount, amount * 1.05 AS amount_with_fee]
  +- Filter (isnotnull(amount) AND amount >= 100)
     +- LogicalRDD [...]
```

因為篩選條件只依賴原始的 `amount`，Spark 可以**先篩掉不符合的訂單，再替留下的資料計算服務費**，少做一些不必要的運算。它也補上 `isnotnull(amount)`：依 SQL 的 `NULL` 語意，`NULL >= 100` 不會成立，因此可以提早排除。

Catalyst 不會任意交換所有操作；只有結果相同時才能調整。假如篩選條件改成依賴新欄位 `amount_with_fee`，就不能簡單移到計算之前。

另一個小實驗中，我建立了完全不會使用的 `debug_label`。它在 Parsed Plan 中還存在，到了 Optimized Logical Plan 已經消失。這讓我更具體感覺到：DataFrame API 不是一行接一行立刻執行，而是一份能被分析的資料處理描述。Spark 因此能移除無用的衍生欄位、合併部分 projection，也能在安全時提早套用條件。

我也用同一份訂單明細切出客戶與訂單兩個視角再 join。Physical Plan 中可以看到兩側先做 `Filter` 與 `Project`，之後才出現 `SortMergeJoin`、`Exchange` 和 `Sort`。這些名詞暫時不用急著背；目前我先理解一件事：**join 接收到的不是原封不動的資料，而是已經依條件與所需欄位整理過的兩個分支。**

## Action：現在才真的開始執行

接著我對同一個 DataFrame 呼叫 action：

```python
print(large_orders_with_fee.count())
large_orders_with_fee.show()
```

`count()` 要的是總列數；`show()` 要的是幾筆資料回到 Driver 顯示。當 action 出現，Spark 才會分析 logical plan、產生 Physical Plan，接著切分、排程並執行工作：

```text
Python 的 count() / show()
  ↓ Py4J
JVM 裡的 Spark 分析與最佳化計畫
  ↓
Physical Plan
  ↓
DAG Scheduler 切出 stage
  ↓
Task Scheduler 將 task 交給可用資源
  ↓
local mode：本機 thread 執行 task
  ↓
結果回到 Driver，再回到 Python
```

| 元件 | 目前可以怎麼理解 |
| --- | --- |
| **DAG Scheduler** | 根據資料相依關係與 shuffle 邊界，把整份工作拆成 stage。 |
| **Task Scheduler** | 把每個 stage 裡的 task 交給可用的運算資源。local mode 下，就是本機可用的 task thread。 |

> ***Catalyst 想的是「怎樣做比較合理」；scheduler 處理的是「怎樣把這份計畫切開並執行」。***

另外，這次沒有使用 cache。`count()` 算完後，`show()` 仍可能重新讀取、篩選與計算同一份資料；Spark 不會自動永久保存每一個中間 DataFrame。什麼情況該使用 `cache()`，留到未來用實驗再來確認。

## 回到 lazy execution：為什麼不立刻執行？

Transformation 先累積、action 再觸發執行，這種設計就是 **lazy execution**。

> ***Lazy execution 並不只是單純延後做事。** 因為 Spark 看得到完整的一串操作，才有機會在真正執行前重新規劃，選擇更有效率的執行方式。*

Transformation 讓 Spark 看見完整規劃，Action 才要求它將規劃化為真正的運算。

這次我已經知道程式從 DataFrame 操作走到 task 執行的大致路徑，但 stage 和 task 實際長什麼樣子還是很抽象。下一篇，我想直接打開 Spark UI：在單機 Spark 裡親眼看看一個 action 如何留下 Job、DAG、Stage 與 Task。

---
## 完整程式碼與參考資料

🚀 GitHub：\
[Ep02｜local mode 下的一段 PySpark 程式，到底經歷了什麼？](https://github.com/jeffery5bai/spark-learning-lab/tree/main/episodes/ep02-local-mode-program-lifecycle)

🚀 spark-learning-lab 系列文章與實作：\
[https://github.com/jeffery5bai/spark-learning-lab](https://github.com/jeffery5bai/spark-learning-lab)

Apache Spark 3.4.4 — RDD Programming Guide：\
[https://archive.apache.org/dist/spark/docs/3.4.4/rdd-programming-guide.html](https://archive.apache.org/dist/spark/docs/3.4.4/rdd-programming-guide.html)

Apache Spark 3.4.4 — SQL Performance Tuning：\
[https://archive.apache.org/dist/spark/docs/3.4.4/sql-performance-tuning.html](https://archive.apache.org/dist/spark/docs/3.4.4/sql-performance-tuning.html)

PySpark 3.4.4 — DataFrame.explain API：\
[https://archive.apache.org/dist/spark/docs/3.4.4/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html](https://archive.apache.org/dist/spark/docs/3.4.4/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html)
