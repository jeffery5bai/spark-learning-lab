# Ep15｜我的第一個 Spark cluster：Master、Worker、Driver、Executor 分別在哪裡？

前面幾集我都在筆電的 `local` mode 跑 Spark。程式能執行、Spark UI 也能看見 Job、Stage 與 Task，但是進入正式工作的叢集環境中，到底 Driver、Executor、Master 與 Worker 各自是什麼？當它們不再待在同一台機器時，Spark application 又是怎麼被送出去的？

這次我用一個最小的 Spark Standalone cluster 做觀察：一個 Master、一個 Worker，以及依模式需要的提交端。container 只是把這些 process 放到可分開觀察的環境；重點是確認每個角色實際出現在哪裡、誰向誰要求資源、誰真正執行 task。

## 讀完這篇後你會更了解……

- `local` mode 與 Standalone cluster 的角色配置差異。
- Master、Worker、Driver、Executor 分別負責什麼，以及它們彼此的關係。
- client mode 與 cluster mode 最關鍵的差異：Driver 放在哪裡。
- 如何從 Master UI、Worker UI 與 application UI 找到這些證據。

## 先從熟悉的 local mode 出發：一台筆電裡已經有 Driver 與 task 執行者

平常以 `local[*]` 建立 `SparkSession` 時，Driver 與本機執行 task 的 threads 都在同一台筆電、同一個 Spark application process 裡。Driver 建立 DataFrame 的 lineage、在 action 發生時產生 Job，並以 local scheduler 將 task 交給本機執行。

```text
筆電上的一個 Spark application process
┌─────────────────────────────────────────────┐
│ Driver                                      │
│  ├─ 建立 SparkSession、規劃 Job 與 Stage     │
│  └─ local scheduler                         │
│       └─ 本機 threads 執行 tasks             │
└─────────────────────────────────────────────┘
```

這個模式沒有另外啟動 Standalone 的 Master 或 Worker。它很適合先理解 API、query plan、partition 與 Spark UI；但因為角色都擠在同一處，很難由 process 或資源畫面看出它們的邊界。

Standalone cluster 將資源管理和 task 執行移出 Driver 所在的位置：Master 與 Worker 先形成一個可提供資源的 cluster，application 再向它請求 Executor。

## 先定義 application：一次提交、一次 Driver 的工作單位

在這篇裡，**Spark application** 指的是一次以 `spark-submit` 啟動的 Spark 程式，以及它執行期間所使用的 Driver 和 Executors。可以把它想成一個完整的工作專案：程式開始時有一位 Driver，結束時 Driver 和它專屬的 Executors 一起釋放。

它和前幾集看過的 Job 不是同一層：一個 application 可以包含多個 Job；每次呼叫 `count()`、`collect()` 或寫出資料等 action，Driver 會在同一個 application 內建立一個 Job。

```text
一次 spark-submit
└── 一個 Spark application
    ├── 一個 Driver
    ├── 零個或多個 Executors
    └── 零個或多個 Jobs
         └── 每個 Job 再拆成 Stages 與 Tasks
```

Master UI 裡的 `Running Applications` 就是在列出目前還活著的這種「完整工作專案」，不是在逐條列出 Job 或 Task。

## 先用一個小團隊理解四個角色

我先把 Standalone cluster 想成一間有派工窗口的辦公室：

| 角色 | 可以先把它想成 | 只要先記住 |
| --- | --- | --- |
| Driver | 一個 application 的專案負責人 | 讀懂程式、拆出 tasks，並等待結果。 |
| Master | 全公司的派工窗口 | 知道哪些 Worker 還有資源，替 application 安排位置。 |
| Worker | 一間提供人力與設備的辦公室 | 回報自己有多少 cores、memory，並依指示啟動工作的人。 |
| Executor | 被派給這個 application 的工作者 | 真正接收並執行 task。 |

這個比喻想要先分開兩件容易混在一起的事：**Master ↔ Worker** 處理「哪裡有資源」；**Driver ↔ Executor** 處理「這個 application 的工作怎麼做」。Worker 能啟動 Executor，但 Worker 本身不是 Executor。

這些名稱既是角色，也會對應到實際的 process 和資源。在這次 cluster 實驗裡，Master、Worker、Driver、Executor 都能在不同 container 或同一個 Worker container 中看到相應的 JVM process；Executor 會被配置實際的 CPU cores 與 memory，並用它們跑 task。Driver 也需要自己的 process 和 memory，但它的責任是協調 application，不是替 Executor 做 task。

```text
Spark application
Driver ── 請求資源 ──→ Master ── 配置資源 ──→ Worker
   │                                               │
   └──────────── 指派 tasks ───────────────→ Executor
```

一個 core 不是「一個 task 永遠固定對應一個 process」，而是 Executor 可同時執行 task 的 CPU 資源上限之一。這次先固定很小的資源配置，讓 UI 的數字容易對照；task 如何被排到 Executor、task 數與可用 cores 如何互動，留到下一篇實驗。

## `--deploy-mode` 決定的是：Driver 到底待在哪一台機器？

提交 application 時，最重要的問題不是指令長什麼樣子，而是：**Driver 的 process 由誰啟動、在哪裡持續活著？**

以這次「一個 Master、一道 Worker、一個提交端」的實驗來說，兩種模式的配置如下：

| 位置 | client mode | cluster mode |
| --- | --- | --- |
| 提交端 container | `spark-submit` **與 Driver** 都在這裡。job 尚未結束前，這個 container 必須持續存在。 | 只有 `spark-submit` 暫時在這裡。它把 application 交給 Master 後即可結束。 |
| Master container | Master 記錄 Worker 資源，替 application 的 Executor 配置位置。 | Master 先將 **Driver** 配置到 Worker；Driver 啟動後，再為 application 的 Executor 配置位置。 |
| Worker container | Worker + **Executor**。Executor 接收提交端的 Driver 指派的 tasks。 | Worker + **Driver** + **Executor**。Master 啟動 Driver，Driver 再協調 Executor 執行 tasks。 |

換成最短的地圖就是：

```text
client mode
提交端 container [spark-submit + Driver]
Master container [Master]
Worker container [Worker + Executor]

cluster mode
提交端 container [spark-submit，提交後結束]
Master container [Master]
Worker container [Worker + Driver + Executor]
```

兩種模式都有四個角色，差別只在 Driver。**client mode 的 Driver 留在提交端；cluster mode 的 Driver 由 Master 啟動並放進 Worker。** 因此 client mode 的提交端中斷，Driver 也隨之消失；cluster mode 則由 cluster 繼續維持 Driver 的生命週期。

這張圖是本次一個 Worker 的實驗配置。有多個 Worker 時，Master 會依可用資源安排 cluster-mode Driver 和 Executors 到哪些 Worker；Driver 和每個 Executor 並不保證在同一台機器。

## 先只啟動 Master：它是一個活著的管理者，卻還沒有可排程的資源

實驗先啟動一個只承載 Spark Master 的 container，並開啟它的 Web UI。此時 UI 顯示 `Alive Workers: 0`、`Applications: 0 Running`、`Drivers: 0 Running`。

![只有 Master 時，尚未有 Worker、application 或 Driver](https://raw.githubusercontent.com/jeffery5bai/spark-learning-lab/main/episodes/ep15-standalone-cluster/screenshots/master-no-workers.png)

這是很有用的起點：Master 已啟動，不等於它自己就是 Worker、Driver 或 Executor。它暫時沒有任何人可分配資源。

## Worker 註冊後，cluster 才有 2 cores 與 3 GiB 可用

接著啟動一個 Worker container，設定它向 Master 宣告 2 cores 和 3 GiB memory。Master UI 立刻將 `Alive Workers` 變成 1，資源欄則顯示 `2 (0 Used)` 與 `3.0 GiB (0.0 B Used)`。

![Worker 已註冊，但尚未有 application 使用資源](https://raw.githubusercontent.com/jeffery5bai/spark-learning-lab/main/episodes/ep15-standalone-cluster/screenshots/master-worker-idle.png)

這裡的數字是 **Worker 提供給 Standalone cluster 的資源**，不是筆電的所有硬體資源。也還沒有 application，所以沒有 Executor 可看。

## client mode：Driver 留在提交端，Worker 只啟動 Executor

為了把 Driver 的位置固定下來，我由另一個 container 以 client mode 提交一個 PySpark application。提交端掛載實驗程式，Driver 完成小型 action 後刻意保留 300 秒，讓 UI 可被觀察：

```python
import time

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("ep15-client-driver").getOrCreate()

result = (
    spark.range(0, 2_000_000, numPartitions=2)
    .repartition(2)
    .groupBy()
    .count()
    .collect()[0][0]
)
print(f"Completed a small action: {result:,} rows")

time.sleep(300)
spark.stop()
```

Master UI 出現一個 Running Application，並為它的 Executor 配置 1 core、1024 MiB。不過 `Drivers` 仍為 `0 Running`：這個 Driver 位在外部的提交端 container，沒有由 Master 啟動或追蹤。

![client mode：application 使用一個 Executor，但 Master 的 Drivers 仍是 0](https://raw.githubusercontent.com/jeffery5bai/spark-learning-lab/main/episodes/ep15-standalone-cluster/screenshots/client-mode-master.png)

從 Worker UI 可以補上另一半證據。`Running Executors (1)` 中的 Executor 0 屬於 `ep15-client-driver`，使用 1 core 與 1024 MiB；這才是執行前面 action 的地方。

![client mode：Worker 上的 Executor 0 屬於 ep15-client-driver](https://raw.githubusercontent.com/jeffery5bai/spark-learning-lab/main/episodes/ep15-standalone-cluster/screenshots/client-mode-worker-executor.png)

而 application 的 Driver UI 位於提交端對外提供的 `4040` 埠。它記錄了三個 Completed Jobs，證明 Driver 持有 application 的 Spark UI 與 job 狀態。

![client mode：位於提交端的 Driver UI 記錄三個已完成的 Job](https://raw.githubusercontent.com/jeffery5bai/spark-learning-lab/main/episodes/ep15-standalone-cluster/screenshots/client-mode-driver-ui.png)

```text
提交端 container
└── PySpark Driver（含 application UI :4040）
       │
       ├─ 向 Master 註冊 application、請求 Executor
       └─ 將 tasks 指派給 Executor

Master container
└── Master

Worker container
├── Worker
└── Executor 0（1 core、1024 MiB）
```

## cluster mode：Master 將 Driver 與 Executor 都放到 Worker

接著用同一個 Standalone cluster 以 cluster mode 提交 Spark 內建的 JVM 範例 `SparkPi`。Spark Standalone 的 cluster deploy mode 不支援 Python application，因此這裡使用 image 內建的 Java／Scala 範例；要驗證的仍是 Driver 的位置，而不是 `SparkPi` 的計算內容。

這次 Master UI 同時顯示 `Running Applications (1)` 與 `Running Drivers (1)`。Worker 的 2 cores 和 2.0 GiB memory 都在使用中：1 core、1024 MiB 分給 cluster-mode Driver，另 1 core、1024 MiB 分給 Executor。

![cluster mode：Master 同時追蹤 Running Application 與 Running Driver，兩者都位於同一個 Worker](https://raw.githubusercontent.com/jeffery5bai/spark-learning-lab/main/episodes/ep15-standalone-cluster/screenshots/cluster-mode-master.png)

此時 Worker container 裡同時有三種角色：常駐的 Worker daemon、由 Master 啟動的 Driver，以及由 application 使用的 Executor。提交端只負責將 application 送到 Master；`spark-submit` 成功提交後即可結束。

```text
提交端
└── spark-submit（提交後結束）

Master container
└── Master
       │ 排程 Driver 與 Executor
       ▼
Worker container
├── Worker
├── Driver（SparkPi）
└── Executor 0（SparkPi 的 task）
```

在只有一個 Worker 的最小實驗中，Driver 與 Executor 落在同一個 Worker 很自然；有多個 Worker 時，Master 會依可用資源安排它們，不能把「cluster mode 的 Driver 一定和每個 Executor 同機」當成規則。

## 實務上先問：誰要維持 Driver 的生命週期？

這次的選擇不是效能調校，而是 application 生命週期的取捨。client mode 很適合在開發機、notebook 或短暫互動中直接看 Driver；提交端若中斷，application 通常也會受到影響。cluster mode 則把 Driver 交由 cluster 中的 Worker 執行，提交端可以離開，但 Driver 的 logs、UI 位址、失敗處理與資源配置也需要由 cluster 環境來觀察與管理。

真實平台的 cluster manager 可能是 YARN 或 Kubernetes，名稱和 UI 都會不同；這幾個核心問題仍然相同：Driver 在哪裡、Executor 被配置在哪裡、誰提供資源、我該去哪裡找 log 與 UI。

## 這次我真正學到的是什麼？

從 `local` mode 走進 cluster，不是把一段 PySpark 程式「搬去更多機器」這麼簡單；它讓 Driver、資源管理與 task 執行的位置變得可分開觀察。這次我用 Master UI、Worker UI 與 Driver UI 看見了各自的證據，也釐清了 `Drivers: 0` 在 client mode 的真正含義。

下一篇會沿著這個 cluster，把焦點放到一個已經拿到 Executor 的 application：Driver 產生的 task，究竟如何被送往 Executor 並在 Spark UI 中留下 Stage 與 Task 的痕跡？

---

## 完整程式碼與參考資料

🚀 GitHub：\
[Ep15｜我的第一個 Spark cluster：Master、Worker、Driver、Executor 分別在哪裡？](https://github.com/jeffery5bai/spark-learning-lab/tree/main/episodes/ep15-standalone-cluster)

🚀 spark-learning-lab 系列文章與實作：\
[https://github.com/jeffery5bai/spark-learning-lab](https://github.com/jeffery5bai/spark-learning-lab)

Apache Spark 3.4.4 — Cluster Mode Overview：\
[https://archive.apache.org/dist/spark/docs/3.4.4/cluster-overview.html](https://archive.apache.org/dist/spark/docs/3.4.4/cluster-overview.html)

Apache Spark 3.4.4 — Spark Standalone Mode：\
[https://archive.apache.org/dist/spark/docs/3.4.4/spark-standalone.html](https://archive.apache.org/dist/spark/docs/3.4.4/spark-standalone.html)
