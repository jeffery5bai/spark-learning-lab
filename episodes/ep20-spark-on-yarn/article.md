# Ep20｜Spark application 跑在 YARN 時，ResourceManager、ApplicationMaster、NodeManager 各自做什麼？

Ep19 把 Standalone、YARN 與 Kubernetes 放進同一張地圖後，我知道 YARN 能替 Spark 配置資源；這篇文章我們要進一步了解 Yarn 的各種機制：為什麼一個 Spark application 同時有 ResourceManager、ApplicationMaster、NodeManager 與 containers？Driver 到底在哪裡？Spark UI 又該從哪裡找？

這一集沿著一次 `spark-submit --master yarn` 的路徑，將這些元件放回各自的層級。我沒有用現有筆電硬建一套多節點 YARN，因此本文使用 Apache Hadoop 與 Spark 官方文件描述的架構與設定模型，不會有本機 YARN UI 的觀察截圖。目標是建立一條可用來閱讀真實 YARN application 的路徑。

## 讀完這篇後你會更了解……

- ResourceManager、NodeManager、ApplicationMaster 與 YARN container 的責任範圍。
- Spark application 在 YARN cluster mode 從提交到 Executor 註冊的完整路徑。
- client mode 與 cluster mode 中，Driver 和 ApplicationMaster 的位置如何不同。
- YARN container、Spark Executor 與 CPU／memory 資源的對應關係。
- ResourceManager UI、NodeManager、Spark UI、YARN Web Application Proxy 與 yarn logs 各適合查什麼。

## 這一課在系列中的位置：把 Ep19 的 YARN 欄位展開

~~~text
Ep19：同一張共同地圖
Standalone、YARN、Kubernetes 都需要 Driver 取得 Executors
        ↓
Ep20：Spark on YARN
ResourceManager、ApplicationMaster、NodeManager、containers 與 logs
        ↓
Ep21：Spark on Kubernetes
control plane、Driver Pod、Executor Pods 與 Pod resources
~~~

YARN 是 Hadoop 的資源管理平台，能讓多個 applications 在同一批 Nodes 上競爭與使用資源。這一集聚焦 Spark 如何使用它；不試圖完整介紹 HDFS、YARN queue policy、ResourceManager HA、Docker container runtime 或 Hadoop 安全設定。

## 先看全貌：YARN 分成整個叢集、單一 application 與單台 Node 三層

YARN 的設計先把資源管理與 application 協調拆開：整個 cluster 有一個 ResourceManager，每台 Node 有一個 NodeManager，每個 application 則有自己的 ApplicationMaster。Spark application 進入這個架構後，會再建立一個或多個 Executor processes。

~~~text
YARN cluster
│
├── ResourceManager
│   └── 在所有 applications 之間仲裁 resources
│
├── Node A
│   └── NodeManager
│       ├── container: ApplicationMaster（某個 application）
│       └── container: Spark Executor 0
│
└── Node B
    └── NodeManager
        ├── container: Spark Executor 1
        └── container: 其他 application 的工作
~~~

| 元件 | 所在層級 | 主要責任 | 先把它想成共享辦公園區裡的…… |
| --- | --- | --- | --- |
| **ResourceManager** | 整個 YARN cluster | 接受 application、依 queue 與可用資源配置 containers。 | **園區的資源調度中心**：知道整個園區還有多少空間，決定哪個專案何時能取得多少位置。 |
| **NodeManager** | 每台 Node | 啟動、監控本機 containers，回報 CPU、memory、disk、network 等使用狀況。 | **每棟樓的現場管理員**：在自己的樓裡準備工作空間、啟動人員需要的環境，並回報樓內使用情況。 |
| **ApplicationMaster（AM）** | 一個 YARN application | 向 ResourceManager 申請 containers，與對應 NodeManagers 協調並追蹤 application 的執行單位。 | **一個專案的協調者**：替自己的專案向調度中心申請位置，確認分到哪些樓、工作是否順利開始。 |
| **YARN container** | 一份配置到某台 Node 的資源與執行空間 | 提供 memory、vcores、工作目錄與啟動環境，承載 AM 或 Executor process。 | **一張被保留的工作桌**：它有明確的空間與設備配額，可讓 AM 或 Executor process 在上面工作。 |

這個比喻只用來辨識角色層級。實際上 ResourceManager 分配的是 cluster resources，NodeManager 啟動的是 containers，ApplicationMaster 協調的是單一 application。

這個分層和 Standalone 有一個重要差異：Standalone Master 直接面對 Workers；YARN 多了一層「每個 application 自己的 AM」。ResourceManager 先分配資源，AM 再代表自己的 application 持續申請與使用 containers。

> *ResourceManager 管整個叢集的資源，ApplicationMaster 管一個 application 的資源協調，NodeManager 管一台機器上的 containers。*

## 從提交開始：cluster mode 的完整路徑

先看最能突顯 YARN 角色分工的 cluster mode。提交端告訴 Spark 使用 YARN，並把 Driver 交給 YARN 在 cluster 中啟動：

~~~bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  app.py
~~~

實際檔案、queue、memory 與 executor 數量還需要依環境補上設定；這段只用來固定 yarn 與 cluster 兩個判讀訊號。從提交到 tasks 執行，流程如下：

~~~text
提交端的 spark-submit
    ↓ 向 ResourceManager 提交 application
ResourceManager
    ↓ 配置第一個 container
某台 Node 的 NodeManager
    ↓ 啟動 ApplicationMaster container
ApplicationMaster container
    └── Spark Driver 在其中啟動
        ↓ Driver／AM 向 ResourceManager 請求 Executor containers
ResourceManager
    ↓ 將每個 container 配置到可用 Node
各 Node 的 NodeManager
    ↓ 啟動 Executor containers
Spark Executors
    ↓ 向 Driver 註冊，接收並執行 tasks
~~~

最先取得的是 ApplicationMaster 的 container。對 Spark on YARN 的 cluster mode 而言，Driver 跑在這個 ApplicationMaster process 中；因此提交端在 application 成功交給 YARN 後可以離開。後續需要多少 Executor containers，則由 application 持續向 ResourceManager 請求，ResourceManager 依叢集可用資源與排程規則決定何時、在哪台 Node 配置。

這裡能將 Ep15 的問題換成 YARN 版本：Driver 沒有直接到每台 Node 開 process；它透過 ApplicationMaster 與 ResourceManager 取得 containers，NodeManager 才在實際 Node 上啟動 Executor process。

## client mode：ApplicationMaster 還在，Driver 留在提交端

YARN 不管使用哪種 deploy mode，都會有 ApplicationMaster。差異只回到最重要的問題：Driver 的 process 在哪裡持續活著？

~~~text
YARN client mode

提交端
├── spark-submit
└── Spark Driver
       ↓ 向 YARN 註冊 application

YARN cluster
├── ApplicationMaster container
│   └── 為 application 申請 Executor containers
└── Executor containers
~~~

~~~text
YARN cluster mode

提交端
└── spark-submit（提交成功後可離開）

YARN cluster
├── ApplicationMaster container
│   └── Spark Driver
└── Executor containers
~~~

| 問題 | client mode | cluster mode |
| --- | --- | --- |
| Driver 在哪裡？ | 提交端的 client process。 | ApplicationMaster process 內。 |
| ApplicationMaster 做什麼？ | 向 YARN 申請與管理 Executor containers。 | 同時承載 Spark Driver，並協調 Executor containers。 |
| 提交端中斷的影響 | Driver 失去生命週期與網路位置，application 會受影響。 | 提交端已完成提交，application 繼續由 YARN cluster 維持。 |
| Spark UI 的起點 | 提交端的 Driver。 | ApplicationMaster container 中的 Driver。 |

client mode 有一個實務前提：Executors 必須能連回提交端的 Driver。提交端若在筆電、VPN 外或短暫的 shell session，網路可達性與生命週期都會影響 application。cluster mode 將 Driver 放入 YARN 的網路與資源邊界內，通常更適合長時間或正式排程的 jobs；互動式開發則常使用 client mode。

## container 是 YARN 的資源單位，Executor 是其中的 Spark process

YARN 的 container 容易讓人聯想到 Docker image，但傳統 YARN container 的核心是資源配置與啟動環境。NodeManager 會在某台 Node 上為它準備工作目錄、localize application 所需檔案，然後以指定的 CPU、memory 與命令啟動 process。

~~~text
Node
└── NodeManager
    ├── AM container
    │   └── ApplicationMaster process
    │       └── cluster mode 時同時包含 Spark Driver
    ├── Executor container 0
    │   └── Spark Executor JVM
    │       └── 可用多個 executor cores 平行執行 tasks
    └── Executor container 1
        └── Spark Executor JVM
~~~

一個 Node 可以同時有多個 containers；它們可以來自同一個 application，也可以屬於不同 application。一個 Spark Executor 通常對應一個 Executor container。若 `spark.executor.cores=4`，且 `spark.task.cpus=1`，這個 Executor 通常能同時執行四個 tasks；YARN 配給 container 的 vcores、memory，以及 Spark 的 memory overhead 等設定，必須能共同滿足這份 Executor 的需求。

Spark application 的 JAR、Python 檔或壓縮 dependencies 可透過 `--jars`、`--files`、`--archives` 與 Spark 的 YARN distribution 設定交給 NodeManager localize。這和 Kubernetes 為 Driver／Executor Pods 選擇 OCI image 是兩種不同模型。YARN 也能經管理者額外設定 Docker 或其他 container runtime，但那不是傳統 Spark on YARN 的預設提交方式。

## 我該去哪裡看：Spark UI、YARN UI 與 logs 回答不同問題

在 Standalone 實驗裡，我曾分開看 Master UI、Worker UI 與 Driver Spark UI。YARN 也需要分開看，只是入口換成 ResourceManager、NodeManager 與 application 的 Driver。

| 位置 | 它回答的問題 | 常見訊號 |
| --- | --- | --- |
| **ResourceManager UI** | application 有沒有被接受？是否取得 AM／Executor containers？cluster 資源或 queue 是否不足？ | `ACCEPTED`、`RUNNING`、application attempt、allocated containers。 |
| **NodeManager UI／Node logs** | 某個 Node 是否健康？本機 container 有沒有被啟動、終止或超出資源？ | container 狀態、local logs、node health。 |
| **Spark UI** | Driver 建立了哪些 Job、Stage、task？Executors 是否註冊？shuffle、GC、task failure 發生什麼？ | Jobs、Stages、Executors、SQL、Storage。 |
| **`yarn logs -applicationId <id>`** | 同一個 YARN application 的 container logs 裡實際寫了什麼？ | ApplicationMaster／Driver、Executor 的 stdout、stderr 與 framework logs。 |

ResourceManager UI 不會顯示 Spark 的 task timeline；Spark UI 也不會說明一個 container 為何因 queue 或 Node 資源不足而沒有被配置。兩者合起來才能判斷問題是在 Spark application 還是 YARN 平台層。

YARN 若啟用 log aggregation，完成後的 container logs 會被集中到 HDFS，`yarn logs -applicationId <id>` 可以從 cluster 其他位置查看。未啟用時，logs 留在當初執行 container 的 Node 上。這也是實務上追查「某個 Executor 為何退出」時，Spark UI 先指出哪個 Executor 有問題，再用 YARN container logs 查原因的常見路徑。

### Web Application Proxy：讓 application 的 Web UI 有一個受控入口

YARN 的 Web Application Proxy 預設可和 ResourceManager 一起運行，也能獨立部署。ApplicationMaster 會把自己的 Web UI tracking URL 提供給 ResourceManager；proxy 讓使用者透過 application 的受控入口前往該 Web UI，而不必先知道 ApplicationMaster 跑在哪台 Node、使用哪個動態埠。

它的主要目的包括降低直接連往非受信任 ApplicationMaster Web UI 的風險，以及為 application UI 提供統一入口。它代理的是瀏覽器的 Web UI 請求；Driver 與 Executor 的 RPC、shuffle fetch 與 task 資料傳輸不會經過這個 proxy。

## 遇到 application 沒跑起來時，先沿著資源路徑排查

YARN 將「平台能否給資源」和「Spark 是否把 task 跑對」分到不同地方。遇到問題時，我會先依序問：

1. **ResourceManager 有接受 application 嗎？**
   若 application 持續停在 `ACCEPTED`，先看 queue、可用資源與 ApplicationMaster container 是否尚未獲得配置。
2. **ApplicationMaster／Driver 是否已經啟動？**
   cluster mode 可從 application attempt 與 AM container logs 找線索；client mode 還要回到提交端確認 Driver。
3. **Executor containers 有被配置並成功啟動嗎？**
   ResourceManager 與 NodeManager 提供平台狀態；Spark UI 的 Executors tab 則顯示 Spark 是否已收到 Executors 註冊。
4. **task 出問題，還是 container 出問題？**
   task duration、shuffle、exception 先看 Spark UI；container exit code、Node 資源、stdout／stderr 則看 `yarn logs` 與 NodeManager。

> *Spark UI 追 application 的計算過程；YARN UI 與 container logs 追平台是否成功提供執行環境。先分開兩條線，排查才不會從錯的地方開始。*

## 這次我真正學到的是什麼？

YARN 沒有取代 Spark 的 Driver 與 Executor。它提供一個多 application 共用的資源平台：ResourceManager 仲裁整個 cluster、NodeManager 維護每台 Node、ApplicationMaster 代表一個 application 取得 containers。Spark 再將 Driver 與 Executors 放進這些 containers，延續我在 Standalone cluster 已經看過的 Job、Stage、task、shuffle 與 retry 模型。

下一篇會把同一條資源路徑換成 Kubernetes：ResourceManager 會變成 control plane 的 API 與 scheduler，YARN containers 會變成 Driver Pod 與 Executor Pods。我會特別對照「YARN container 不等於 Docker image」這個差異，理解 Spark 在 Kubernetes 上如何用 image 與 Pod resources 描述執行環境。

---

## 完整文章與參考資料

🚀 GitHub：\
[Ep20｜Spark application 跑在 YARN 時，ResourceManager、ApplicationMaster、NodeManager 各自做什麼？](https://github.com/jeffery5bai/spark-learning-lab/tree/main/episodes/ep20-spark-on-yarn)

🚀 spark-learning-lab 系列文章與實作：\
[https://github.com/jeffery5bai/spark-learning-lab](https://github.com/jeffery5bai/spark-learning-lab)

Apache Hadoop — YARN：\
[https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YARN.html](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YARN.html)

Apache Hadoop — Web Application Proxy：\
[https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/WebApplicationProxy.html](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/WebApplicationProxy.html)

Apache Spark 3.4.4 — Running Spark on YARN：\
[https://archive.apache.org/dist/spark/docs/3.4.4/running-on-yarn.html](https://archive.apache.org/dist/spark/docs/3.4.4/running-on-yarn.html)
