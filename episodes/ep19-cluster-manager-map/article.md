# Ep19｜同一個 Spark application，放到 YARN 或 Kubernetes 後誰替它找資源？

在真實的大型資料場景裡，許多 applications、團隊與排程工作會同時競爭 CPU、memory 與機器。透過單一 Standalone cluster 的實作，已經幫我理解 Driver、Executor 與 task 的基本關係，但是在企業環境中，需要更穩定、可擴展的資源管理平台，處理多個 workload 的資源配置、隔離、生命週期與失敗等等。

YARN 與 Kubernetes 是業界最常見的兩種選擇。兩者都能在多台機器間配置 CPU、memory 與執行單位，讓 Spark 取得 Executors；Kubernetes 也能接上現代 container、雲端與部署工具鏈。這一集會以 Standalone 作為基準，先建立三者的共同地圖，之後兩集再分別深入介紹 YARN 與 Kubernetes 的平台細節。

## 讀完這篇後你會更了解……

- Standalone、YARN 與 Kubernetes 上都成立的 Spark application 執行路徑。
- Driver、Executor 與 cluster manager 各自負責的事。
- Master／Worker、ResourceManager／NodeManager、control plane／kubelet 如何對照。
- YARN container 與 Kubernetes Pod 和 Executor 的關係。
- 面對陌生 Spark 平台時，如何先找到 Driver、資源配置、UI 與 logs。

## 這一課在系列中的位置：先有共同地圖，再走進兩個平台

```text
第四段：Standalone cluster
Ep15–Ep18 已經確認角色、task、資料位置與 task retry
        ↓
第五段：Spark 跑在不同平台上
Ep19 先建立 Standalone、YARN、Kubernetes 的共同地圖
        ↓
Ep20 深入 Spark on YARN
        ↓
Ep21 深入 Spark on Kubernetes
```

這篇文章算是一個 Overview，重點是把 Spark 官方文件中的平台架構，具體對照前四集已經實際觀察過的 Standalone 行為。各平台的提交設定、deploy mode 細節、logs 與 UI 等差異，會在後兩集深入討論。

## 先抓住不變的主線：Driver 需要 Executor，平台提供 Executor 的落點

不論 `--master` 指向 Standalone、YARN 或 Kubernetes，一個 Spark application 的主要資料流都可以先看成這樣：

```text
spark-submit
    ↓
Driver 啟動或被交給平台啟動
    ↓
Driver 向 cluster manager 要求 Executor 所需資源
    ↓
cluster manager 在可用節點配置執行單位
    ↓
Executor 啟動、向 Driver 註冊
    ↓
Driver 將 stage 中的 tasks 交給 Executors
```

這張圖連回 Ep15 與 Ep16：Driver 仍然建立 Job、Stage 與 task；Executor 仍然執行 task，保存 shuffle data 或 cache blocks；每個 application 仍有自己專屬的 Executor processes。Spark 官方的 cluster overview 也以同一個順序描述：`SparkContext` 先連上 cluster manager、取得 Executors，再將 tasks 送到 Executors。

platform 改變後，最重要的轉換只有一件事：**Standalone 的 Master 直接管理 Workers；YARN 與 Kubernetes 則由各自的平台元件管理整個叢集資源，Spark 透過它們取得 Executor 的執行位置。**

> *Spark 負責把資料處理工作拆成 tasks；cluster manager 負責把執行 Executor 所需的 CPU 與 memory 放到可用的節點上。*

## 先切開責任邊界：Spark application 做什麼，cluster manager 做什麼？

這個區分能避免把所有平台元件都誤認成「另一種 Executor」。

| 層次 | 主要責任 | 在 Ep15–Ep18 已看過的樣子 |
| --- | --- | --- |
| Spark application 的 Driver | 規劃 Job／Stage、排程 task、追蹤成功與失敗、請求 Executor 資源 | task retry 時，Driver 找出缺少成功結果的 partitions 並重新排程。 |
| Spark application 的 Executor | 執行 task，提供 CPU 與 memory 給 application 的計算，也持有部分執行期資料 | 在 Worker 上跑 task，寫 shuffle files、保存 cache blocks。 |
| cluster manager | 接受資源請求、選擇可用節點、啟動或終止 application 的執行單位、回報資源狀態 | Standalone 的 Master 與 Worker 協作配置 Executor。 |
| 節點上的平台 agent | 在那台機器上啟動、監控平台要求的 process 或隔離單位 | Standalone 的 Worker daemon。 |

Driver 與 Executor 屬於 Spark application；cluster manager 與節點屬於資源平台。它們需要合作，但不會互相取代。舉例來說，YARN 的 NodeManager 或 Kubernetes 的 kubelet 都不會執行 Spark task；它們替平台啟動 container 或 Pod，裡面的 Executor process 才會執行 task。

## 三種平台的名稱放回同一張地圖

先以「哪個元件完成哪項責任」對照，不急著把名稱硬翻成一對一關係：

| 要解決的事 | Standalone | YARN | Kubernetes |
| --- | --- | --- | --- |
| 接受 Spark 的資源請求、協調叢集資源 | **Master** | **ResourceManager** | **Kubernetes control plane**；API server 接收請求，scheduler 選擇 Node |
| 節點上代表平台、提供可配置資源 | **Worker** | **NodeManager** | **kubelet** 所管理的 Node |
| 承載一個 Executor 的平台單位 | Worker 啟動的 Executor process | **YARN container** | **Executor Pod** |
| Spark 的執行者 | Executor process | container 內的 Executor process | Executor Pod 內的 Executor process |
| Driver 的落點 | 由 deploy mode 決定 | 由 deploy mode 與 YARN application 模型決定 | 由 deploy mode 與 Pod 配置決定 |

這張表有兩個需要特別停下來記住的地方。

第一，**container 與 Pod 是承載 Spark process 的平台單位**。Spark Executor 仍是執行 task 的 process；它由 YARN container 或 Kubernetes Executor Pod 提供執行空間與資源。

第二，表中的名稱只用來定位責任，不能機械地視為一對一翻譯。YARN 的 ApplicationMaster、ResourceManager 與 NodeManager 如何合作，以及 Kubernetes control plane、Driver Pod 與 Executor Pod 的完整互動，分別留到 Ep20、Ep21 再展開。

## 同一個資源請求，在三種平台會長成不同的外殼

假設 Driver 想要幾個具備指定 CPU 與 memory 的 Executors。Spark 的需求本身沒有變；平台會用自己的資源物件表達與實現它：

```text
Driver 想要 Executor resources
        │
        ├─ Standalone：Master 在 Worker 的 cores、memory 中配置 Executor process
        ├─ YARN：ResourceManager 配置 container；NodeManager 在選定節點啟動它
        └─ Kubernetes：Driver 建立 Executor Pod 的需求；scheduler 將 Pod 放到 Node，kubelet 啟動它
```

因此，讀設定時要分清楚「Spark 想給 Executor 多少資源」和「平台如何落實資源隔離」。例如 `spark.executor.memory` 描述 Executor 可使用的 JVM memory；YARN 會將 Executor 與 ApplicationMaster 放進 containers，Kubernetes 則以 Pod 的 CPU、memory requests／limits 與 Spark 設定一起形成實際排程條件。

## deploy mode 仍然只先問一件事：Driver 在哪裡？

Ep15 已經建立過 deploy mode 最關鍵的判讀方式：找 Driver 的 process 在哪裡持續活著。這個問題在所有平台都有效。

| 問題 | client mode 的方向 | cluster mode 的方向 |
| --- | --- | --- |
| Driver 在哪裡？ | 通常留在提交端或由提交端直接維持。 | 交給 cluster manager 在 cluster 內啟動與維持。 |
| 提交端能否離開？ | Driver 仍依賴提交端的生命週期與網路可達性。 | application 已交給平台；提交端完成提交後可離開。 |
| 下一步該看哪裡？ | 提交端的 Driver logs／Spark UI，以及平台分配的 Executors。 | 平台中的 Driver、application／Pod／container logs，以及 Executors。 |

每個平台對 client mode 的支援方式與網路要求仍有差異。YARN 的 ApplicationMaster、Kubernetes 的 Driver Pod、client mode 的連線設定都會影響實際行為。這張表只固定判讀問題，避免在還沒介紹平台元件前背誦過多例外。

## 當我第一次接手一個 Spark 平台時，先用這四個問題定位現場

同一段 PySpark 或 Scala 程式放到不同平台，程式的 transformation 與 action 往往不需要改寫；排查與觀察的位置卻會改變。遇到一個陌生的 Spark application，我會先問：

1. **Driver 在哪裡？**
   這決定 Spark UI、Driver logs、Job／Stage 狀態主要從哪裡找，也關係到 client mode 的連線需求。
2. **Executor 由什麼平台單位承載？**
   在 Standalone 找 Executor process；在 YARN 找 containers；在 Kubernetes 找 Executor Pods。
3. **誰在配置資源，資源不足時我該看什麼？**
   Standalone 看 Master／Worker；YARN 看 ResourceManager、NodeManager 與 application containers；Kubernetes 看 Pod 狀態、scheduler events 與 Node 資源。
4. **平台 logs 與 Spark logs 各在哪裡？**
   Spark UI 描述 application 的 Job、Stage、task 與 Executor；平台 UI 或命令則說明 container／Pod 為何尚未啟動、被終止或無法放進某個 Node。

這四個問題把「Spark 為什麼還沒有跑起來」切成兩個可查的方向：Driver 是否已經取得資源，以及已啟動的 Executors 是否正在正確執行 Spark 工作。它們也會是 Ep20、Ep21 對照 UI 與 logs 的導航。

## 這次我真正學到的是什麼？

從 Standalone 走到 YARN 或 Kubernetes，不需要重新學一套 Spark 的 task 模型。Driver、Executor、partition、shuffle 與 task retry 仍是同一個 application 的核心概念。新的平台名稱描述的是資源管理與執行隔離的外層：YARN 使用 ApplicationMaster 與 containers，Kubernetes 使用 control plane 與 Pods。

下一篇會先走進 YARN，沿著這張地圖逐一確認 ResourceManager、NodeManager、ApplicationMaster 與 Executor container 的位置；也會將 client mode 與 cluster mode 的 Driver 位置落到具體的提交流程。

---

## 完整文章與參考資料

🚀 GitHub：\
[Ep19｜同一個 Spark application，放到 YARN 或 Kubernetes 後誰替它找資源？](https://github.com/jeffery5bai/spark-learning-lab/tree/main/episodes/ep19-cluster-manager-map)

🚀 spark-learning-lab 系列文章與實作：\
[https://github.com/jeffery5bai/spark-learning-lab](https://github.com/jeffery5bai/spark-learning-lab)

Apache Spark 3.4.4 — Cluster Mode Overview：\
[https://archive.apache.org/dist/spark/docs/3.4.4/cluster-overview.html](https://archive.apache.org/dist/spark/docs/3.4.4/cluster-overview.html)

Apache Hadoop — YARN：\
[https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YARN.html](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YARN.html)

Kubernetes Documentation — Overview：\
[https://kubernetes.io/docs/concepts/overview/](https://kubernetes.io/docs/concepts/overview/)

Apache Spark 3.4.4 — Running Spark on YARN：\
[https://archive.apache.org/dist/spark/docs/3.4.4/running-on-yarn.html](https://archive.apache.org/dist/spark/docs/3.4.4/running-on-yarn.html)

Apache Spark 3.4.4 — Running Spark on Kubernetes：\
[https://archive.apache.org/dist/spark/docs/3.4.4/running-on-kubernetes.html](https://archive.apache.org/dist/spark/docs/3.4.4/running-on-kubernetes.html)
