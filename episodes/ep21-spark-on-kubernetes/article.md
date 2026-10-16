# Ep21｜Spark application 跑在 Kubernetes 時，Driver Pod、Executor Pod 與 scheduler 怎麼合作？

在 Ep20 中我們認識了 Yarn 平台，ResourceManager、ApplicationMaster、NodeManager 與 container 分別負責哪些事等等。這一集我們要來介紹另一個業界常採用的工作平台 Kubernetes，最先遇到的是一整組新的名詞：API Server、scheduler、kubelet、Pod、container image、namespace。它們會改變 Spark 怎麼取得資源、Driver 和 Executor 落在哪裡，以及出問題時該去哪裡找證據。

這篇沿著一次 `spark-submit --master k8s://...` 的路徑，將 Kubernetes 元件放回 Spark application 的執行流程。現有筆電沒有合適的 Kubernetes 叢集可供觀察，因此本文使用 Spark 3.4.4 與 Kubernetes 官方文件中的架構、提交與設定模型，目標是建立一條閱讀真實 Spark on Kubernetes application 的路徑。

## 讀完這篇後你會更了解……

- Kubernetes control plane、Node、kubelet、Pod 與 Spark Driver／Executor 的責任範圍。
- Spark application 在 Kubernetes cluster mode 從提交到 Executor 註冊的完整路徑。
- Driver Pod、Executor Pod、container 與 container image 的層級關係。
- `spark.executor.cores`、Pod CPU request／limit 與 memory 設定各自影響什麼。
- `kubectl`、Driver Spark UI 與 Pod logs 各適合查什麼，以及 client mode 的網路條件。

## 這一課在系列中的位置：把 Ep19 的 Kubernetes 欄位展開

~~~text
Ep19：同一張共同地圖
Standalone、YARN、Kubernetes 都需要 Driver 取得 Executors
        ↓
Ep20：Spark on YARN
ResourceManager、ApplicationMaster、NodeManager、containers 與 logs
        ↓
Ep21：Spark on Kubernetes
control plane、Driver Pod、Executor Pods、Pod resources 與 logs
~~~

Kubernetes 是通用的 container orchestration 平台，Spark 只是其中一種會建立 Pod 的 workload。這一集聚焦 Spark 如何使用 Kubernetes API 與 Pods；不會特別展開 Kubernetes 安裝、叢集維運、Helm、Service Mesh、autoscaling、雲端供應商差異或完整 RBAC 設計。

## 先看全貌：Kubernetes 提供 Pod 的生命週期，Spark 仍管理自己的 tasks

~~~text
Kubernetes cluster
│
├── control plane：管理整個 cluster 與安排工作位置
│
├── Node A
│   └── Driver Pod
│       └── Spark Driver process
│
└── Node B
    ├── Executor Pod 0
    │   └── Spark Executor process
    └── Executor Pod 1
        └── Spark Executor process
~~~

先從外到內讀這張圖：Kubernetes cluster 有一個負責管理全局的 control plane，以及一台或多台提供運算資源的 Nodes。Kubernetes 將每份工作放進 Pod，再將 Pod 配置到某台 Node。

Spark application 放進這個結構後，Driver 位於 Driver Pod，每個 Executor 位於一個 Executor Pod。Driver 與 Executors 仍各自負責 Spark 的工作分工；Kubernetes 則負責讓這些 Pods 取得 Node 上的資源並維持生命週期。

接著再把 control plane 與 Node 中真正需要辨識的 Kubernetes 元件放進表裡：

| 元件 | 所在層級 | 主要責任 | 先把它想成共享辦公園區裡的…… |
| --- | --- | --- | --- |
| **Kubernetes API Server** | control plane | 提供 Kubernetes API，接收並保存 Pod 等資源物件的期望狀態。 | **園區的申請窗口與登記系統**：記錄每個專案希望使用哪些工作空間。 |
| **kube-scheduler** | control plane | 為還沒綁定 Node 的 Pod 選擇合適 Node。 | **座位安排員**：依可用空間與規則，決定每張工作桌放進哪棟樓。 |
| **kubelet** | 每台 Node | 確保分配到該 Node 的 Pod 裡 containers 正常運行。 | **每棟樓的現場管理員**：在自己的樓內準備並持續檢查工作空間。 |
| **Driver Pod** | 一個 Spark application | 承載 Spark Driver；Driver 透過 Kubernetes API 建立與追蹤 Executor Pods。 | **專案負責人的辦公室**：規劃工作、申請人力，並接收執行結果。 |
| **Executor Pod** | 一個 Spark application | 承載一個 Spark Executor process，執行 Driver 指派的 tasks。 | **一位工作者的工作空間**：取得任務後實際進行計算。 |

這張表先定位責任。API Server、scheduler 與 kubelet 是 Kubernetes 平台元件；Driver 和 Executor 是 Spark application 元件。Pod 是 Kubernetes 排程與生命週期管理的單位，Executor 是 Pod 中實際執行 Spark task 的 process。

> *Kubernetes 決定 Pod 能在哪個 Node 運行；Spark Driver 決定拿到 Executor 後，要讓它執行哪些 tasks。*

## 從提交開始：cluster mode 的完整路徑

先看最能完整呈現平台生命週期的 cluster mode。`--master` 指向 Kubernetes API Server，並指定可在 Pod 中啟動 Spark 的 container image：

~~~bash
spark-submit \\
  --master k8s://https://<api-server-host>:<api-server-port> \\
  --deploy-mode cluster \\
  --name example-job \\
  --conf spark.kubernetes.container.image=<registry>/<spark-image>:<tag> \\
  local:///path/in/image/app.py
~~~

這只是固定流程所需訊號的示意。真實提交還需要依環境指定 namespace、application 檔案位置、ServiceAccount、image registry 存取權限與資源設定。

整條路徑如下：

~~~text
提交端的 spark-submit
    ↓ 向 Kubernetes API Server 建立 Driver Pod
API Server
    ↓ 記錄 Driver Pod 的期望狀態
kube-scheduler
    ↓ 選擇一台可容納 Driver Pod 的 Node
該 Node 的 kubelet
    ↓ 拉取 image、啟動 Driver Pod 的 container
Spark Driver
    ↓ 透過 API Server 建立 Executor Pod 請求
kube-scheduler
    ↓ 為每個 Executor Pod 選擇 Node
各 Node 的 kubelet
    ↓ 啟動 Executor Pod
Spark Executors
    ↓ 向 Driver 註冊，接收並執行 tasks
~~~

Spark 文件直接描述這個分工：Spark 建立在 Pod 裡運行的 Driver，Driver 再建立同樣運行於 Pods 的 Executors；Driver 與 Executor Pods 的調度由 Kubernetes 處理。這也讓我能用 Ep20 的 YARN 流程做對照：YARN 由 ApplicationMaster 向 ResourceManager 申請 Executor containers；Kubernetes 上的 Driver 則透過 API Server 建立 Executor Pods，由 scheduler 為它們找 Node。

Driver Pod 需要能使用 Kubernetes API。實務上常由 ServiceAccount 與 RBAC 授予它建立、查看與刪除 Pods、Services、ConfigMaps 等必要權限。當 Executor Pods 沒有出現時，Driver Pod 的 log、ServiceAccount 權限與 API Server 回應，都是優先檢查的證據。

## Pod、container、image 與 Executor：四個不同層次的單位

這幾個詞經常一起出現，先把層次切開：

~~~text
container image
└── 可被拉取的執行環境與檔案內容
        ↓ 被 Kubernetes 用來啟動 container

Executor Pod
└── Kubernetes 的排程與生命週期單位
    └── container
        └── Spark Executor JVM／Python worker 執行 tasks
~~~

container image 是可部署的檔案系統與執行環境。Spark on Kubernetes 以 `spark.kubernetes.container.image` 指定 Driver 與 Executor 預設使用的 image；也能分別以 driver、executor 專屬設定使用不同 image。Pod 是 Kubernetes 把一組 containers、網路與資源設定放在一起調度的單位。Spark Executor 則是其中實際執行 task 的 JVM process。

一個 Executor Pod 通常承載一個 Executor。Pod 可以有其他 sidecar containers，Spark Executor 並不等同整個 Pod；在最小 Spark 設定裡，它通常是 Pod 裡的主要 container。Driver Pod 也遵循同一個層次，只是它承載的是 Spark Driver。

application 程式與 dependencies 也需要讓 Driver／Executor Pods 能存取。它們可以預先存在 image 內、放在遠端儲存位置供 Spark 取得，或依提交機制上傳到 Hadoop-compatible filesystem。這是 Kubernetes 與先前本機或 YARN 提交最容易被忽略的差異：提交端本機磁碟上的檔案，不會自動出現在每一個 Pod 裡。

## Executor 的 task slots 與 Pod 可被排程的資源，要分開看

Standalone 實驗裡，`spark.executor.cores` 讓我知道一個 Executor 能同時跑多少 task。在 Kubernetes 上，這個 Spark 層級的平行度概念仍然存在；Kubernetes 另外還需要知道 Pod 排程時應保留與限制多少 CPU、memory。

| 問題 | 主要設定與層次 | 它影響什麼 |
| --- | --- | --- |
| 一個 Executor 可同時執行多少 task？ | Spark 的 `spark.executor.cores`，並受 `spark.task.cpus` 影響 | Executor 內可同時執行的 task 數。例：4 cores、每 task 需 1 core 時，通常可同時跑 4 個 tasks。 |
| scheduler 替每個 Executor Pod 尋找 Node 時，至少要預留多少 CPU？ | `spark.kubernetes.executor.request.cores` | Kubernetes 排程判斷 Node 是否可容納 Pod 的 CPU request；此設定不改變 task parallelism。 |
| Executor Pod 最多可使用多少 CPU？ | `spark.kubernetes.executor.limit.cores` | Kubernetes 對 Pod 設定的 CPU hard limit。 |
| Executor 可用多少 JVM heap？ | `spark.executor.memory` | Spark Executor JVM 的 heap 記憶體。 |
| Pod 需要向 Kubernetes 申請多少記憶體？ | Executor memory、memory overhead 與 Kubernetes Pod 的 memory request／limit | Pod 的實際資源需求與 Kubernetes 是否能將它排進 Node。 |

`spark.kubernetes.executor.request.cores` 若被設定，Spark 文件明確說它用於 Executor Pod 的 CPU request，並優先於 `spark.executor.cores` 作為 request 值；它不會改變一個 Executor 的 task parallelism。這個差異很重要：把 Pod 的 request 調小，可能讓 scheduler 更容易放入 Pod，卻不會憑空增加 Executor 實際可穩定執行的 CPU 資源。

當 Pod 長時間停在 `Pending`，第一個問題通常是「request 是否超過任何可用 Node 的資源或平台規則」。當 Pod 已經 `Running`，但 task 慢、GC 高或出現 OOM，才回到 Spark UI、Executor logs、memory 設定與資料分布查看。

> *Spark 的 Executor cores 描述 task 的並行能力；Kubernetes 的 requests 與 limits 描述 Pod 在平台上要取得與可使用的資源邊界。*

## 我該去哪裡看：Pod 狀態、Spark UI 與 logs 回答不同問題

Kubernetes 沒有 Standalone Master UI 那樣專門面向 Spark 的單一畫面。實務上會將 Kubernetes CLI 或平台 UI 與 Spark UI 並用：前者回答 Pod 為何沒有啟動或被終止，後者回答 Spark 的 Job、Stage、task 與 Executor 如何運作。

| 位置或指令 | 它回答的問題 | 常見訊號 |
| --- | --- | --- |
| `kubectl get pods -n <namespace>` | Driver／Executor Pods 是否存在，以及目前處於哪個 phase？ | `Pending`、`Running`、`Succeeded`、`Failed`、重啟次數。 |
| `kubectl describe pod <pod> -n <namespace>` | scheduler 為何還沒配置 Pod？image、volume、資源或事件發生什麼？ | Events、`FailedScheduling`、`ImagePullBackOff`、資源不足。 |
| `kubectl logs <pod> -n <namespace>` | Driver 或特定 Executor container 的 stdout、stderr 與 application log 寫了什麼？ | Python exception、Driver log、Executor log、container 啟動錯誤。 |
| Driver Spark UI | Spark 是否建立 Job／Stage／task？Executors 是否註冊？shuffle、GC、task failure 如何發生？ | Jobs、Stages、Executors、SQL、Storage。 |

Spark 官方文件示範可用下列方式將 Driver Pod 的 Spark UI 暫時轉送到本機：

~~~bash
kubectl port-forward <driver-pod-name> 4040:4040
~~~

接著在本機開啟 `http://localhost:4040`。正式平台也可能透過 Service、Ingress 或公司既有的 proxy 提供入口；那些做法處理網路暴露與存取控制，Spark UI 的來源仍是 Driver。

排查時可以沿著資源路徑逐步縮小範圍：

1. **Driver Pod 有被建立並排到 Node 嗎？** 先看 `kubectl get pods` 與 `kubectl describe pod` 的 Events。
2. **Driver 能與 API Server 溝通並建立 Executor Pods 嗎？** 看 Driver Pod logs、ServiceAccount 權限與 Kubernetes API 錯誤。
3. **Executor Pods 有 Running 並向 Driver 註冊嗎？** 看 Pod 狀態、Executor logs 與 Spark UI 的 Executors tab。
4. **已開始跑 task 後才失敗或變慢嗎？** 看 Spark UI 的 task、shuffle、GC 與 Executor metrics，再回看特定 Pod 的 logs 和終止原因。

## client mode 仍可使用，但 Driver 網路與生命週期需要額外確認

Kubernetes 上也支援 client mode。此時 Driver 可以跑在提交端實體主機，也可以已經位在一個 Pod 裡；Executor Pods 仍由 Kubernetes 執行。核心前提和 Ep15、Ep20 相同：Executors 必須能透過可路由的 hostname 與 port 連回 Driver。

~~~text
Kubernetes client mode

提交端或既有 Driver Pod
└── Spark Driver
       ↓ 透過 Kubernetes API 建立 Executor Pods

Kubernetes cluster
└── Executor Pods
       ↓ 必須能連回 Driver
~~~

如果 Driver 本身位在 Kubernetes Pod，Spark 文件建議以 headless Service 提供穩定、可路由的 hostname，並設定 `spark.driver.host` 與 `spark.driver.port`。Driver 位於叢集外時，則需要確認 Kubernetes 網路、VPN、防火牆與 DNS 都允許 Executor Pods 主動連回提交端。

cluster mode 將 Driver 放進 Driver Pod，提交端完成提交後即可離開；client mode 保留提交端的 Driver，適合互動情境，也讓網路與 shell 生命週期成為 application 成功的一部分。這兩種模式都能使用 Kubernetes，選擇時先確認 Driver 該在哪裡持續活著，以及 Executors 能否可靠地連到它。

## 這次我真正學到的是什麼？

Spark 放到 Kubernetes 後，Job、Stage、task、shuffle 與 Executor 的核心模型沒有改變。改變的是 Spark 取得與維護執行環境的方式：`spark-submit` 建立 Driver Pod，Driver 透過 Kubernetes API 建立 Executor Pods，scheduler 決定 Pod 落在哪個 Node，kubelet 確保 Pod 內的 containers 運行。

因此，排查也有兩條清楚的線：Spark UI 用來理解計算過程；`kubectl` 與 Pod logs 用來理解 Pod 是否取得資源、成功啟動，或為何被終止。下一段會從這張平台地圖回到實務診斷：當 Spark job 變慢時，該先從哪些 UI 指標與執行證據開始。

---

## 完整程式碼與參考資料

🚀 GitHub：\\
[Ep21｜Spark application 跑在 Kubernetes 時，Driver Pod、Executor Pod 與 scheduler 怎麼合作？](https://github.com/jeffery5bai/spark-learning-lab/tree/main/episodes/ep21-spark-on-kubernetes)

🚀 spark-learning-lab 系列文章與實作：\\
[https://github.com/jeffery5bai/spark-learning-lab](https://github.com/jeffery5bai/spark-learning-lab)

Apache Spark 3.4.4 — Running Spark on Kubernetes：\\
[https://archive.apache.org/dist/spark/docs/3.4.4/running-on-kubernetes.html](https://archive.apache.org/dist/spark/docs/3.4.4/running-on-kubernetes.html)

Kubernetes Documentation — Components：\\
[https://kubernetes.io/docs/concepts/overview/components/](https://kubernetes.io/docs/concepts/overview/components/)

Kubernetes Documentation — Pods：\\
[https://kubernetes.io/docs/concepts/workloads/pods/](https://kubernetes.io/docs/concepts/workloads/pods/)
