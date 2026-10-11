# Ep16 experiment

本集沿用 Ep15 的 Podman Spark Standalone cluster，觀察一個 Driver 如何將 task 送到 Executor。

實驗固定以下條件：

- 一個 Worker：提供 2 cores、3 GiB memory。
- 一個 application：使用 client deploy mode，讓 Driver UI 固定開在提交端的 `http://localhost:4040`。
- 一個 Executor：配置 2 cores、1024 MiB memory。
- 六個 input partitions：每個 partition 的 task 暫停數秒，讓 Spark UI 能顯示同時執行中的 task。

預期觀察順序：

1. Driver 對 `count()` 的 action 建立一個 Job。
2. Job 的 Stage 產生六個 tasks，因為來源有六個 partitions。
3. Executor 只有兩個 cores，所以同一時間至多執行兩個 tasks；其餘 tasks 留在 Driver 的排程佇列，等待 task slot 空出。
4. Worker UI 顯示一個 Executor；Driver UI 的 Stage 頁顯示 task 逐批完成；executor log 則能辨識各 partition 實際執行的位置。

`task_observer.py` 是本集的 application。`submit_task_observer.sh` 假設 Ep15 的 Master 與 Worker 已啟動，並用新的 `ep16-spark-submit` container 提交 application。
