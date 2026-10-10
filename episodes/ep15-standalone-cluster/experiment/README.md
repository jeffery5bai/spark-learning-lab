# Ep15 experiment

本集用 Podman 建立可重複的 Spark Standalone cluster。

- `start_master.sh`：建立實驗專用 network，並啟動 Spark Master。
- `start_worker_1.sh`：啟動第一個 Worker，讓它向 Master 註冊 2 cores、3 GiB 記憶體。
- `submit_client_app.sh`：由另一個 container 以 client mode 提交 PySpark application；該 container 同時承載 Driver。
- `client_driver.py`：執行一個小 action 後保留 application 5 分鐘，供觀察 Driver 與 Executor。
- `submit_cluster_app.sh`：以 cluster mode 提交 image 內建的 SparkPi；Master 會將 Driver 安排到 Worker。
