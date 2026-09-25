# spark-learning-lab

這是一個從「我每天都在用 Spark，但其實沒有完全看懂它」開始的學習 lab。

平常寫 Python、SQL，也能把 PySpark job 跑起來；不過當 job 變慢、資源不夠、
或是看到 Driver、Executor、YARN、shuffle 這些字時，我常常只能知道它們大概有
關係，卻說不清楚中間到底發生了什麼。

所以我想從日常工作裡真的會遇到的問題出發，自己做小實驗、看 Spark UI、讀 log，
再把理解過程留下來。希望最後不只會「把 Spark 跑起來」，也能比較有底氣地理解它
為什麼這樣跑、出問題時可以從哪裡開始看。

## 這裡會放什麼？

內容會隨著學習慢慢補上。目前預計會包含：

- local mode 下的 PySpark 與 Spark UI
- Job、Stage、Task、partition 與 shuffle 的實驗紀錄
- join、資料傾斜、資源設定等效能問題
- 從單機到小型 Spark cluster，以及後續的 YARN／Kubernetes 探索
- 每個主題對 MLE 日常工作的實際影響

程式、操作紀錄與文章會盡量放在一起。不是為了做一份看起來很厲害的答案集，而是
保留「當時我怎麼理解、怎麼驗證」的過程。

## 開始前需要什麼？

目前的本機環境已用下面這組版本驗證過：

| 項目 | 版本 |
| --- | --- |
| Python | 3.11 |
| PySpark | 3.4.4 |
| Java | 17 |
| 套件管理 | uv |

Java 可以和電腦裡其他版本並存。這個專案在 macOS 上使用 Eclipse Temurin 17；
它是常見的 OpenJDK 發行版，Spark 不要求特定 Java 廠商，只要是相容的 Java 17 即可。

### macOS 安裝方式

先準備 Git、[uv](https://docs.astral.sh/uv/)，以及 Java 17：

```bash
brew install --cask temurin@17
```

接著 clone repo，並讓 uv 建立 Python 環境與安裝鎖定的套件：

```bash
git clone https://github.com/jeffery5bai/spark-learning-lab.git
cd spark-learning-lab
uv python install 3.11
uv sync
```

執行 Spark 前，在目前這個 terminal 選擇 Java 17：

```bash
export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
export PATH="$JAVA_HOME/bin:$PATH"
```

想確認環境有沒有通，可以跑這個最小測試：

```bash
uv run python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.master('local[2]').appName('smoke-test').getOrCreate(); print(f'Spark {spark.version} | count={spark.range(3).count()}'); spark.stop()"
```

若看到 `Spark 3.4.4 | count=3`，代表 PySpark、Java 與本機 Spark 都能正常合作。
在 macOS 上看到 native Hadoop library 的 warning 是 local mode 常見訊息，可以先不用緊張。

## 目錄（持續更新中）

```text
.
├── README.md          # 你正在看的首頁
├── pyproject.toml     # Python 與 PySpark 相依設定
├── uv.lock            # 鎖定的套件版本
└── docs/              # 主題規劃與公開筆記
```
