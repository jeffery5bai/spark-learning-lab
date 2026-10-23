# Ep28｜只剩一個 task 還在跑，要不要多派一個幫手？聊聊 Spark speculation

有時候一個 Stage 已經完成大部分工作，只剩一個 task 遲遲不結束，整個 job 就只能等它。Ep23 看過其中一種原因：資料集中在少數 key，讓某個 task 比其他人多做很多工作。但如果資料量差不多，慢的是 Executor 當下的狀態，例如遇上 GC pause、CPU 競爭或節點抖動，換個地方重做一份，有沒有機會早點完成？

Spark 的 speculation 就是提供這個機會：原本的 task 繼續跑，同時在其他可用資源上啟動一份相同 partition 的備份。誰先成功，就採用誰的結果，另一份再取消。

多派一個幫手會花資源，也未必更快。這篇要談的是 Spark 怎麼決定值得重做、有哪些設定，以及工作現場該如何判斷要不要開。

## 讀完這篇後你會更了解……

- speculation 如何縮短少數慢 task 造成的尾端等待。
- 完成比例、中位耗時、最低執行時間與處理效率如何影響觸發。
- 暫時性的慢 task，和每次重跑都一樣慢的工作，有什麼差別。
- 從預設值開始，如何選擇較保守的設定與評估方式。
- 多跑一份的資源成本，以及寫入外部系統時要注意的重複執行。

## 這一課在系列中的位置：工作切好後，還是有人落後怎麼辦？

~~~bash
Ep23：資料分布不均，讓少數 task 扛太多工作
        ↓
Ep27：調整 shuffle 工作的切分與合併
        ↓
Ep28：工作量相近，卻有少數 task 暫時落後
speculation 能不能讓另一份 attempt 先完成？
        ↓
Ep29：小表要多小，才適合送到每個 Executor？
~~~

本篇聚焦 Spark 3.4.4 的 task 排程。speculation 能用在 RDD，也能作用於 SQL／DataFrame 工作所產生的 tasks；它和 Ep27 的 AQE 分別處理不同層次的問題。

## 原本的 task 繼續跑，備份從頭做同一份工作

speculation 的備份稱為 speculative attempt。它會重新計算同一個 partition，不會接手原本 task 的進度，也不會把剩下的資料切一半分工。

~~~bash
一個 Stage 的大部分 tasks 已完成
        ↓
Driver 發現某個 task 明顯落後，符合推測執行條件
        ↓
找到合適的空閒 task slot，啟動相同 partition 的備份
        ↓
原始 attempt 與備份同時執行
        ↓
先成功的結果被採用，另一個 attempt 收到取消要求
~~~

這和 task failure 後的 retry 有差別：retry 是失敗後再試，speculation 則是在原始 task 還沒失敗時，就先多跑一份。備份仍需要 CPU、memory、資料讀取與可能的 shuffle I/O；如果 cluster 已經很忙，備份可能得等待可用 slot。

因此，要問的是：**換一個地方重做，是否有機會比原本那份更早完成？** 如果慢的原因跟著資料走，例如同一個 hot key 讓 partition 特別大，備份也會遇到相同工作量。

## Spark 怎麼判斷「這個 task 慢得值得多跑一份」？

先看一般的判斷路徑。Spark 需要足夠多已完成的 tasks，才能拿它們的耗時當參考；接著用已完成 task 的中位耗時乘上一個倍數，並和最低執行時間取較大的值。

~~~bash
時間門檻 = max（已完成 tasks 的中位耗時 × multiplier，minTaskRuntime）
~~~

假設一個 Stage 有 100 個 tasks，設定使用預設的完成比例 `0.75`、倍數 `1.5`，已完成 tasks 的中位耗時為 10 秒。當至少 75 個 tasks 完成後，仍在跑的 task 超過約 15 秒，就可能進入下一步判斷。

這裡的「可能」很重要。Spark 3.4.4 預設還會檢查 task 的處理效率：若 metrics 顯示它處理資料的速率偏低，或它已經跑到更長的時間門檻，才會將它列為可推測執行的候選。列為候選後，也還要等排程器找到可用資源。

換句話說，完成比例決定「何時有足夠樣本」，耗時倍數決定「慢多少才值得懷疑」，最低時間則避免剛啟動的小 task 太早被複製。

## 設定先看預設值，再決定改哪個

以下預設值以 Apache Spark 3.4.4 為準。雲端平台與其他版本可能有不同設定，實際使用前仍要確認 application 的有效值。speculation 屬於排程器設定，應在 `spark-submit` 或 SparkContext 建立前配置。

| 設定 | 預設值 | 白話來說，以及調整方向 |
| --- | --- | --- |
| `spark.speculation` | `false` | 是否允許啟動備份。適合先針對有尾端等待問題的 application 試開。 |
| `spark.speculation.interval` | `100ms` | Driver 多久檢查一次。縮短間隔只會更頻繁檢查，仍需滿足其他條件；通常先保留預設。 |
| `spark.speculation.quantile` | `0.75` | Stage 至少完成多少比例後，才啟用一般的慢 task 判斷。提高會等更多 tasks 完成；降低會更早判斷，但參考樣本也更少。 |
| `spark.speculation.multiplier` | `1.5` | 中位耗時乘上的倍數。調高較保守，調低則更容易把正常差異也當成落後。 |
| `spark.speculation.minTaskRuntime` | `100ms` | task 至少跑多久，才有機會成為候選。工作本來很短時，可用較高下限減少過早複製。 |

Spark 3.4 起另有以下效率判斷，這次實驗保留預設：

| 設定 | 預設值 | 它影響的判斷 |
| --- | --- | --- |
| `spark.speculation.efficiency.enabled` | `true` | 用 task metrics 的資料處理速率與耗時，進一步篩選需要備份的工作。 |
| `spark.speculation.efficiency.processRateMultiplier` | `0.75` | 將 task 處理速率和已成功 tasks 的平均速率比較；低於平均的 75% 是效率偏低的判斷條件。調高會讓更多 tasks 可能被視為低效率。 |
| `spark.speculation.efficiency.longRunTaskFactor` | `2` | task 跑超過前述時間門檻的兩倍後，即使處理速率沒有偏低，也可能成為候選，避免漏掉無法由速率解釋的慢工作。 |

這套速率判斷並沒有讓 Spark 學會辨認 data skew。它依據可取得的 metrics 做決策，遇到資料量差很多、外部服務變慢或自訂 Python 工作，仍需要人回頭檢查原因。tasks 很少的 Stage 也未必容易累積足夠的完成樣本。

## 實務上先怎麼設？保留門檻，觀察備份到底有沒有幫上忙

我會先確認三件事：少數 task 是否反覆拖住 Stage、它們的資料量是否和其他 task 接近，以及 cluster 是否還有資源能跑備份。若是所有 tasks 都慢，或每次都是同一個大 partition 落後，應先處理整體瓶頸與資料分布。

確認值得嘗試後，可以只開 `spark.speculation=true`，其餘先保留預設：`quantile=0.75`、`multiplier=1.5`、`interval=100ms`，效率判斷也保持開啟。先跑有代表性的 workload，比較端到端 latency、Stage 尾端時間、備份成功比例與額外資源消耗。

如果正常 tasks 通常跑數十秒到數分鐘，可以再依其正常耗時評估最低執行時間。例如先測試 `minTaskRuntime=10s`，讓剛啟動的短工作不會太早被複製；這是測試起點，仍要和中位耗時一起看。若備份常常被取消、卻很少先完成，可先提高 `multiplier`，例如由 `1.5` 試到 `2`，或提高最低執行時間，再比較資源成本。

反過來，如果已確認是暫時性落後，備份卻總是啟動太晚，才考慮逐步降低倍數或完成比例。不要同時把所有門檻都降低，否則很難知道哪個改動有效，也容易增加不必要的重複工作。

寫入外部系統時還有一項前提：task 可能被重複執行。若在 `foreachPartition` 裡直接呼叫 API、寄送訊息或寫入資料庫，需要以唯一鍵、去重或冪等操作處理重複請求。Spark 採用一份成功結果，並不會替已發生的外部副作用自動復原。

> *值得開 speculation 的情況，是另一份 task 有機會繞過暫時的慢；評估成效時，還要一起算進多跑那份工作的成本。*

## 同樣開 speculation，兩種慢 task 會有不同結果

這次環境有兩個 Standalone Executors，各有 2 cores、512 MiB heap，合計四個 task slots。每個 case 都有八個 partitions，其中七個 task 等待約 1 秒，partition 0 則依案例加入不同延遲。

| 案例 | 原始 attempt 0 | 備份 attempt 1 | 要驗證的問題 |
| --- | --- | --- | --- |
| 暫時性落後 | 等待 12 秒 | 等待 0.2 秒 | 換一份 attempt 後，能否避開原本的延遲？ |
| 固定慢工作 | 等待 12 秒 | 同樣等待 12 秒 | 重做仍要付相同成本時，備份是否幫得上忙？ |

程式用 `TaskContext.attemptNumber()` 區分 attempts，再用 `sleep` 控制延遲。第二個案例用來呈現「換地方仍然慢」的結果，實際並沒有製造 hot key 或大量資料；真實 skew 的資料證據可回看 Ep23。

兩個案例使用相同設定：開啟 speculation，保留 `interval=100ms`、`quantile=0.75`，將 `multiplier` 由預設 `1.5` 改成 `1.0`、`minTaskRuntime` 由預設 `100ms` 改成 `2s`。這組值用來觀察短時間的受控工作，實務設定仍需依前一節的判斷方式評估。

![實驗設定與完成結果：暫時性落後由 attempt 1 勝出，固定慢工作由 attempt 0 勝出](https://raw.githubusercontent.com/jeffery5bai/spark-learning-lab/main/episodes/ep28-speculation/screenshots/speculation-settings-and-results.png)

結果輸出中的 `partition_0_winning_attempt` 分別為 `1`、`0`，已經能看出兩者差異；Executor 的實際位置則由下方 Tasks table 確認。

## 暫時性落後：備份先完成，原本那份收到取消要求

Stage 0 中，partition 0 的原始 attempt 在 Executor 0 啟動。其他 tasks 陸續完成後，Spark 將它列為可推測執行的候選，並在 Executor 1 啟動 attempt 1。這份備份約 222 ms 就成功，於是原始 attempt 被取消。

![Stage 0 的 Index 0 有兩個 attempts：Executor 0 的 attempt 0 被取消，Executor 1 的 attempt 1 約 0.2 秒完成](https://raw.githubusercontent.com/jeffery5bai/spark-learning-lab/main/episodes/ep28-speculation/screenshots/temporary-straggler-task-attempts.png)

Speculation Summary 顯示一份 speculative attempt，且 Complete 為 1。八個 partitions 最後出現九筆 task attempts，正是因為 partition 0 被多跑了一份；資料仍只有八份結果。

![Stage 0 timeline：原始 attempt 與備份有重疊區間，Speculation Summary 顯示備份成功](https://raw.githubusercontent.com/jeffery5bai/spark-learning-lab/main/episodes/ep28-speculation/screenshots/temporary-straggler-timeline.png)

timeline 還有一個細節：收到取消要求，不代表執行中的程式立即停止。這次被取消的 Python task 延後才回報結束，所以它的 bar 可能比 Job 完成時間更長。取消延遲也會占用資源，不能只看備份成功，就認為另一份工作立刻沒有成本。

## 固定慢工作：備份也慢，原本那份反而先完成

Stage 1 同樣啟動了 partition 0 的 attempt 1，但這次每個 attempt 都要等待 12 秒。原始 attempt 先開始，也先完成，備份最後被取消；Speculation Summary 的 Complete 為 0、Killed 為 1。

![Stage 1 的 Index 0：原始 attempt 0 成功，備份 attempt 1 被取消，兩份 attempt 的 duration 都約 12 秒](https://raw.githubusercontent.com/jeffery5bai/spark-learning-lab/main/episodes/ep28-speculation/screenshots/persistent-slow-task-attempts.png)

timeline 上可以看見兩條長時間重疊的工作。Spark 確實多派了一個幫手，但幫手也得從頭做同樣慢的事，因此沒有搶先完成。

![Stage 1 timeline：原始 attempt 和備份都長時間執行，備份未能縮短尾端等待](https://raw.githubusercontent.com/jeffery5bai/spark-learning-lab/main/episodes/ep28-speculation/screenshots/persistent-slow-task-timeline.png)

Jobs 頁顯示兩個案例都完成八個 partitions，且各有一個被取消的 attempt。第一個 Job 約 6 秒，第二個約 12 秒；程式量到的 action 時間則約為 6.8 秒與 12.1 秒。

![Jobs 總覽：兩個案例都成功完成，暫時性落後約 6 秒、固定慢工作約 12 秒](https://raw.githubusercontent.com/jeffery5bai/spark-learning-lab/main/episodes/ep28-speculation/screenshots/jobs-overview.png)

這兩個數字來自不同的延遲條件，第一個案例還包含 Executor 與 Python worker 啟動成本。它們能說明備份是否先完成，不能當成同一份 workload 開關 speculation 的加速比例。

## 回到工作現場：哪種慢，值得多跑一份？

| 現象 | 較合理的處理方向 |
| --- | --- |
| task 資料量相近，但偶爾換不同 task／Executor 落後 | 檢查節點、GC 與資源競爭；有空閒資源時，可測試 speculation 是否縮短尾端等待。 |
| 每次都由相同的大 partition 拖住進度 | 檢查 key 分布、partition 大小與資料處理方式，優先改善工作量不均。 |
| 備份常常啟動，但多數被取消，額外 CPU／I/O 上升 | 提高觸發門檻，或關閉 speculation，評估節省的時間是否值得資源支出。 |
| 所有 tasks 都很慢，或共同依賴的外部服務已飽和 | 處理共同瓶頸；多一份請求可能讓競爭更嚴重。 |

## 這次我真正學到的是什麼？

這次我學到，speculation 提供的是「換一份 attempt 試試看」的機會。原始工作繼續執行，備份從頭計算同一個 partition，Spark 採用先成功的結果。當延遲只發生在原始 attempt，備份確實能搶先完成；如果慢的是那份工作本身，備份也得承受相同成本。

我也更清楚調整門檻的意義：先有足夠完成樣本，再用中位耗時、最低執行時間與效率判斷篩選候選，最後還需要可用資源來執行。實務上，我會把 Stage 尾端省下的時間、備份勝出比例與多花的資源一起比較，再決定是否保留這項設定。

下一篇回到 join：如果一張表看起來很小，Spark 為什麼仍選擇 shuffle？判斷是否該 broadcast，除了資料筆數，還需要看哪些大小、記憶體與等待時間？

---

## 完整程式碼與參考資料

🚀 GitHub：\
[Ep28｜只剩一個 task 還在跑，要不要多派一個幫手？聊聊 Spark speculation](https://github.com/jeffery5bai/spark-learning-lab/tree/main/episodes/ep28-speculation)

🚀 spark-learning-lab 系列文章與實作：\
[https://github.com/jeffery5bai/spark-learning-lab](https://github.com/jeffery5bai/spark-learning-lab)

Apache Spark 3.4.4 — Configuration（speculation 與效率判斷設定）：\
[https://archive.apache.org/dist/spark/docs/3.4.4/configuration.html](https://archive.apache.org/dist/spark/docs/3.4.4/configuration.html)
