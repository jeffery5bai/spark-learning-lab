from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

C = F.col

spark: SparkSession = (
    SparkSession.builder
    .appName("ep07-shuffle-aqe")
    .master("local[2]")
    .config("spark.ui.port", "4040")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.shuffle.partitions", "6")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()
)


def show_physical_plan(label: str, dataframe: DataFrame) -> None:
    print(f"\n{label}")
    print("   Physical Plan：請特別找 Exchange 與 Sort")
    dataframe.explain(mode="simple")


def run_action(dataframe: DataFrame) -> None:
    rows = dataframe.collect()
    print(f"   collect() 收到 {len(rows)} 筆結果")


try:
    print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
    print(f"Application ID: {spark.sparkContext.applicationId}")
    print(f"Spark master: {spark.sparkContext.master}")
    print("Initial AQE enabled: false")
    print("Initial shuffle partitions: 6")

    print("\n1. 建立兩個 partition 的訂單與客戶資料")
    orders = (
        spark.range(1, 13, numPartitions=2)
        .withColumn("customer_id", C("id") % 3)
        .withColumn("event_time", C("id"))
        .withColumn("amount", C("id") * F.lit(25))
    )
    customers = (
        spark.range(0, 3, numPartitions=2)
        .withColumn("tier", F.when(C("id") == 0, "gold").otherwise("standard"))
        .withColumnRenamed("id", "customer_id")
    )
    print(f"   orders partitions: {orders.rdd.getNumPartitions()}")
    print(f"   customers partitions: {customers.rdd.getNumPartitions()}")

    print("\n2. 無 key 的 repartition 再 groupBy：資料要重分配兩次")
    round_robin_grouped = (
        orders.repartition(6)
        .groupBy("customer_id")
        .agg(F.sum("amount").alias("total_amount"))
    )
    show_physical_plan(
        "   round-robin repartition(6) → groupBy(customer_id)", round_robin_grouped
    )
    run_action(round_robin_grouped)
    input(
        "\n請到 Spark UI 查看這個 Job：應能找到 RoundRobin 與依 customer_id 的兩個 Exchange。"
        "完成後按 Enter 繼續。"
    )

    print("\n3. 依 key 的 repartition 再 groupBy：同一份分布可以沿用")
    keyed_grouped = (
        orders.repartition(6, "customer_id")
        .groupBy("customer_id")
        .agg(F.sum("amount").alias("total_amount"))
    )
    show_physical_plan(
        "   repartition(6, customer_id) → groupBy(customer_id)", keyed_grouped
    )
    run_action(keyed_grouped)
    input(
        "\n請比較上一個 Job：這次依 customer_id 的 Exchange 有幾個？"
        "完成後按 Enter 繼續。"
    )

    print("\n4. 依 key 的 repartition 再做 window：沿用 shuffle，但仍要排序")
    customer_window = Window.partitionBy("customer_id").orderBy("event_time")
    keyed_window = orders.repartition(6, "customer_id").withColumn(
        "row_num", F.row_number().over(customer_window)
    )
    show_physical_plan(
        "   repartition(6, customer_id) → Window(partitionBy/orderBy)", keyed_window
    )
    run_action(keyed_window)
    input(
        "\n請找這個 plan 中的 Exchange 與 Sort：Window 多了什麼成本？"
        "完成後按 Enter 繼續。"
    )

    print("\n5. 大型 inner join 的同 key repartition：兩側各做一次必要的 shuffle")
    keyed_join = orders.repartition(6, "customer_id").join(
        customers.repartition(6, "customer_id"),
        on="customer_id",
        how="inner",
    )
    show_physical_plan("   keyed repartition on both sides → inner join", keyed_join)
    run_action(keyed_join)
    input(
        "\n請找 join 左右兩側的 Exchange：它們在進入 SortMergeJoin 前各自準備資料。"
        "完成後按 Enter 繼續。"
    )

    print("\n6. 開啟 AQE：初始 6 個 shuffle partitions 可能被實際資料量調整")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    adaptive_grouped = orders.groupBy("customer_id").agg(
        F.sum("amount").alias("total_amount")
    )
    show_physical_plan("   AQE enabled 的 groupBy（action 前）", adaptive_grouped)
    run_action(adaptive_grouped)
    show_physical_plan("   AQE enabled 的 groupBy（action 後）", adaptive_grouped)
    input(
        "\n請到 Spark UI 找 AQEShuffleRead 或 coalesced 的線索，並比較實際 task 數。"
        "完成後按 Enter 結束 application。"
    )
finally:
    spark.stop()
