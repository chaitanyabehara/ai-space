task_skew = (
    task_accumulators
    .groupBy("stage_id")
    .agg(
        spark_max("output_rows").alias("max_task_output"),
        avg("output_rows").alias("avg_task_output")
    )
    .withColumn(
        "skew_ratio",
        col("max_task_output") / col("avg_task_output")
    )
    .orderBy(col("skew_ratio").desc())
)

task_skew.show()


from pyspark.sql.functions import collect_list
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf

def gini(values):
    values = sorted([v for v in values if v is not None])
    n = len(values)
    if n == 0:
        return 0.0
    mean = sum(values) / n
    diff_sum = sum(abs(x - y) for x in values for y in values)
    return diff_sum / (2 * n * n * mean)

gini_udf = udf(gini, DoubleType())

stage_gini = (
    task_accumulators
    .groupBy("stage_id")
    .agg(collect_list("output_rows").alias("values"))
    .withColumn("gini_output", gini_udf(col("values")))
)

stage_gini.show()



runtime_vs_output = (
    task_runtime_df
    .join(task_accumulators, ["stage_id", "task_id"])
)

runtime_vs_output.stat.corr("output_rows", "task_runtime")


stage_amplification = (
    stage_metrics
    .withColumn(
        "row_amplification",
        col("total_output_rows") / col("total_input_rows")
    )
)
