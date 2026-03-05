import { useState } from "react";

const questions = [
  {
    id: 1,
    category: "Core Concepts",
    difficulty: "Medium",
    question: "What is the difference between RDD, DataFrame, and Dataset in Spark?",
    answer: `**RDD (Resilient Distributed Dataset):**
- Low-level API, immutable distributed collection of objects
- No schema enforcement, operates on JVM objects
- Less optimized — no Catalyst optimizer
- Use when: you need fine-grained control or working with unstructured data

**DataFrame:**
- Distributed collection of data organized into named columns (like a SQL table)
- Schema-aware, uses Catalyst optimizer → faster than RDDs
- Available in Python, Scala, Java, R
- Use when: structured/semi-structured data processing (most common in PySpark)

**Dataset:**
- Type-safe version of DataFrame (compile-time type checking)
- Only available in Scala/Java (not Python)
- Combines benefits of RDD (type safety) + DataFrame (optimizer)

**Key insight for interviews:** In PySpark, you'll primarily work with DataFrames. Datasets aren't available in Python due to dynamic typing. Always prefer DataFrames over RDDs unless you have a specific reason.`,
    followUp: "When would you still choose RDD over DataFrame in a production pipeline?"
  },
  {
    id: 2,
    category: "Core Concepts",
    difficulty: "Easy",
    question: "Explain lazy evaluation in Spark. Why is it important?",
    answer: `**Lazy Evaluation** means Spark does NOT execute transformations immediately. It builds a logical plan (DAG - Directed Acyclic Graph) and only executes when an **action** is called.

**Transformations (lazy):** \`filter()\`, \`map()\`, \`select()\`, \`groupBy()\`, \`join()\`
**Actions (triggers execution):** \`collect()\`, \`count()\`, \`show()\`, \`write()\`, \`take()\`

**Why it matters:**
1. **Optimization** — Catalyst can reorder, combine, or eliminate operations before running
2. **Efficiency** — Avoids unnecessary computation; only processes what's needed
3. **Fault tolerance** — DAG can be recomputed from lineage if a partition fails

**Example:**
\`\`\`python
df = spark.read.parquet("s3://data/events")
df_filtered = df.filter(df.country == "IN")  # Nothing runs yet
df_agg = df_filtered.groupBy("user_id").count()  # Still nothing
df_agg.show()  # NOW Spark executes the full plan
\`\`\`

**Interview tip:** Mention that Spark may push filter predicates before joins (predicate pushdown) thanks to lazy evaluation + Catalyst.`,
    followUp: "What's the difference between a narrow and wide transformation?"
  },
  {
    id: 3,
    category: "Performance",
    difficulty: "Hard",
    question: "What is data skew and how do you handle it in PySpark?",
    answer: `**Data Skew** occurs when data is unevenly distributed across partitions — some partitions have much more data than others, causing a few tasks to take much longer (stragglers).

**Symptoms:**
- Most tasks complete in seconds, 1-2 tasks take minutes
- Spark UI shows very uneven task duration in a stage
- OOM errors on specific executors

**Common Causes:**
- Skewed join keys (e.g., NULL values, very popular user IDs)
- GROUP BY on low-cardinality columns

**Solutions:**

**1. Salting (most common):**
\`\`\`python
import random
# Add random salt to distribute skewed keys
df_salted = df.withColumn("salt", (rand() * 10).cast("int"))
df_salted = df_salted.withColumn("salted_key", concat(col("user_id"), lit("_"), col("salt")))
\`\`\`

**2. Broadcast Join (for skewed small tables):**
\`\`\`python
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")
\`\`\`

**3. Skew Hint (Spark 3.0+, AQE):**
\`\`\`python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
\`\`\`

**4. Repartition on composite key:**
\`\`\`python
df.repartition(200, "country", "date")
\`\`\`

**Interview tip:** Always mention Adaptive Query Execution (AQE) in Spark 3.x — it handles skew automatically at runtime.`,
    followUp: "How does Adaptive Query Execution (AQE) in Spark 3.x help with skew?"
  },
  {
    id: 4,
    category: "Performance",
    difficulty: "Hard",
    question: "Explain the difference between repartition() and coalesce(). When do you use each?",
    answer: `**repartition(n):**
- **Full shuffle** — redistributes data across ALL partitions
- Can increase OR decrease partition count
- Results in evenly distributed partitions
- Expensive (network I/O)
- Use when: increasing partitions, or you need balanced partitions before a heavy operation

**coalesce(n):**
- **No full shuffle** — merges existing partitions (narrow transformation)
- Can ONLY decrease partition count
- May result in uneven partitions
- Much cheaper than repartition
- Use when: reducing partitions before writing output to avoid many small files

**Example:**
\`\`\`python
# Before a heavy join — use repartition for balance
df = df.repartition(200, "join_key")

# Before writing to storage — use coalesce to reduce files
df.coalesce(10).write.parquet("s3://output/")
\`\`\`

**Key rule of thumb:**
- Reading data → repartition for processing balance
- Writing data → coalesce to reduce output file count
- coalesce(1) is common for writing a single output file (but avoid on large datasets — OOM risk!)

**Gotcha:** \`coalesce\` can push upstream transformations into fewer partitions even before writing, reducing parallelism. If you need to maintain parallelism, \`repartition\` is safer.`,
    followUp: "What is the 'small files problem' and how does coalesce help?"
  },
  {
    id: 5,
    category: "Performance",
    difficulty: "Medium",
    question: "What is caching/persistence in Spark? What storage levels exist?",
    answer: `**Caching** stores intermediate results in memory (or disk) so they don't get recomputed when reused.

**When to use:**
- DataFrame used multiple times in your code
- Iterative algorithms (ML training)
- After expensive operations (joins, aggregations)

**Methods:**
\`\`\`python
df.cache()           # Shorthand for MEMORY_AND_DISK
df.persist()         # Default: MEMORY_AND_DISK
df.persist(StorageLevel.MEMORY_ONLY)
df.unpersist()       # Always unpersist when done!
\`\`\`

**Storage Levels:**

| Level | Memory | Disk | Serialized | Replicated |
|-------|--------|------|------------|------------|
| MEMORY_ONLY | ✅ | ❌ | ❌ | ❌ |
| MEMORY_AND_DISK | ✅ | ✅ (overflow) | ❌ | ❌ |
| MEMORY_ONLY_SER | ✅ | ❌ | ✅ | ❌ |
| DISK_ONLY | ❌ | ✅ | ✅ | ❌ |
| MEMORY_AND_DISK_2 | ✅ | ✅ | ❌ | ✅ (2x) |

**Best practices:**
- Use \`MEMORY_AND_DISK\` (default) for production — safe fallback to disk
- Use \`MEMORY_ONLY\` if data fits in memory and recompute is cheap
- **Always \`unpersist()\`** when done — prevents memory leaks in long jobs
- Don't cache DataFrames you only use once`,
    followUp: "How do you decide whether to cache a DataFrame or not?"
  },
  {
    id: 6,
    category: "Joins",
    difficulty: "Hard",
    question: "Explain the different join strategies in Spark (Broadcast, Sort-Merge, Shuffle Hash).",
    answer: `**1. Broadcast Hash Join (BHJ):**
- Broadcasts the smaller table to ALL executors
- No shuffle of the large table
- Fastest join strategy
- Threshold: \`spark.sql.autoBroadcastJoinThreshold\` (default 10MB)
\`\`\`python
result = large_df.join(broadcast(small_df), "id")
\`\`\`
- **Use when:** one table is small enough to fit in executor memory

**2. Sort-Merge Join (SMJ):**
- Default for large-large joins
- Both tables shuffled by join key, then sorted, then merged
- Memory efficient, handles large datasets
- Requires sortable keys
\`\`\`python
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
\`\`\`
- **Use when:** both tables are large

**3. Shuffle Hash Join (SHJ):**
- Shuffles both tables by join key, builds hash table from smaller side
- Doesn't require sorting
- More memory intensive than SMJ
- **Use when:** join keys aren't sortable, or smaller side fits in memory after shuffle

**Spark chooses automatically, but you can hint:**
\`\`\`python
df1.hint("broadcast").join(df2, "key")
df1.hint("merge").join(df2, "key")
df1.hint("shuffle_hash").join(df2, "key")
\`\`\`

**Interview tip:** Check the query plan with \`df.explain()\` to verify which join strategy Spark chose.`,
    followUp: "What happens when you broadcast a table that's too large for executor memory?"
  },
  {
    id: 7,
    category: "Architecture",
    difficulty: "Medium",
    question: "Explain Spark's execution model: Driver, Executors, Tasks, Stages, and Jobs.",
    answer: `**Hierarchy: Job → Stages → Tasks**

**Driver:**
- The main process that runs your PySpark application
- Creates SparkContext/SparkSession
- Builds the DAG (logical + physical plan)
- Schedules tasks to executors
- Single point of failure — keep it lightweight

**Executor:**
- JVM processes that run on worker nodes
- Execute tasks, store cached data
- Report results back to driver
- Each executor has multiple cores (task slots)

**Job:**
- Triggered by each **action** (\`count()\`, \`show()\`, \`write()\`)
- One application can have many jobs

**Stage:**
- A set of tasks that can run without a shuffle
- Stage boundary = shuffle boundary
- Types: ShuffleMapStage, ResultStage

**Task:**
- Smallest unit of execution
- One task per partition
- Runs on one executor core

**Example flow:**
\`\`\`
df.groupBy("country").count().show()
  ↓
Job created
  ↓ 
Stage 1: Read + partial aggregation (map-side)
  ↓ [SHUFFLE]
Stage 2: Final aggregation + show (reduce-side)
\`\`\`

**Interview tip:** "Wide transformations (groupBy, join, distinct) cause stage boundaries because they require shuffles across executors."`,
    followUp: "How do you tune the number of shuffle partitions?"
  },
  {
    id: 8,
    category: "Architecture",
    difficulty: "Medium",
    question: "What is a shuffle in Spark and why is it expensive?",
    answer: `**Shuffle** is the process of redistributing data across partitions/executors — triggered by wide transformations like \`groupBy\`, \`join\`, \`distinct\`, \`repartition\`.

**Why it's expensive:**
1. **Disk I/O** — shuffle data is written to disk before being transferred
2. **Network I/O** — data moves across the network between executors
3. **Serialization/Deserialization** — data must be serialized to transfer
4. **GC pressure** — large objects in memory

**Key config:**
\`\`\`python
# Number of shuffle partitions (default 200 — often needs tuning)
spark.conf.set("spark.sql.shuffle.partitions", "400")

# Rule of thumb: target ~128MB-200MB per partition
# partitions = total_data_size / target_partition_size
\`\`\`

**How to minimize shuffles:**
- Use \`broadcast\` joins to avoid shuffling large tables
- Filter/project data BEFORE joining (reduce data size)
- Partition your source data on join keys (avoid re-shuffle)
- Use \`cogroup\` instead of multiple joins where possible
- Enable AQE to dynamically tune shuffle partitions

**Shuffle in Spark UI:**
- Look at "Shuffle Read/Write" bytes in Stage metrics
- High shuffle = potential bottleneck

**Interview tip:** "One of the most common performance issues I debug is shuffle spill — when shuffle data exceeds memory and spills to disk. Tuning \`spark.sql.shuffle.partitions\` and executor memory is key."`,
    followUp: "What is shuffle spill and how do you diagnose it in Spark UI?"
  },
  {
    id: 9,
    category: "PySpark API",
    difficulty: "Medium",
    question: "What are Window functions in PySpark? Give a practical example.",
    answer: `**Window functions** perform calculations across a set of rows related to the current row — without collapsing rows (unlike aggregations).

**Core components:**
\`\`\`python
from pyspark.sql import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead, sum, avg

# Define window spec
windowSpec = Window.partitionBy("department").orderBy(col("salary").desc())
\`\`\`

**Common functions:**

**Ranking:**
\`\`\`python
df.withColumn("rank", rank().over(windowSpec))
df.withColumn("dense_rank", dense_rank().over(windowSpec))
df.withColumn("row_num", row_number().over(windowSpec))
\`\`\`

**Running totals / Moving averages:**
\`\`\`python
windowSpec2 = Window.partitionBy("dept").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
df.withColumn("running_total", sum("sales").over(windowSpec2))
\`\`\`

**Lead/Lag (time-series analysis):**
\`\`\`python
df.withColumn("prev_day_sales", lag("sales", 1).over(windowSpec))
df.withColumn("next_day_sales", lead("sales", 1).over(windowSpec))
\`\`\`

**Real use case — Top N per group:**
\`\`\`python
# Top 3 highest paid employees per department
w = Window.partitionBy("department").orderBy(col("salary").desc())
df.withColumn("rank", dense_rank().over(w)).filter(col("rank") <= 3)
\`\`\`

**Interview tip:** Window functions trigger a shuffle (partitionBy creates a shuffle). Be mindful of cardinality in partitionBy columns.`,
    followUp: "What's the difference between rank(), dense_rank(), and row_number()?"
  },
  {
    id: 10,
    category: "PySpark API",
    difficulty: "Medium",
    question: "How do you handle NULL values in PySpark?",
    answer: `**Checking for NULLs:**
\`\`\`python
df.filter(col("name").isNull())
df.filter(col("name").isNotNull())
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])  # NULL count per column
\`\`\`

**Dropping NULLs:**
\`\`\`python
df.dropna()                          # Drop rows with ANY null
df.dropna(how="all")                 # Drop rows where ALL columns are null
df.dropna(subset=["name", "email"])  # Drop rows with null in specific columns
df.dropna(thresh=3)                  # Drop rows with fewer than 3 non-null values
\`\`\`

**Filling NULLs:**
\`\`\`python
df.fillna(0)                                    # Fill all numeric nulls with 0
df.fillna({"age": 0, "name": "Unknown"})        # Column-specific fill
df.fillna(df.agg(avg("salary")).first()[0])     # Fill with mean
\`\`\`

**NULL-safe equality:**
\`\`\`python
# Regular == treats NULL != NULL
# Use eqNullSafe for NULL-safe comparison
df.filter(col("a").eqNullSafe(col("b")))
\`\`\`

**NULL in aggregations:**
- Most aggregate functions (\`sum\`, \`avg\`, \`count\`) **ignore NULLs**
- \`count(*)\` counts all rows; \`count(col)\` skips NULLs
- Use \`coalesce()\` to substitute a default value

\`\`\`python
df.withColumn("revenue", coalesce(col("revenue"), lit(0)))
\`\`\`

**Interview tip:** Know how NULLs behave in joins — NULL keys don't match in inner joins but appear in outer joins with nullified columns.`,
    followUp: "How does NULL propagate in expressions like col('a') + col('b') when one is NULL?"
  },
  {
    id: 11,
    category: "Optimization",
    difficulty: "Hard",
    question: "What is Predicate Pushdown and Partition Pruning in Spark?",
    answer: `**Predicate Pushdown:**
Spark pushes filter conditions as close to the data source as possible — reducing the amount of data read.

\`\`\`python
df = spark.read.parquet("s3://data/events")
df.filter(col("country") == "IN").select("user_id", "event_type")
# Spark pushes filter to Parquet reader → reads only matching row groups
\`\`\`

Works with: Parquet, ORC, Delta Lake, JDBC sources
Verify with: \`df.explain()\` — look for \`PushedFilters\`

**Partition Pruning:**
When data is partitioned on disk (Hive-style partitioning), Spark skips reading irrelevant partitions entirely.

\`\`\`
s3://data/events/
  ├── country=IN/
  ├── country=US/
  └── country=UK/
\`\`\`

\`\`\`python
df.filter(col("country") == "IN")
# Spark only reads s3://data/events/country=IN/ — skips all other partitions
\`\`\`

**Best practices:**
- Partition on columns used frequently in filters (e.g., date, country, region)
- Don't over-partition — avoid high-cardinality partition columns (like user_id)
- Use Parquet/ORC (columnar formats) for maximum pushdown benefit
- Delta Lake Z-Ordering also helps with data skipping

\`\`\`python
# Write with partitioning
df.write.partitionBy("country", "year").parquet("s3://output/")
\`\`\`

**Interview tip:** Always mention that predicate pushdown + partition pruning is why choosing the right file format and partitioning strategy is critical for pipeline performance.`,
    followUp: "How would you choose partition columns for a large event log table?"
  },
  {
    id: 12,
    category: "Optimization",
    difficulty: "Medium",
    question: "What are UDFs in PySpark and what are their performance implications?",
    answer: `**UDF (User Defined Function):** Custom Python functions registered for use in Spark SQL / DataFrame operations.

\`\`\`python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define and register
def clean_email(email):
    return email.strip().lower() if email else None

clean_email_udf = udf(clean_email, StringType())
df.withColumn("clean_email", clean_email_udf(col("email")))
\`\`\`

**Why UDFs are slow:**
1. **Serialization overhead** — data must leave JVM → Python process → back to JVM (row by row!)
2. **No Catalyst optimization** — Spark treats UDF as a black box
3. **GIL limitations** — Python GIL limits parallelism

**Better alternatives (in order of preference):**

1. **Built-in functions** — Always prefer these
\`\`\`python
from pyspark.sql.functions import lower, trim, regexp_replace
df.withColumn("clean", lower(trim(col("email"))))
\`\`\`

2. **Pandas UDF (Vectorized UDF)** — 10-100x faster than regular UDFs
\`\`\`python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(StringType())
def clean_email_pandas(s: pd.Series) -> pd.Series:
    return s.str.strip().str.lower()

df.withColumn("clean", clean_email_pandas(col("email")))
\`\`\`

3. **Spark SQL expressions** — leverage Catalyst
4. **Regular UDF** — last resort

**Interview tip:** "I always first check if a built-in Spark function can replace a UDF. If I must use a UDF, I use Pandas UDFs with Apache Arrow for vectorized processing."`,
    followUp: "What is Apache Arrow and how does it speed up Pandas UDFs?"
  },
  {
    id: 13,
    category: "Delta Lake / Lakehouse",
    difficulty: "Hard",
    question: "What is Delta Lake and how does it improve on plain Parquet?",
    answer: `**Delta Lake** is an open-source storage layer that brings ACID transactions and data reliability to data lakes.

**Problems with plain Parquet/data lakes:**
- No ACID transactions → partial writes, corrupt state
- No schema enforcement → bad data silently lands
- No deletes/updates → can't comply with GDPR
- No time travel
- Inconsistent reads during writes

**Delta Lake solves all of these:**

**1. ACID Transactions:**
\`\`\`python
df.write.format("delta").save("s3://delta/events")
# Atomic — either fully written or not at all
\`\`\`

**2. Time Travel:**
\`\`\`python
spark.read.format("delta").option("versionAsOf", 5).load("s3://delta/events")
spark.read.format("delta").option("timestampAsOf", "2024-01-01").load(path)
\`\`\`

**3. Schema Evolution & Enforcement:**
\`\`\`python
df.write.format("delta").option("mergeSchema", "true").mode("append").save(path)
\`\`\`

**4. Upserts (MERGE / SCD Type 2):**
\`\`\`python
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, path)
delta_table.alias("target").merge(
    updates.alias("source"), "target.id = source.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
\`\`\`

**5. OPTIMIZE & Z-ORDER (data skipping):**
\`\`\`python
delta_table.optimize().executeZOrderBy("user_id", "date")
\`\`\`

**Interview tip:** Delta Lake is the foundation of the Lakehouse architecture (Databricks). Expect questions on MERGE for SCD Type 2 patterns specifically.`,
    followUp: "How would you implement SCD Type 2 using Delta Lake MERGE?"
  },
  {
    id: 14,
    category: "Streaming",
    difficulty: "Hard",
    question: "How does Spark Structured Streaming work? Explain watermarking.",
    answer: `**Structured Streaming** treats a live data stream as an unbounded table. New data arrives as new rows, and your query runs continuously.

\`\`\`python
# Read from Kafka
stream_df = spark.readStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "broker:9092") \\
    .option("subscribe", "events") \\
    .load()

# Transform
result = stream_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Write (sink)
query = result.writeStream \\
    .format("delta") \\
    .outputMode("append") \\
    .option("checkpointLocation", "s3://checkpoints/") \\
    .start()
\`\`\`

**Output Modes:**
- \`append\` — only new rows added (default, no aggregation or for append-only)
- \`complete\` — full result table each time (aggregations)
- \`update\` — only updated rows (aggregations)

**Watermarking — handling late data:**
\`\`\`python
# Tell Spark: wait up to 10 minutes for late events
result = stream_df \\
    .withWatermark("event_time", "10 minutes") \\
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("user_id")
    ).count()
\`\`\`

**How watermark works:**
- Spark tracks the max event_time seen
- Watermark = max_event_time - delay_threshold
- Events older than watermark are dropped
- Allows Spark to clean up state for old windows

**Triggers:**
\`\`\`python
.trigger(processingTime="30 seconds")  # Micro-batch every 30s
.trigger(once=True)                    # Process available data once
.trigger(continuous="1 second")        # Experimental continuous processing
\`\`\`

**Interview tip:** Always mention checkpointing — it's how Structured Streaming achieves fault tolerance by persisting offsets and state.`,
    followUp: "What is stateful streaming and when would you use mapGroupsWithState?"
  },
  {
    id: 15,
    category: "Real-world",
    difficulty: "Hard",
    question: "Walk me through how you'd debug a slow Spark job in production.",
    answer: `**Systematic debugging approach:**

**Step 1: Check Spark UI**
- **Jobs tab** → identify slow stages (sort by duration)
- **Stages tab** → look at task distribution (min/median/max task time)
- **Executors tab** → check GC time, shuffle read/write, spill

**Red flags in UI:**
- GC time > 10% of task time → memory pressure, tune executor memory
- Shuffle spill to disk → increase \`spark.executor.memory\` or reduce shuffle partitions
- Straggler tasks → data skew
- Very few tasks → too few partitions

**Step 2: Identify the bottleneck**

\`\`\`python
df.explain(True)  # See physical plan + statistics
\`\`\`

Common issues:
- **Data skew** → salt keys, enable AQE
- **Too many small files** → coalesce reads, compact table
- **No partition pruning** → check filter columns match partition columns
- **Missing broadcast** → small table not being broadcast
- **Wrong shuffle partitions** → tune \`spark.sql.shuffle.partitions\`

**Step 3: Common fixes**

\`\`\`python
# AQE (Spark 3.x) - handles many issues automatically
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Tune shuffle partitions based on data size
spark.conf.set("spark.sql.shuffle.partitions", "400")  # ~200MB per partition

# Increase executor memory if spilling
# --executor-memory 8g --executor-cores 4
\`\`\`

**Step 4: Check upstream causes**
- Data volume change? (sudden increase)
- Schema change? (new nulls, type changes)
- Source system slowness? (JDBC throttling)

**Interview tip:** Walk through a real example: "I had a groupBy job taking 45 minutes. Spark UI showed 2 tasks taking 40 minutes. I identified a NULL user_id causing skew, filtered out NULLs upstream, and the job ran in 3 minutes."`,
    followUp: "How do you monitor Spark jobs in a production environment?"
  },
];

const categories = ["All", ...new Set(questions.map(q => q.category))];
const difficulties = ["All", "Easy", "Medium", "Hard"];

const difficultyColor = {
  Easy: { bg: "#d4edda", text: "#155724", border: "#c3e6cb" },
  Medium: { bg: "#fff3cd", text: "#856404", border: "#ffeeba" },
  Hard: { bg: "#f8d7da", text: "#721c24", border: "#f5c6cb" },
};

function CodeBlock({ code }) {
  const lines = code.split('\n');
  return (
    <pre style={{
      background: "#0d1117",
      border: "1px solid #30363d",
      borderRadius: "8px",
      padding: "16px",
      overflowX: "auto",
      fontSize: "13px",
      lineHeight: "1.6",
      margin: "12px 0",
      color: "#e6edf3",
      fontFamily: "'Fira Code', 'Cascadia Code', monospace",
    }}>
      {lines.map((line, i) => {
        const colored = line
          .replace(/(#[^\n]*)/g, '<span style="color:#8b949e">$1</span>')
          .replace(/(".*?")/g, '<span style="color:#a5d6ff">$1</span>')
          .replace(/\b(from|import|def|return|if|else|for|in|not|and|or|True|False|None)\b/g, '<span style="color:#ff7b72">$1</span>')
          .replace(/\b(spark|df|result|windowSpec|delta_table|stream_df|query)\b/g, '<span style="color:#79c0ff">$1</span>');
        return <div key={i} dangerouslySetInnerHTML={{ __html: colored || ' ' }} />;
      })}
    </pre>
  );
}

function renderAnswer(text) {
  const parts = text.split(/(```[\s\S]*?```|\*\*.*?\*\*)/g);
  return parts.map((part, i) => {
    if (part.startsWith('```') && part.endsWith('```')) {
      const code = part.slice(3).replace(/^python\n/, '').slice(0, -3);
      return <CodeBlock key={i} code={code} />;
    }
    if (part.startsWith('**') && part.endsWith('**')) {
      return <strong key={i} style={{ color: "#ff6b35", fontWeight: 700 }}>{part.slice(2, -2)}</strong>;
    }
    return <span key={i}>{part}</span>;
  });
}

export default function App() {
  const [selectedCategory, setSelectedCategory] = useState("All");
  const [selectedDifficulty, setSelectedDifficulty] = useState("All");
  const [expandedId, setExpandedId] = useState(null);
  const [showFollowUp, setShowFollowUp] = useState({});
  const [studiedIds, setStudiedIds] = useState(new Set());
  const [searchQuery, setSearchQuery] = useState("");
  const [activeTab, setActiveTab] = useState("questions");

  const filtered = questions.filter(q => {
    const matchCat = selectedCategory === "All" || q.category === selectedCategory;
    const matchDiff = selectedDifficulty === "All" || q.difficulty === selectedDifficulty;
    const matchSearch = q.question.toLowerCase().includes(searchQuery.toLowerCase()) ||
                        q.category.toLowerCase().includes(searchQuery.toLowerCase());
    return matchCat && matchDiff && matchSearch;
  });

  const toggleStudied = (id) => {
    setStudiedIds(prev => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  };

  const progress = Math.round((studiedIds.size / questions.length) * 100);

  const tips = [
    { icon: "🎯", title: "STAR Method", body: "Structure behavioral answers as Situation → Task → Action → Result. Always quantify your impact." },
    { icon: "⚡", title: "Performance First", body: "Interviewers love when you proactively mention performance tradeoffs. Don't just answer — optimize." },
    { icon: "🔍", title: "Use Spark UI", body: "Mention the Spark UI in debugging answers. It shows you've worked with production systems." },
    { icon: "📐", title: "Know Your Numbers", body: "Default shuffle partitions: 200. Broadcast threshold: 10MB. Target partition size: ~128-200MB." },
    { icon: "🌊", title: "Mention AQE", body: "Adaptive Query Execution (Spark 3.x) is a silver bullet. Mention it for skew, shuffle, and join optimizations." },
    { icon: "💾", title: "Delta Lake Matters", body: "Most DE roles use Delta Lake or similar. Know MERGE for upserts and SCD Type 2 patterns cold." },
    { icon: "🧪", title: "Testing Spark", body: "Know how to unit test PySpark with pytest and chispa. Shows production maturity." },
    { icon: "📊", title: "Data Modeling", body: "Be ready for questions on medallion architecture (Bronze/Silver/Gold) — very common in Databricks environments." },
  ];

  return (
    <div style={{
      minHeight: "100vh",
      background: "linear-gradient(135deg, #0f0c29, #302b63, #24243e)",
      fontFamily: "'Segoe UI', system-ui, sans-serif",
      color: "#e8e8f0",
    }}>
      {/* Header */}
      <div style={{
        background: "rgba(255,255,255,0.04)",
        backdropFilter: "blur(20px)",
        borderBottom: "1px solid rgba(255,255,255,0.08)",
        padding: "28px 32px",
        position: "sticky",
        top: 0,
        zIndex: 100,
      }}>
        <div style={{ maxWidth: 1100, margin: "0 auto" }}>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: 16 }}>
            <div>
              <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 4 }}>
                <span style={{ fontSize: 28 }}>⚡</span>
                <h1 style={{ margin: 0, fontSize: 26, fontWeight: 800, letterSpacing: "-0.5px",
                  background: "linear-gradient(90deg, #ff6b35, #f7c59f)", WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent" }}>
                  PySpark Interview Prep
                </h1>
              </div>
              <p style={{ margin: 0, color: "rgba(255,255,255,0.5)", fontSize: 14 }}>
                3 YOE · Data Engineering · 15 curated questions
              </p>
            </div>
            <div style={{
              background: "rgba(255,107,53,0.12)",
              border: "1px solid rgba(255,107,53,0.3)",
              borderRadius: 12,
              padding: "12px 20px",
              textAlign: "center",
            }}>
              <div style={{ fontSize: 22, fontWeight: 800, color: "#ff6b35" }}>{progress}%</div>
              <div style={{ fontSize: 11, color: "rgba(255,255,255,0.5)", marginTop: 2 }}>STUDIED</div>
              <div style={{
                marginTop: 6,
                height: 4,
                width: 120,
                background: "rgba(255,255,255,0.1)",
                borderRadius: 2,
                overflow: "hidden",
              }}>
                <div style={{ height: "100%", width: `${progress}%`, background: "linear-gradient(90deg, #ff6b35, #f7c59f)", borderRadius: 2, transition: "width 0.5s" }} />
              </div>
            </div>
          </div>

          {/* Tabs */}
          <div style={{ display: "flex", gap: 4, marginTop: 20 }}>
            {["questions", "tips"].map(tab => (
              <button key={tab} onClick={() => setActiveTab(tab)} style={{
                padding: "8px 20px",
                borderRadius: 8,
                border: "none",
                cursor: "pointer",
                fontWeight: 600,
                fontSize: 14,
                transition: "all 0.2s",
                background: activeTab === tab ? "#ff6b35" : "rgba(255,255,255,0.06)",
                color: activeTab === tab ? "#fff" : "rgba(255,255,255,0.6)",
              }}>
                {tab === "questions" ? "📋 Questions" : "💡 Interview Tips"}
              </button>
            ))}
          </div>
        </div>
      </div>

      <div style={{ maxWidth: 1100, margin: "0 auto", padding: "32px 24px" }}>

        {activeTab === "tips" ? (
          <div>
            <h2 style={{ fontWeight: 800, fontSize: 20, marginBottom: 24, color: "#f7c59f" }}>
              💡 Interview Strategy for 3 YOE Data Engineers
            </h2>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(300px, 1fr))", gap: 16 }}>
              {tips.map((tip, i) => (
                <div key={i} style={{
                  background: "rgba(255,255,255,0.04)",
                  border: "1px solid rgba(255,255,255,0.08)",
                  borderRadius: 14,
                  padding: "20px 22px",
                  transition: "transform 0.2s, border-color 0.2s",
                }}>
                  <div style={{ fontSize: 28, marginBottom: 10 }}>{tip.icon}</div>
                  <div style={{ fontWeight: 700, fontSize: 15, color: "#ff6b35", marginBottom: 8 }}>{tip.title}</div>
                  <div style={{ fontSize: 14, color: "rgba(255,255,255,0.65)", lineHeight: 1.6 }}>{tip.body}</div>
                </div>
              ))}
            </div>

            <div style={{
              marginTop: 32,
              background: "rgba(255,107,53,0.08)",
              border: "1px solid rgba(255,107,53,0.25)",
              borderRadius: 14,
              padding: 24,
            }}>
              <h3 style={{ fontWeight: 800, color: "#ff6b35", marginTop: 0 }}>🎯 Most Likely Topics for Your Level</h3>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, fontSize: 14 }}>
                {[
                  "Data skew & how you handled it",
                  "Explain a complex pipeline you built",
                  "Performance optimization you did",
                  "Broadcast vs Sort-Merge joins",
                  "Delta Lake MERGE / upserts",
                  "Structured Streaming + watermarking",
                  "Partitioning strategy decisions",
                  "Debugging a slow Spark job",
                  "Window functions (rank, row_number)",
                  "AQE and Spark 3.x features",
                ].map((t, i) => (
                  <div key={i} style={{ display: "flex", alignItems: "flex-start", gap: 8, color: "rgba(255,255,255,0.75)", padding: "4px 0" }}>
                    <span style={{ color: "#ff6b35", marginTop: 1 }}>▸</span> {t}
                  </div>
                ))}
              </div>
            </div>
          </div>

        ) : (
          <>
            {/* Filters */}
            <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginBottom: 24, alignItems: "center" }}>
              <input
                placeholder="🔍 Search questions..."
                value={searchQuery}
                onChange={e => setSearchQuery(e.target.value)}
                style={{
                  padding: "9px 16px",
                  borderRadius: 10,
                  border: "1px solid rgba(255,255,255,0.12)",
                  background: "rgba(255,255,255,0.06)",
                  color: "#e8e8f0",
                  fontSize: 14,
                  outline: "none",
                  minWidth: 220,
                }}
              />
              <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                {categories.map(c => (
                  <button key={c} onClick={() => setSelectedCategory(c)} style={{
                    padding: "7px 14px",
                    borderRadius: 8,
                    border: "1px solid",
                    cursor: "pointer",
                    fontSize: 13,
                    fontWeight: 600,
                    transition: "all 0.15s",
                    borderColor: selectedCategory === c ? "#ff6b35" : "rgba(255,255,255,0.15)",
                    background: selectedCategory === c ? "rgba(255,107,53,0.2)" : "transparent",
                    color: selectedCategory === c ? "#ff6b35" : "rgba(255,255,255,0.6)",
                  }}>{c}</button>
                ))}
              </div>
              <div style={{ display: "flex", gap: 6 }}>
                {difficulties.map(d => (
                  <button key={d} onClick={() => setSelectedDifficulty(d)} style={{
                    padding: "7px 14px",
                    borderRadius: 8,
                    border: `1px solid`,
                    cursor: "pointer",
                    fontSize: 13,
                    fontWeight: 600,
                    transition: "all 0.15s",
                    borderColor: selectedDifficulty === d
                      ? (d === "All" ? "#ff6b35" : difficultyColor[d]?.border || "#ff6b35")
                      : "rgba(255,255,255,0.15)",
                    background: selectedDifficulty === d
                      ? (d === "All" ? "rgba(255,107,53,0.15)" : `${difficultyColor[d]?.bg}33`)
                      : "transparent",
                    color: selectedDifficulty === d
                      ? (d === "All" ? "#ff6b35" : difficultyColor[d]?.text)
                      : "rgba(255,255,255,0.6)",
                  }}>{d}</button>
                ))}
              </div>
            </div>

            <div style={{ fontSize: 13, color: "rgba(255,255,255,0.35)", marginBottom: 20 }}>
              Showing {filtered.length} of {questions.length} questions · {studiedIds.size} studied
            </div>

            {/* Questions */}
            <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
              {filtered.map(q => {
                const isOpen = expandedId === q.id;
                const isStudied = studiedIds.has(q.id);
                const dc = difficultyColor[q.difficulty];

                return (
                  <div key={q.id} style={{
                    background: isStudied ? "rgba(21,87,36,0.15)" : "rgba(255,255,255,0.04)",
                    border: `1px solid ${isStudied ? "rgba(21,87,36,0.4)" : isOpen ? "rgba(255,107,53,0.35)" : "rgba(255,255,255,0.08)"}`,
                    borderRadius: 14,
                    overflow: "hidden",
                    transition: "all 0.2s",
                  }}>
                    {/* Question Header */}
                    <div
                      onClick={() => setExpandedId(isOpen ? null : q.id)}
                      style={{
                        display: "flex",
                        alignItems: "flex-start",
                        gap: 14,
                        padding: "18px 20px",
                        cursor: "pointer",
                        userSelect: "none",
                      }}
                    >
                      <div style={{
                        width: 28,
                        height: 28,
                        borderRadius: "50%",
                        background: isOpen ? "#ff6b35" : "rgba(255,255,255,0.08)",
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "center",
                        fontSize: 13,
                        fontWeight: 700,
                        flexShrink: 0,
                        marginTop: 2,
                        transition: "background 0.2s",
                        color: isOpen ? "#fff" : "rgba(255,255,255,0.5)",
                      }}>
                        {q.id}
                      </div>
                      <div style={{ flex: 1 }}>
                        <div style={{ display: "flex", gap: 8, marginBottom: 6, flexWrap: "wrap" }}>
                          <span style={{
                            fontSize: 11,
                            fontWeight: 700,
                            padding: "2px 10px",
                            borderRadius: 20,
                            background: "rgba(255,107,53,0.15)",
                            color: "#ff6b35",
                            letterSpacing: "0.5px",
                            textTransform: "uppercase",
                          }}>{q.category}</span>
                          <span style={{
                            fontSize: 11,
                            fontWeight: 700,
                            padding: "2px 10px",
                            borderRadius: 20,
                            background: `${dc.bg}33`,
                            color: dc.text,
                            letterSpacing: "0.5px",
                            textTransform: "uppercase",
                          }}>{q.difficulty}</span>
                          {isStudied && <span style={{
                            fontSize: 11,
                            fontWeight: 700,
                            padding: "2px 10px",
                            borderRadius: 20,
                            background: "rgba(21,87,36,0.3)",
                            color: "#56c27c",
                            letterSpacing: "0.5px",
                          }}>✓ STUDIED</span>}
                        </div>
                        <div style={{ fontWeight: 600, fontSize: 15.5, lineHeight: 1.5, color: "#e8e8f0" }}>
                          {q.question}
                        </div>
                      </div>
                      <div style={{ fontSize: 18, color: "rgba(255,255,255,0.3)", transition: "transform 0.2s", transform: isOpen ? "rotate(180deg)" : "none" }}>
                        ↓
                      </div>
                    </div>

                    {/* Answer */}
                    {isOpen && (
                      <div style={{
                        padding: "0 20px 20px 20px",
                        borderTop: "1px solid rgba(255,255,255,0.07)",
                        paddingTop: 20,
                      }}>
                        <div style={{
                          fontSize: 14.5,
                          lineHeight: 1.8,
                          color: "rgba(255,255,255,0.82)",
                          whiteSpace: "pre-wrap",
                        }}>
                          {renderAnswer(q.answer)}
                        </div>

                        {/* Follow-up */}
                        <div style={{
                          marginTop: 20,
                          padding: "14px 18px",
                          background: "rgba(247,197,159,0.07)",
                          borderLeft: "3px solid #f7c59f",
                          borderRadius: "0 10px 10px 0",
                        }}>
                          <div style={{ fontSize: 12, fontWeight: 700, color: "#f7c59f", letterSpacing: "0.5px", marginBottom: 6 }}>
                            💬 LIKELY FOLLOW-UP
                          </div>
                          <div style={{ fontSize: 14, color: "rgba(255,255,255,0.7)" }}>{q.followUp}</div>
                        </div>

                        {/* Mark studied */}
                        <div style={{ marginTop: 16, display: "flex", justifyContent: "flex-end" }}>
                          <button
                            onClick={() => toggleStudied(q.id)}
                            style={{
                              padding: "8px 18px",
                              borderRadius: 8,
                              border: `1px solid ${isStudied ? "rgba(86,194,124,0.4)" : "rgba(255,255,255,0.15)"}`,
                              background: isStudied ? "rgba(86,194,124,0.15)" : "rgba(255,255,255,0.06)",
                              color: isStudied ? "#56c27c" : "rgba(255,255,255,0.55)",
                              cursor: "pointer",
                              fontSize: 13,
                              fontWeight: 600,
                              transition: "all 0.2s",
                            }}
                          >
                            {isStudied ? "✓ Mark as Unread" : "Mark as Studied"}
                          </button>
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </>
        )}
      </div>
    </div>
  );
}
