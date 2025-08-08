# Spark Intermediate 

## 1. Partitioning Strategies

### Core Concept
**Data distribution mechanism** that determines how data is split across cluster nodes, directly impacting performance, shuffle operations, and parallelism.

### Types of Partitioning

#### Hash Partitioning (Default)
```scala
// Default partitioner uses hash function
val hashPartitioned = rdd.partitionBy(new HashPartitioner(numPartitions))

// Hash function: hash(key) % numPartitions
// Ensures even distribution for random keys
```

#### Range Partitioning
```scala
// Partitions based on key ranges
val rangePartitioned = rdd.partitionBy(new RangePartitioner(numPartitions, rdd))

// Useful for ordered data and range queries
// Partitions: [1-100], [101-200], [201-300], etc.
```

#### Custom Partitioning
```scala
// Define custom partitioner
class GeographicPartitioner(numPartitions: Int) extends Partitioner {
  def getPartition(key: Any): Int = {
    val location = key.asInstanceOf[String]
    location match {
      case city if city.startsWith("US_") => 0
      case city if city.startsWith("EU_") => 1
      case city if city.startsWith("ASIA_") => 2
      case _ => 3
    }
  }
  
  def numPartitions: Int = numPartitions
}

val geoPartitioned = cityDataRDD.partitionBy(new GeographicPartitioner(4))
```

### How Partitioning Works Internally
```
Data Input → Partitioner Function → Route to Executors
    ↓              ↓                      ↓
Key-Value → hash(key) % partitions → Executor N
```

### DataFrames Partitioning
```scala
// DataFrame partitioning
val partitionedDF = df
  .repartition($"country")        // Hash partition by country
  .sortWithinPartitions($"timestamp") // Sort within each partition

// Custom partitioning with SQL
spark.sql("""
  INSERT OVERWRITE TABLE partitioned_table
  PARTITION(year=2023, month=12)  
  SELECT * FROM source_table
""")
```

### Partition Information
```scala
// Check current partitioning
rdd.partitioner          // Returns Option[Partitioner]
rdd.getNumPartitions     // Number of partitions
df.rdd.partitions.length // DataFrame partitions

// View partition contents
rdd.glom().collect()     // Array of partition contents
df.rdd.mapPartitionsWithIndex((idx, iter) => 
  Iterator(s"Partition $idx: ${iter.size} records")).collect()
```

### Best Practices
- **Key Selection**: Choose keys with good distribution
- **Partition Count**: 2-4x number of CPU cores
- **Data Locality**: Partition to minimize network shuffle
- **Join Optimization**: Co-partition datasets for efficient joins
- **Avoid Over-partitioning**: Too many small partitions create overhead

**Analogy**: Like organizing a library - hash partitioning randomly distributes books across floors, range partitioning groups by alphabetical sections, custom partitioning groups by subject (fiction, science, etc.).

### Uber Example
Uber partitions driver location data by city using custom partitioner, ensuring all NYC trips stay on same cluster nodes, reducing network I/O for city-specific surge pricing calculations by 80%.

---

## 2. Repartition vs Coalesce

### Core Concept
**Partition management operations** that change the number of partitions in RDD/DataFrame, with different performance characteristics and use cases.

### Repartition - Full Shuffle
```scala
// Creates new partitions with full shuffle
val repartitioned = df.repartition(10)  // Exactly 10 partitions

// Repartition by column (hash partitioning)
val byColumn = df.repartition($"country")

// Repartition with specific count and column
val combined = df.repartition(20, $"user_id")
```

### Coalesce - Minimize Shuffle
```scala
// Reduces partitions without full shuffle
val coalesced = df.coalesce(5)  // At most 5 partitions

// Only reduces partitions, never increases
val cantIncrease = df.coalesce(50)  // If df has 10 partitions, still 10
```

### How They Work Internally

#### Repartition Process
```
Current Partitions: [P1] [P2] [P3] [P4]
         ↓ Full Shuffle ↓
New Partitions: [N1] [N2] [N3] [N4] [N5]
(Data from all partitions redistributed)
```

#### Coalesce Process
```
Current Partitions: [P1] [P2] [P3] [P4] [P5] [P6]
         ↓ Combine Adjacent ↓
New Partitions: [P1+P2] [P3+P4] [P5+P6]
(Minimal data movement)
```

### Performance Comparison
| Operation | Shuffle | Network I/O | Use Case |
|-----------|---------|-------------|----------|
| repartition() | Full | High | Increase partitions, even distribution |
| coalesce() | Minimal | Low | Reduce partitions, avoid small files |

### When to Use Each

#### Use Repartition When:
```scala
// Need more partitions for parallelism
val moreParallel = smallDF.repartition(100)

// Need even distribution across partitions
val evenDistribution = skewedDF.repartition(20)

// Preparing for joins (co-partitioning)
val prepared = df1.repartition($"join_key")
```

#### Use Coalesce When:
```scala
// Reducing partitions before writing (avoid small files)
df.coalesce(1).write.parquet("output/")

// After filtering (fewer records, fewer partitions needed)
val filtered = largeDF.filter($"status" === "active")
val optimized = filtered.coalesce(10)  // Reduce from 100 to 10 partitions
```

### Advanced Usage
```scala
// Coalesce with shuffle (acts like repartition with fewer partitions)
df.coalesce(5, shuffle = true)

// Optimal partition calculation
val targetPartitionSize = 128 * 1024 * 1024  // 128MB per partition
val optimalPartitions = (dataSize / targetPartitionSize).toInt
val optimized = df.coalesce(optimalPartitions)
```

### Small Files Problem
```scala
// Problem: Many small output files
largeDF.write.parquet("output/")  // Creates 1000 tiny files

// Solution: Coalesce before writing
largeDF
  .coalesce(10)  // Reduce to 10 partitions
  .write
  .mode("overwrite")
  .parquet("output/")  // Creates 10 reasonably sized files
```

### Best Practices
- **Before Writing**: Always coalesce to avoid small files
- **After Filtering**: Reduce partitions after significant data reduction
- **For Joins**: Repartition on join keys for better performance
- **Monitor Size**: Target 128MB-1GB per partition
- **Avoid Excessive**: Don't over-repartition; it's expensive

**Analogy**: Repartition is like completely reshuffling a deck of cards and dealing new hands. Coalesce is like combining some hands together without reshuffling the entire deck.

### Uber Example
Uber's ETL pipeline coalesces filtered trip data from 1000 to 50 partitions before writing to S3, reducing output from 1000 tiny files to 50 optimally-sized files, improving downstream query performance by 5x.

---

## 3. Shuffle Operations

### Core Concept
**Expensive data redistribution** across cluster nodes that occurs during wide transformations, involving disk writes, network transfers, and memory usage.

### What Triggers Shuffle
```scala
// Wide transformations that trigger shuffle
val shuffled1 = rdd.groupByKey()           // Group by key
val shuffled2 = rdd.reduceByKey(_ + _)     // Reduce by key  
val shuffled3 = rdd1.join(rdd2)            // Join operations
val shuffled4 = rdd.distinct()             // Remove duplicates
val shuffled5 = df.repartition($"column")  // Explicit repartitioning
val shuffled6 = df.orderBy($"timestamp")   // Global sorting
```

### Shuffle Process Internal Flow
```
Stage 1 (Map Side):
Partition 1 → [Shuffle Write] → Local Disk Files
Partition 2 → [Shuffle Write] → Local Disk Files  
Partition 3 → [Shuffle Write] → Local Disk Files

Network Transfer:
Local Disk → Network → Remote Executors

Stage 2 (Reduce Side):  
[Shuffle Read] → Partition A (keys 1-100)
[Shuffle Read] → Partition B (keys 101-200)
[Shuffle Read] → Partition C (keys 201-300)
```

### Shuffle Phases

#### Phase 1: Shuffle Write (Map Side)
```scala
// Each task writes shuffle data to local disk
// Data partitioned by target partition ID
// Creates shuffle files: shuffle_0_0, shuffle_0_1, etc.
```

#### Phase 2: Shuffle Read (Reduce Side)
```scala
// Tasks read shuffle data from remote executors
// Network transfer and decompression
// May involve sorting and merging
```

### Shuffle Configuration
```scala
// Shuffle performance tuning
spark.conf.set("spark.sql.shuffle.partitions", "400")  // Default 200
spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "128MB")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

// Shuffle service settings
spark.conf.set("spark.shuffle.service.enabled", "true")
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.shuffle.spill.compress", "true")
```

### Shuffle Optimization Strategies

#### 1. Reduce Shuffle Operations
```scala
// Bad: Multiple shuffles
val result1 = data
  .groupByKey()        // Shuffle 1
  .map(processGroup)   // Processing
  .sortByKey()         // Shuffle 2

// Good: Combine operations
val result2 = data
  .reduceByKey(combineValues)  // Single shuffle with combining
  .sortByKey()                 // Still need sort shuffle
```

#### 2. Use Combiners
```scala
// Bad: groupByKey followed by aggregation
val inefficient = keyValueRDD
  .groupByKey()                    // Shuffle all values
  .mapValues(values => values.sum) // Sum after shuffle

// Good: reduceByKey with combining
val efficient = keyValueRDD
  .reduceByKey(_ + _)  // Combine locally before shuffle
```

#### 3. Broadcast Joins
```scala
// Bad: Large shuffle for join
val bigJoin = largeDF.join(smallDF, "key")  // Both sides shuffled

// Good: Broadcast small side
val broadcastJoin = largeDF.join(broadcast(smallDF), "key")  // No shuffle
```

### Shuffle Spill Behavior
```scala
// When shuffle data exceeds memory
// Spark spills to disk automatically
// Configure spill thresholds
spark.conf.set("spark.shuffle.spill.diskWriteBufferSize", "1m")
spark.conf.set("spark.shuffle.file.buffer", "32k")
```

### Monitoring Shuffle
```scala
// Spark UI shows shuffle metrics:
// - Shuffle Read/Write bytes
// - Shuffle Read/Write records  
// - Spill memory/disk
// - Task duration breakdown

// Programmatic shuffle metrics
val metrics = spark.sparkContext.statusTracker.getExecutorInfos
  .map(_.memoryUsed).sum
```

### Adaptive Query Execution (AQE) for Shuffle
```scala
// AQE optimizes shuffles dynamically
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

// Benefits:
// - Coalesce small shuffle partitions
// - Handle skewed joins automatically  
// - Adjust partition counts based on data size
```

### Best Practices
- **Minimize Shuffles**: Use narrow transformations when possible
- **Use Combiners**: Prefer reduceByKey over groupByKey
- **Broadcast Small Data**: Use broadcast joins for small datasets
- **Tune Partitions**: Optimize shuffle partition count
- **Enable AQE**: Let Spark optimize shuffles adaptively
- **Monitor Spill**: Watch for excessive disk spill

**Analogy**: Like reorganizing a large office - everyone packs their current work (shuffle write), moves to new departments (network transfer), and unpacks in new locations (shuffle read). Very expensive but sometimes necessary for efficiency.

### Uber Example
Uber optimized driver-rider matching by reducing shuffles from 3 to 1 (eliminated intermediate groupBy), enabled broadcast joins for city boundaries, and tuned shuffle partitions from 200 to 800, reducing match latency from 5 seconds to 1 second.

---

## 4. Bucketing

### Core Concept
**Pre-partitioning and sorting** data during write time to eliminate shuffles during subsequent reads and joins, trading write-time cost for read-time performance.

### How Bucketing Works
```scala
// Create bucketed table
df.write
  .mode("overwrite")
  .option("path", "s3://bucket/user_data")
  .bucketBy(10, "user_id")           // 10 buckets on user_id
  .sortBy("timestamp")               // Sort within buckets
  .saveAsTable("bucketed_users")

// File structure created:
// bucket_00000  bucket_00001  ...  bucket_00009
// Each bucket contains users with hash(user_id) % 10 == bucket_id
```

### Internal Bucketing Process
```
Write Time:
Data → Hash(user_id) → Bucket Assignment → Sort within Bucket → Write Files

Read Time:  
Query → Bucket Pruning → Read Only Required Buckets → No Shuffle Join
```

### Bucketing Benefits

#### 1. Shuffle-Free Joins
```scala
// Both tables bucketed on same key with same number of buckets
val users = spark.table("bucketed_users")     // 10 buckets on user_id
val orders = spark.table("bucketed_orders")   // 10 buckets on user_id

// No shuffle join - buckets joined directly
val result = users.join(orders, "user_id")
// Bucket 0 joins with Bucket 0, Bucket 1 with Bucket 1, etc.
```

#### 2. Efficient Sampling
```scala
// Read subset of buckets for sampling
val sample = spark.read
  .option("spark.sql.sources.bucketing.enabled", "true")
  .table("bucketed_table")
  .sample(0.1)  // Reads ~1 bucket instead of scanning entire table
```

#### 3. Bucket Pruning
```scala
// Filter pushdown to specific buckets
val filtered = spark.table("bucketed_users")
  .filter($"user_id" === 12345)
// Only reads bucket: hash(12345) % 10, not all 10 buckets
```

### Bucketing Requirements
```scala
// Requirements for bucketing to work:
// 1. Same bucketing column(s)
// 2. Same number of buckets
// 3. Same hash function
// 4. Bucketing enabled in Spark session

spark.conf.set("spark.sql.sources.bucketing.enabled", "true")
spark.conf.set("spark.sql.sources.bucketing.maxBuckets", "100000")
```

### Bucketing vs Partitioning
| Aspect | Bucketing | Partitioning |
|--------|-----------|--------------|
| File Organization | Fixed number of files | Subdirectories |
| Pruning | Hash-based | Value-based |
| Join Optimization | Eliminates shuffle | Eliminates some data reading |
| Cardinality | High cardinality keys | Low cardinality keys |
| Use Case | Joins, point lookups | Time-based queries |

### Advanced Bucketing
```scala
// Multi-column bucketing
df.write
  .bucketBy(20, "user_id", "region")  // Composite bucketing key
  .sortBy("timestamp", "event_type")  // Multiple sort columns
  .saveAsTable("multi_bucketed")

// Dynamic bucket count based on data size
val optimalBuckets = Math.max(1, dataSize / (128 * 1024 * 1024))  // 128MB per bucket
df.write.bucketBy(optimalBuckets, "key").saveAsTable("optimized_bucketed")
```

### Bucketing Limitations
- **Write Performance**: Slower writes due to sorting and bucketing
- **Fixed Structure**: Difficult to change bucketing once set
- **Storage Overhead**: May create uneven bucket sizes
- **Metastore Dependency**: Requires Hive metastore for metadata

### Best Practices
- **Join-Heavy Workloads**: Bucket on frequently joined keys
- **Bucket Count**: Choose based on data size and parallelism needs
- **Composite Keys**: Use multiple columns for better distribution
- **Monitor Skew**: Check for bucket size imbalance
- **Cost-Benefit**: Evaluate write cost vs read benefits

### Monitoring Bucketing
```scala
// Check if bucketing is being used
df.explain()  // Look for "bucketed scan" in physical plan

// Bucket file sizes
val bucketSizes = spark.sql("""
  SHOW TABLE EXTENDED LIKE 'bucketed_table'
""").collect()
```

**Analogy**: Like organizing a warehouse with numbered aisles based on product category - takes extra time during stocking (write) but makes picking orders (reads/joins) much faster since you know exactly which aisle to go to.

### Uber Example
Uber buckets trip data by city_id (50 buckets) and orders by city_id, enabling shuffle-free joins for surge pricing calculations. Despite 20% slower writes, query performance improved 3x and reduced cluster resource usage by 40%.

---

## 5. Data Skew Detection and Solutions

### Core Concept
**Uneven data distribution** across partitions that causes some tasks to process significantly more data than others, leading to performance bottlenecks and failures.

### Types of Data Skew

#### 1. Partition Skew
```scala
// Some partitions have much more data
// Partition 0: 1M records
// Partition 1: 100 records  
// Partition 2: 2M records  
// Partition 3: 50 records
```

#### 2. Join Skew
```scala
// Hot keys in joins cause skew
val skewedJoin = users.join(events, "user_id")
// If user_id = "bot_user" has 90% of events, one task processes 90% of data
```

#### 3. Aggregation Skew
```scala
// Hot keys in group operations
val skewedAgg = data.groupBy("category").count()
// If category = "mobile" has 80% of records, one partition gets overloaded
```

### Detecting Data Skew

#### Manual Detection
```scala
// Check partition sizes
val partitionSizes = df.rdd.mapPartitionsWithIndex { (idx, iter) =>
  Iterator((idx, iter.size))
}.collect()

partitionSizes.foreach { case (partition, size) =>
  println(s"Partition $partition: $size records")
}

// Check value distribution  
df.groupBy("skewed_column").count()
  .orderBy(desc("count"))
  .show(20)
```

#### Automatic Detection with AQE
```scala
// Enable Adaptive Query Execution skew detection
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
```

### Skew Solutions

#### 1. Salting Technique
```scala
// Add random salt to distribute hot keys
import scala.util.Random

val saltedDF = skewedDF.withColumn("salted_key", 
  concat($"original_key", lit("_"), lit(Random.nextInt(10))))

// Process with salted key, then aggregate results
val result = saltedDF
  .groupBy("salted_key")
  .agg(sum("value").as("partial_sum"))
  .withColumn("original_key", split($"salted_key", "_")(0))
  .groupBy("original_key")  
  .agg(sum("partial_sum").as("final_sum"))
```

#### 2. Two-Phase Aggregation
```scala
// Phase 1: Local aggregation with salt
val phase1 = data
  .withColumn("salt", lit(Random.nextInt(100)))
  .groupBy("key", "salt")
  .agg(sum("value").as("partial_sum"))

// Phase 2: Global aggregation  
val phase2 = phase1
  .groupBy("key")
  .agg(sum("partial_sum").as("total_sum"))
```

#### 3. Broadcast Join for Skewed Keys
```scala
// Extract hot keys and broadcast
val hotKeys = Set("popular_key1", "popular_key2")

val hotData = largeDF.filter($"key".isin(hotKeys: _*))
val normalData = largeDF.filter(!$"key".isin(hotKeys: _*))

// Normal join for regular data
val normalJoin = normalData.join(smallDF, "key")

// Broadcast join for hot keys
val hotJoin = hotData.join(broadcast(smallDF.filter($"key".isin(hotKeys: _*))), "key")

// Union results
val result = normalJoin.union(hotJoin)
```

#### 4. Custom Partitioning
```scala
// Custom partitioner to handle skewed keys
class SkewAwarePartitioner(numPartitions: Int, hotKeys: Set[String]) extends Partitioner {
  def getPartition(key: Any): Int = {
    val keyStr = key.toString
    if (hotKeys.contains(keyStr)) {
      // Distribute hot keys across multiple partitions
      (keyStr.hashCode % (numPartitions / 2)).abs
    } else {
      // Regular partitioning for normal keys
      ((keyStr.hashCode % (numPartitions / 2)) + (numPartitions / 2)).abs
    }
  }
  
  def numPartitions: Int = numPartitions
}
```

#### 5. Iterative Broadcast Join
```scala
// For extremely skewed joins
val threshold = 1000000  // Records threshold

// Identify skewed keys
val keyCount = largeDF.groupBy("join_key").count()
val skewedKeys = keyCount.filter($"count" > threshold).select("join_key").collect()

// Iteratively join in batches
val batchSize = 100
val results = skewedKeys.grouped(batchSize).map { batch =>
  val batchKeys = batch.map(_.getString(0))
  largeDF.filter($"join_key".isin(batchKeys: _*))
    .join(broadcast(smallDF.filter($"join_key".isin(batchKeys: _*))), "join_key")
}.reduce(_.union(_))
```

### Monitoring and Alerting
```scala
// Custom metrics for skew detection
def calculateSkew(df: DataFrame, key: String): Double = {
  val counts = df.groupBy(key).count().collect()
  val totalRecords = counts.map(_.getLong(1)).sum
  val avgPerKey = totalRecords.toDouble / counts.length
  val maxRecords = counts.map(_.getLong(1)).max
  maxRecords.toDouble / avgPerKey  // Skew factor
}

val skewFactor = calculateSkew(df, "user_id")
if (skewFactor > 10) {
  // Alert: High skew detected
}
```

### Best Practices
- **Monitor Continuously**: Set up alerts for task duration variance
- **Profile Data**: Understand key distribution patterns
- **Use AQE**: Enable adaptive query execution for automatic handling
- **Salt Strategically**: Only salt known problematic keys
- **Combine Techniques**: Use multiple approaches for complex skew
- **Test Solutions**: Validate performance improvements

**Analogy**: Like a restaurant where one waiter gets all the difficult customers while others stand idle - you need to redistribute the workload fairly across all staff.

### Uber Example
Uber detected severe skew in driver location updates (90% from NYC), implemented salting with 50 salt values, reducing max task duration from 45 minutes to 3 minutes and eliminating out-of-memory failures during peak traffic.

---

## 6. Join Types and Execution Plans

### Core Concept
**Combining datasets** based on common keys using different algorithms optimized for various data sizes and distribution patterns.

### Join Types

#### Inner Join
```scala
// Returns only matching records from both sides
val innerJoin = df1.join(df2, "key")
// SQL: SELECT * FROM df1 INNER JOIN df2 ON df1.key = df2.key
```

#### Left Outer Join
```scala
// Returns all records from left side, matching from right
val leftJoin = df1.join(df2, Seq("key"), "left_outer")
// NULL values for non-matching right side records
```

#### Right Outer Join  
```scala
// Returns all records from right side, matching from left
val rightJoin = df1.join(df2, Seq("key"), "right_outer")
```

#### Full Outer Join
```scala
// Returns all records from both sides
val fullJoin = df1.join(df2, Seq("key"), "full_outer")  
// NULL values for non-matching records on either side
```

#### Cross Join
```scala
// Cartesian product of both datasets
val crossJoin = df1.crossJoin(df2)
// Every record in df1 combined with every record in df2
```

#### Semi Join
```scala
// Returns records from left that have matches in right (no columns from right)
val semiJoin = df1.join(df2, Seq("key"), "left_semi")
// Like EXISTS clause in SQL
```

#### Anti Join
```scala
// Returns records from left that have NO matches in right
val antiJoin = df1.join(df2, Seq("key"), "left_anti")
// Like NOT EXISTS clause in SQL
```

### Join Execution Strategies

#### 1. Broadcast Hash Join (BHJ)
```scala
// Small table broadcasted to all executors
val broadcastJoin = largeDF.join(broadcast(smallDF), "key")

// Automatic broadcast if table < spark.sql.autoBroadcastJoinThreshold (10MB default)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")
```

**Internal Process**:
```
Small Table → Broadcast to all executors → Build hash map
Large Table → Stream through → Probe hash map → Results
```

#### 2. Sort-Merge Join (SMJ)
```scala
// Both sides sorted and merged
// Default for large-large joins
val sortMergeJoin = largeDF1.join(largeDF2, "key")

// Requires both sides to be sorted by join key
// May require shuffle if not already partitioned correctly
```

**Internal Process**:
```
Both Tables → Sort by join key → Shuffle if needed → Merge sorted streams
```

#### 3. Shuffled Hash Join (SHJ)
```scala
// Build hash table from smaller side after shuffle
// Used when one side is much smaller but too large to broadcast

// Controlled by configuration
spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")
```

#### 4. Nested Loop Join
```scala
// Fallback for complex join conditions
// Very expensive - nested loops over both datasets
val complexJoin = df1.join(df2, 
  $"df1.lat" > $"df2.lat" && $"df1.lon" < $"df2.lon")
```

### Join Strategy Selection
| Left Size | Right Size | Strategy | Performance |
|-----------|------------|----------|-------------|
| Large | Small (<10MB) | Broadcast Hash | Excellent |
| Large | Medium | Shuffled Hash | Good |
| Large | Large | Sort-Merge | Fair |
| Any | Any (complex condition) | Nested Loop | Poor |

### Analyzing Join Plans
```scala
// View execution plan
df1.join(df2, "key").explain(true)

// Look for join strategy in physical plan:
// BroadcastHashJoin
// SortMergeJoin  
// ShuffledHashJoin
// BroadcastNestedLoopJoin
```

### Join Optimization Techniques

#### 1. Bucketing for Join Optimization
```scala
// Pre-partition both tables on join key
df1.write.bucketBy(20, "join_key").saveAsTable("table1")
df2.write.bucketBy(20, "join_key").saveAsTable("table2")

// Shuffle-free join
val optimizedJoin = spark.table("table1").join(spark.table("table2"), "join_key")
```

#### 2. Broadcast Optimization
```scala
// Force broadcast for better performance
val forceBroadcast = largeDF.join(broadcast(mediumDF), "key")

// Broadcast hint in SQL
spark.sql("""
  SELECT /*+ BROADCAST(small_table) */ *
  FROM large_table l
  JOIN small_table s ON l.key = s.key
""")
```

#### 3. Join Reordering
```scala
// Catalyst automatically reorders joins for better performance
val multiJoin = table1
  .join(table2, "key1")      // Large join
  .join(smallTable, "key2")  // Broadcast join - reordered first

// Manual optimization - filter before join
val optimized = largeDF
  .filter($"status" === "active")    // Reduce data size first
  .join(otherDF, "key")              // Then join
```

### Complex Join Scenarios
```scala
// Multi-key joins
val multiKeyJoin = df1.join(df2, 
  df1("user_id") === df2("user_id") && 
  df1("date") === df2("date"))

// Range joins
val rangeJoin = df1.join(df2,
  df1("timestamp") >= df2("start_time") && 
  df1("timestamp") <= df2("end_time"))

// Self joins
val selfJoin = df.as("a").join(df.as("b"),
  $"a.manager_id" === $"b.employee_id")
```

### Best Practices
- **Size Awareness**: Know your data sizes for strategy selection
- **Filter Early**: Apply filters before joins to reduce data
- **Use Broadcast**: Leverage broadcast for small dimension tables
- **Bucket Strategy**: Pre-bucket frequently joined tables
- **Monitor Plans**: Use explain() to verify join strategy
- **Key Selection**: Choose selective join keys when possible

**Analogy**: Like organizing a matchmaking event - broadcast join is like giving everyone a list of potential matches, sort-merge join is like having two sorted lines meet in the middle, hash join is like having a booth where people check for matches.

### Uber Example
Uber optimizes driver-trip matching by broadcasting city boundary data (5MB), using bucketed joins for driver-location correlation (same city_id bucketing), and leveraging AQE for dynamic broadcast of filtered results, reducing join time from 30 seconds to 3 seconds.

---

## 7. Broadcast Variables

### Core Concept
**Read-only variables** cached on each executor node, eliminating the need to send large datasets with every task, reducing network overhead and improving performance.

### How Broadcast Variables Work
```scala
// Create broadcast variable on driver
val cityBoundaries = Map("NYC" -> (40.7, -74.0), "SF" -> (37.7, -122.4))
val broadcastBoundaries = spark.sparkContext.broadcast(cityBoundaries)

// Use in transformations (sent once per executor, not per task)
val enrichedRDD = gpsDataRDD.map { location =>
  val boundaries = broadcastBoundaries.value  // Access broadcast data
  val city = findCity(location, boundaries)
  (location, city)
}

// Cleanup when done
broadcastBoundaries.destroy()
```

### Internal Broadcast Mechanism
```
Driver: Create broadcast variable → Serialize data → Send to Block Manager

Executors: 
Task 1 → Check local cache → Download if not cached → Use data
Task 2 → Check local cache → Use cached data (no download)
Task 3 → Check local cache → Use cached data (no download)
```

### Broadcast vs Regular Variables
| Aspect | Broadcast Variable | Regular Variable |
|--------|-------------------|------------------|
| Network Traffic | Sent once per executor | Sent with every task |
| Memory Usage | Cached on executor | Copied per task |
| Serialization | Once per executor | Once per task |
| Performance | High for large data | Poor for large data |

### When to Use Broadcast Variables
```scala
// Good candidates (small, frequently used data)
val countryCodeMap = Map("US" -> "United States", "UK" -> "United Kingdom")
val mlModel = trainedModel.weights  // ML model parameters
val lookupTable = referenceData.collectAsMap()

// Poor candidates (large data, rarely used)
val hugeDataset = largeDF.collect()  // Too big to broadcast
val oneTimeUse = smallMap            // Used only once
```

### Broadcast Variable Lifecycle
```scala
// 1. Creation (on driver)
val broadcastVar = spark.sparkContext.broadcast(data)

// 2. Usage (in transformations)
rdd.map(record => processWithBroadcast(record, broadcastVar.value))

// 3. Management
broadcastVar.unpersist()  // Remove from executors but keep on driver
broadcastVar.destroy()    // Remove completely (cannot be used again)
```

### Advanced Broadcast Patterns

#### Dynamic Broadcast Updates
```scala
// Periodic broadcast refresh pattern
class BroadcastManager[T](spark: SparkSession, refreshInterval: Long) {
  @volatile private var currentBroadcast: Broadcast[T] = _
  
  def updateBroadcast(newData: T): Unit = {
    val oldBroadcast = currentBroadcast
    currentBroadcast = spark.sparkContext.broadcast(newData)
    if (oldBroadcast != null) oldBroadcast.destroy()
  }
  
  def getValue: T = currentBroadcast.value
}
```

#### Lazy Broadcast Loading
```scala
// Load broadcast data only when needed
lazy val expensiveBroadcast = spark.sparkContext.broadcast(computeExpensiveData())

rdd.mapPartitions { partition =>
  val broadcastData = expensiveBroadcast.value  // Loaded only when accessed
  partition.map(processWithBroadcast(_, broadcastData))
}
```

### Broadcast Configuration
```scala
// Broadcast-related configurations
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")  // Auto broadcast threshold
spark.conf.set("spark.broadcast.blockSize", "4m")                // Broadcast block size
spark.conf.set("spark.broadcast.compress", "true")               // Compress broadcast data
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  // Efficient serialization
```

### Memory Management
```scala
// Monitor broadcast variable memory usage
val executorInfos = spark.sparkContext.statusTracker.getExecutorInfos
executorInfos.foreach { executor =>
  println(s"Executor ${executor.executorId}: ${executor.memoryUsed} bytes used")
}

// Broadcast cleanup
spark.sparkContext.getPersistentRDDs.foreach { case (id, rdd) =>
  if (rdd.name.startsWith("broadcast_")) rdd.unpersist()
}
```

### Best Practices
- **Size Limit**: Keep broadcast variables under 100MB typically
- **Immutability**: Broadcast variables should be read-only
- **Cleanup**: Always destroy unused broadcast variables
- **Serialization**: Use Kryo serializer for complex objects
- **Partitioning**: Use broadcast for lookup tables and dimension data
- **Monitoring**: Track memory usage across executors

### Common Use Cases
```scala
// 1. Lookup tables / Dimension data
val productCatalog = spark.sparkContext.broadcast(productMap)

// 2. ML model parameters
val modelWeights = spark.sparkContext.broadcast(trainedModel.coefficients)

// 3. Configuration data  
val appConfig = spark.sparkContext.broadcast(configMap)

// 4. Small reference datasets
val currencyRates = spark.sparkContext.broadcast(ratesMap)
```

**Analogy**: Like distributing phone books to every neighborhood once, rather than mailing a copy to each house every time someone needs to look up a number.

### Uber Example
Uber broadcasts city geofence boundaries (50MB) and surge pricing zones (20MB) to all executors, reducing network traffic from 5GB per job to 70MB and improving real-time location processing latency by 60%.

---

## 8. Adaptive Query Execution (AQE)

### Core Concept
**Dynamic query optimization** that adjusts execution plans during runtime based on actual data statistics and intermediate results.

### AQE Features

#### 1. Dynamically Coalescing Shuffle Partitions
```scala
// Enable AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

// Before AQE: Fixed 200 shuffle partitions (default)
// After AQE: Dynamically adjust based on data size
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
```

#### 2. Dynamically Switching Join Strategies
```scala
// AQE can switch from sort-merge to broadcast join
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

// If intermediate result becomes small enough, switch to broadcast
val query = largeTable
  .filter($"status" === "active")     // Reduces data size significantly  
  .join(otherTable, "key")           // May switch to broadcast join
```

#### 3. Dynamically Optimizing Skew Joins
```scala
// Automatically detect and handle skewed joins
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
```

### How AQE Works Internally
```
Initial Plan → Execute Stage 1 → Collect Runtime Stats → Reoptimize Remaining Stages
      ↓                              ↓                        ↓
Static Plan          Actual partition sizes,      Dynamic plan adjustments:
Based on            join selectivity,            - Coalesce small partitions
Statistics          filter effectiveness         - Switch join strategies  
                                                 - Handle skew
```

### AQE Configuration
```scala
// Core AQE settings
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

// Tuning parameters
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
spark.conf.set("spark.sql.adaptive.coalescePartitions.maxBatchSize", "100")
spark.conf.set("spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold", "0")
```

### AQE Benefits

#### Before AQE
```scala
// Static plan with suboptimal decisions
val query = spark.sql("""
  SELECT customer_id, SUM(amount) 
  FROM orders 
  WHERE order_date >= '2023-01-01'
  GROUP BY customer_id
""")

// Problems:
// - Fixed 200 partitions regardless of filtered data size
// - No runtime optimization based on actual selectivity
// - Skewed partitions not handled automatically
```

#### After AQE
```scala
// Same query with AQE optimizations
// - Coalesces 200 partitions to 20 based on actual data size
// - Handles any partition skew automatically
// - Optimizes join strategies based on intermediate results
```

### AQE Monitoring
```scala
// View AQE optimizations in execution plan
df.explain()
// Look for:
// "AQEShuffleRead coalesced"
// "BroadcastHashJoin (switched from SortMergeJoin)"
// "Skew join optimization applied"

// AQE metrics in Spark UI
// SQL tab shows adaptive optimizations applied
// Look for "Final Plan" vs "Initial Plan"
```

### Skew Join Optimization Details
```scala
// AQE detects skewed partitions and splits them
// Original skewed partition → Multiple smaller partitions
// Each smaller partition joins with broadcast copy of other side

// Detection criteria:
// partition_size > skewedPartitionThresholdInBytes AND
// partition_size > median_partition_size * skewedPartitionFactor
```

### Dynamic Partition Coalescing
```scala
// AQE combines small adjacent partitions
// Target size: advisoryPartitionSizeInBytes (128MB default)

// Before: [50MB] [30MB] [20MB] [40MB] [60MB] [25MB]
// After:  [80MB]        [60MB]        [85MB]
// Reduces task overhead while maintaining parallelism
```

### Best Practices
- **Always Enable**: AQE provides significant benefits with minimal overhead
- **Tune Thresholds**: Adjust partition size targets based on workload
- **Monitor Impact**: Use Spark UI to verify optimizations
- **Statistics**: Keep table statistics updated for better initial plans
- **Complex Queries**: AQE is most beneficial for multi-stage queries

### AQE Limitations
- **Overhead**: Small overhead for collecting runtime statistics
- **Late Optimization**: Can't optimize very early stages
- **Memory Requirements**: May need more memory for runtime statistics
- **Version Dependency**: Available in Spark 3.0+

**Analogy**: Like a GPS that recalculates your route based on current traffic conditions rather than sticking to the original route planned before you started driving.

### Uber Example
Uber enabled AQE for surge pricing calculations, reducing shuffle partitions from 500 to 50 during low-traffic hours, switching to broadcast joins for filtered city data, and automatically handling skew during peak times, improving query performance by 40% and reducing resource usage by 25%.

---

## 9. Storage Levels and Caching Trade-offs

### Core Concept
**Strategic memory and disk usage** for caching frequently accessed data, balancing speed, memory consumption, fault tolerance, and CPU overhead.

### Comprehensive Storage Levels
```scala
import org.apache.spark.storage.StorageLevel

// Memory-only levels
MEMORY_ONLY         // JVM heap, deserialized objects
MEMORY_ONLY_2       // 2 replicas in memory across nodes
MEMORY_ONLY_SER     // JVM heap, serialized (Java/Kryo)
MEMORY_ONLY_SER_2   // 2 replicas, serialized

// Memory + Disk levels  
MEMORY_AND_DISK     // Spill to disk when memory full
MEMORY_AND_DISK_2   // 2 replicas, memory preferred, disk fallback
MEMORY_AND_DISK_SER // Serialized, memory + disk
MEMORY_AND_DISK_SER_2 // 2 replicas, serialized, memory + disk

// Disk-only levels
DISK_ONLY           // Store only on local disk
DISK_ONLY_2         // 2 replicas on different nodes

// Off-heap storage (requires off-heap memory configured)
OFF_HEAP            // Tachyon/Alluxio or other off-heap storage
```

### Storage Level Selection Matrix
| Scenario | Recommended Level | Reasoning |
|----------|------------------|-----------|
| Frequently accessed, fits in memory | MEMORY_ONLY | Fastest access, no serialization overhead |
| Large dataset, frequent access | MEMORY_ONLY_SER | Memory efficient, acceptable CPU cost |
| Critical data, cannot lose | MEMORY_AND_DISK_2 | Fault tolerance with replication |
| Memory constrained | MEMORY_AND_DISK_SER | Efficient memory use, disk fallback |
| Cold storage, infrequent access | DISK_ONLY | Cost effective, still faster than recomputation |
| GC-sensitive applications | OFF_HEAP | Avoid garbage collection pressure |

### Caching Performance Analysis
```scala
// Measure caching effectiveness
def measureCachePerformance[T](df: DataFrame, level: StorageLevel): Unit = {
  // First run - cache population
  val start1 = System.currentTimeMillis()
  val result1 = df.persist(level).count()
  val time1 = System.currentTimeMillis() - start1
  
  // Second run - cache utilization  
  val start2 = System.currentTimeMillis()
  val result2 = df.count()
  val time2 = System.currentTimeMillis() - start2
  
  println(s"Cache population: ${time1}ms")
  println(s"Cache utilization: ${time2}ms") 
  println(s"Speedup: ${time1.toDouble / time2}x")
}
```

### Memory Management Deep Dive
```scala
// Configure storage memory
spark.conf.set("spark.sql.cache.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.storage.memoryFraction", "0.6")        // 60% for storage
spark.conf.set("spark.storage.safetyFraction", "0.9")       // 90% safety buffer
spark.conf.set("spark.storage.unrollFraction", "0.2")       // 20% for unrolling blocks

// Off-heap configuration
spark.conf.set("spark.sql.columnVector.offheap.enabled", "true")
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "2g")
```

### Serialization Trade-offs
```scala
// Java serialization (default, slower, more memory)
val javaSerialized = df.persist(StorageLevel.MEMORY_ONLY_SER)

// Kryo serialization (faster, less memory, requires configuration)
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
val kryoSerialized = df.persist(StorageLevel.MEMORY_ONLY_SER)

// Custom serialization for specific types
spark.conf.set("spark.kryo.registrator", "com.company.MyKryoRegistrator")
```

### Cache Monitoring and Management
```scala
// Monitor cache status
val storageStatus = spark.sparkContext.getExecutorStorageStatus
storageStatus.foreach { executor =>
  println(s"Executor ${executor.blockManagerId.executorId}:")
  println(s"  Memory used: ${executor.memoryUsed} / ${executor.maxMemory}")
  println(s"  Disk used: ${executor.diskUsed}")
}

// Programmatic cache management
def manageCacheMemory(threshold: Double): Unit = {
  val used = spark.sparkContext.getExecutorStorageStatus
    .map(_.memoryUsed).sum
  val total = spark.sparkContext.getExecutorStorageStatus
    .map(_.maxMemory).sum
    
  if (used.toDouble / total > threshold) {
    // Unpersist least recently used RDDs
    spark.sparkContext.getPersistentRDDs
      .values.toSeq
      .sortBy(_.name)  // Or implement LRU tracking
      .take(5)
      .foreach(_.unpersist())
  }
}
```

### Dynamic Storage Level Selection
```scala
def selectOptimalStorageLevel(dataSize: Long, memoryAvailable: Long, 
                            accessFrequency: Int, faultTolerance: Boolean): StorageLevel = {
  (dataSize, memoryAvailable, accessFrequency, faultTolerance) match {
    case (size, memory, freq, _) if size < memory * 0.5 && freq > 5 => 
      StorageLevel.MEMORY_ONLY
    case (size, memory, freq, true) if size < memory * 0.8 && freq > 3 => 
      StorageLevel.MEMORY_ONLY_2  
    case (size, memory, freq, false) if size > memory => 
      StorageLevel.MEMORY_AND_DISK_SER
    case (_, _, freq, true) if freq > 2 => 
      StorageLevel.MEMORY_AND_DISK_2
    case _ => 
      StorageLevel.DISK_ONLY
  }
}
```

### Cache Eviction Strategies
```scala
// LRU eviction implementation
class CacheManager {
  private val accessTimes = mutable.Map[String, Long]()
  
  def accessRDD(rddName: String): Unit = {
    accessTimes(rddName) = System.currentTimeMillis()
  }
  
  def evictLRU(count: Int): Seq[String] = {
    accessTimes.toSeq
      .sortBy(_._2)  // Sort by access time
      .take(count)
      .map(_._1)
  }
}
```

### Best Practices
- **Profile Before Caching**: Measure actual access patterns
- **Size Awareness**: Monitor memory consumption vs performance gain
- **Appropriate Levels**: Match storage level to use case requirements  
- **Cleanup Discipline**: Unpersist when data no longer needed
- **Serialization Choice**: Use Kryo for better serialized storage
- **Fault Tolerance**: Use replication for critical cached data
- **Memory Tuning**: Configure storage memory fraction appropriately

### Common Anti-patterns
```scala
// BAD: Caching everything
df1.cache()
df2.cache() 
df3.cache()  // May cause memory pressure

// BAD: Wrong storage level
smallDF.persist(StorageLevel.DISK_ONLY)  // Should use MEMORY_ONLY

// BAD: Not unpersisting
tempDF.cache()
// ... use tempDF
// tempDF.unpersist()  // MISSING - memory leak

// GOOD: Strategic caching
val baseData = rawDF.filter(expensiveCondition).cache()  // Cache after expensive operation
val result1 = baseData.groupBy("col1").sum()
val result2 = baseData.groupBy("col2").avg() 
baseData.unpersist()  // Clean up when done
```

**Analogy**: Like organizing your workspace - keep frequently used items on your desk (MEMORY_ONLY), less frequent items in drawers (MEMORY_AND_DISK), and archives in storage (DISK_ONLY). Important documents get copies (replication).

### Uber Example
Uber uses MEMORY_ONLY for real-time driver locations (5GB, accessed 100+ times/min), MEMORY_AND_DISK_2 for city boundaries (critical data, moderate access), and DISK_ONLY for historical analytics (large, infrequent access), optimizing memory usage while maintaining performance.

---

## 10. File Formats and Compression

### Core Concept
**Data storage optimization** through efficient file formats and compression algorithms that balance storage space, I/O performance, and query speed.

### File Formats Comparison

#### Columnar Formats

##### Parquet
```scala
// Write Parquet with optimization
df.write
  .mode("overwrite")
  .option("compression", "snappy")           // Fast compression
  .option("parquet.block.size", "134217728") // 128MB block size
  .parquet("s3://data/parquet/")

// Read with predicate pushdown
val filtered = spark.read.parquet("s3://data/parquet/")
  .filter($"year" === 2023)                 // Pushed to file reader
  .select("id", "amount", "timestamp")      // Column pruning
```

##### Delta Lake
```scala
// Delta table with ACID properties
df.write
  .format("delta")
  .mode("overwrite")
  .option("delta.autoOptimize.optimizeWrite", "true")
  .save("s3://data/delta-table/")

// Time travel and versioning
val historical = spark.read
  .format("delta")
  .option("timestampAsOf", "2023-01-01 00:00:00")
  .load("s3://data/delta-table/")
```

#### Row-Based Formats

##### Avro
```scala
// Schema-rich format with evolution support
df.write
  .format("avro")
  .option("avroSchema", schemaString)
  .save("s3://data/avro/")

// Read with schema evolution
val evolved = spark.read
  .format("avro")
  .load("s3://data/avro/")
```

### Format Performance Characteristics
| Format | Storage Efficiency | Query Speed | Schema Evolution | ACID Support |
|--------|-------------------|-------------|------------------|--------------|
| Parquet | Excellent | Excellent | Limited | No |
| Delta | Excellent | Excellent | Good | Yes |
| Avro | Good | Good | Excellent | No |
| JSON | Poor | Poor | Excellent | No |
| CSV | Poor | Poor | None | No |

### Compression Algorithms

#### Snappy (Default for Parquet)
```scala
// Fast compression/decompression, moderate compression ratio
df.write
  .option("compression", "snappy")
  .parquet("data/snappy/")

// Best for: Real-time analytics, frequent reads
// Compression ratio: ~2-3x
// Speed: Very fast
```

#### ZSTD (Zstandard)
```scala
// Excellent compression ratio with good speed
df.write
  .option("compression", "zstd")
  .option("zstd.level", "3")  // Compression level 1-22
  .parquet("data/zstd/")

// Best for: Cold storage, archival data
// Compression ratio: ~3-5x  
// Speed: Fast
```

#### GZIP
```scala
// High compression ratio, slower speed
df.write
  .option("compression", "gzip")
  .parquet("data/gzip/")

// Best for: Network transfer, long-term storage
// Compression ratio: ~3-4x
// Speed: Moderate
```

#### LZ4
```scala
// Fastest compression, lower compression ratio
df.write
  .option("compression", "lz4") 
  .parquet("data/lz4/")

// Best for: High throughput scenarios
// Compression ratio: ~2x
// Speed: Fastest
```

### Compression Performance Matrix
| Algorithm | Compression Speed | Decompression Speed | Compression Ratio | Use Case |
|-----------|------------------|-------------------|------------------|----------|
| LZ4 | Fastest | Fastest | 2x | High throughput |
| Snappy | Very Fast | Very Fast | 2-3x | General purpose |
| ZSTD | Fast | Fast | 3-5x | Balanced performance |
| GZIP | Moderate | Moderate | 3-4x | Network/archive |

### Partition Pruning Optimization
```scala
// Partition by common filter columns
df.write
  .mode("overwrite")
  .partitionBy("year", "month", "region")    // Physical partitioning
  .option("compression", "snappy")
  .parquet("s3://data/partitioned/")

// Query with partition pruning
val pruned = spark.read.parquet("s3://data/partitioned/")
  .filter($"year" === 2023 && $"month" === 12)  // Only scans relevant partitions
  .filter($"region" === "US")                    // Further partition pruning
```

### Advanced Parquet Optimizations
```scala
// Fine-tune Parquet settings
df.write
  .option("parquet.block.size", "134217728")           // 128MB blocks
  .option("parquet.page.size", "1048576")              // 1MB pages  
  .option("parquet.dictionary.page.size", "1048576")   // 1MB dictionary
  .option("parquet.enable.dictionary", "true")         // Enable dictionary encoding
  .option("parquet.compression.codec", "snappy")       // Compression
  .parquet("optimized/")
```

### Delta Lake Advanced Features
```scala
// Optimize and Z-order for better performance
spark.sql("OPTIMIZE delta_table ZORDER BY (user_id, timestamp)")

// Vacuum old files
spark.sql("VACUUM delta_table RETAIN 168 HOURS")  // Keep 7 days

// Auto-compaction
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")
```

### Format Selection Guidelines
```scala
def selectOptimalFormat(useCase: String, dataSize: String, 
                       queryPattern: String, acidNeeds: Boolean): String = {
  (useCase, dataSize, queryPattern, acidNeeds) match {
    case ("analytics", "large", "columnar", false) => "parquet"
    case ("analytics", _, _, true) => "delta"  
    case ("streaming", _, "append-heavy", true) => "delta"
    case ("schema_evolution", _, _, false) => "avro"
    case ("interchange", "small", "row-based", false) => "json"
    case _ => "parquet"  // Default recommendation
  }
}
```

### Monitoring File Format Performance
```scala
// Monitor compression effectiveness  
def analyzeCompressionRatio(path: String): Unit = {
  val fileSystem = org.apache.hadoop.fs.FileSystem.get(
    spark.sparkContext.hadoopConfiguration)
  val files = fileSystem.listFiles(new org.apache.hadoop.fs.Path(path), true)
  
  var totalSize = 0L
  var fileCount = 0
  
  while (files.hasNext) {
    val file = files.next()
    totalSize += file.getLen
    fileCount += 1
  }
  
  println(s"Files: $fileCount, Total size: ${totalSize / (1024*1024)}MB")
}

// Query performance analysis
val startTime = System.currentTimeMillis()
val result = spark.read.parquet("data/").filter($"col" > 100).count()
val queryTime = System.currentTimeMillis() - startTime
println(s"Query time: ${queryTime}ms, Result: $result")
```

### Best Practices
- **Analytics Workloads**: Use Parquet with Snappy compression
- **ACID Requirements**: Use Delta Lake for transactional guarantees
- **Schema Evolution**: Use Avro for frequently changing schemas
- **Partition Strategy**: Partition by common filter columns
- **Compression Choice**: Balance speed vs storage based on access patterns
- **File Size**: Target 128MB-1GB files for optimal performance
- **Column Ordering**: Place frequently filtered columns first in schema

### Small Files Problem Solutions
```scala
// Problem: Many small files hurt performance
df.repartition(10)  // Coalesce before writing
  .write
  .mode("overwrite")
  .parquet("output/")

// Delta Lake auto-compaction
spark.sql("""
  CREATE TABLE optimized_table
  USING DELTA
  TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
  )
""")
```

**Analogy**: Like choosing the right container for storage - Parquet is like vacuum-sealed bags (great compression, organized), Delta is like a filing cabinet with locks (organized + secure), Avro is like labeled boxes (flexible labeling), JSON is like transparent bags (easy to see contents but bulky).

### Uber Example
Uber uses Parquet with Snappy compression for trip analytics (10:1 compression ratio), Delta Lake for driver state management (ACID guarantees for status updates), and ZSTD compression for long-term trip archives (5:1 compression), reducing storage costs by 60% while maintaining query performance.

---

## Real-World Analogy: Global Restaurant Chain Operations

**Scenario**: International restaurant chain with distributed kitchens, supply chain, and dynamic menu optimization

### Components Mapping
- **Spark Cluster** = Restaurant Chain Network
- **Partitioning** = Regional Kitchen Distribution
- **Repartition/Coalesce** = Staff Redistribution During Peak/Off Hours
- **Shuffle Operations** = Ingredient Exchange Between Kitchens
- **Bucketing** = Pre-organized Ingredient Storage by Recipe Type
- **Data Skew** = Some Locations Getting Overwhelmed While Others Idle
- **Joins** = Combining Ingredients from Different Suppliers
- **Broadcast Variables** = Corporate Recipe Book Distributed to All Locations
- **Caching** = Keeping Popular Ingredients Ready in Each Kitchen
- **File Formats** = Different Storage Methods for Ingredients

### How It Works

**Partitioning Strategies**: 
- **Hash Partitioning**: Randomly distribute customers across restaurants (even load)
- **Range Partitioning**: Group restaurants by region (West Coast, East Coast)  
- **Custom Partitioning**: Specialized restaurants (Italian, Asian, Fast Food)

**Repartition vs Coalesce**:
- **Repartition**: Complete staff reshuffling during season changes (expensive but thorough)
- **Coalesce**: Combining nearby restaurants during slow periods (efficient downsizing)

**Shuffle Operations**: When making fusion dishes, ingredients need to move between different specialized kitchens - very expensive operation requiring coordination and transport.

**Data Skew**: Times Square location gets 90% of customers while suburban locations sit empty - need to redistribute load or handle specially.

**Bucketing**: Pre-organize ingredients by recipe type (appetizer ingredients, main course, desserts) so chefs know exactly where to find what they need without searching.

**Join Types**:
- **Broadcast Join**: Send popular sauce recipe to all restaurants (small data to everywhere)
- **Sort-Merge Join**: Two supply trucks arriving simultaneously, sorted by ingredient type
- **Hash Join**: Quick ingredient lookup using organized storage system

**Broadcast Variables**: Corporate sends standardized recipe book to all locations once, rather than sending individual recipes with each order.

**Caching**: Keep frequently ordered ingredients (potatoes, onions, bread) readily available in each kitchen rather than going to warehouse each time.

**File Formats & Compression**:
- **Parquet**: Vacuum-sealed ingredient packages (great compression, organized by type)
- **Delta Lake**: Inventory system with transaction logs (know