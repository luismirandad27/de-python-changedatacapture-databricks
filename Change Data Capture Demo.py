# Databricks notebook source
# MAGIC %md
# MAGIC ## Change Data Capture (CDC)

# COMMAND ----------

# MAGIC %md
# MAGIC *Author: Luis Miguel Miranda*

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/luismirandad27/de-python-changedatacapture-databricks/main/assets/CDC_Architecture.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Dropping Tables - Just in Case :)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS games_raw;
# MAGIC DROP TABLE IF EXISTS games_silver;

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/cdc_demo/checkpoints/games_silver",True)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's create a table in the **Bronze Layer** `games_raw`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS games_raw
# MAGIC (
# MAGIC   row_id INT,
# MAGIC   row_json STRING,
# MAGIC   row_topic STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC Inserting some initial data on `games_raw`

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO games_raw VALUES
# MAGIC (1,"{
# MAGIC   'game_id': '0001', 'title': 'The Legend of Zelda: Breath of the Wild',
# MAGIC   'company': 'Nintendo',
# MAGIC   'platform': ['Nintendo Switch'],
# MAGIC   'row_action': 'insert',
# MAGIC   'row_time': '2022-04-30T10:20:00.000Z'
# MAGIC  }","games"),
# MAGIC (2,"{
# MAGIC   'game_id': '0002',
# MAGIC   'title': 'Halo Infinite',
# MAGIC   'company': '343 Industries',
# MAGIC   'platform': ['Xbox Series X\/S', 'Xbox One', 'PC'],
# MAGIC   'row_action': 'insert',
# MAGIC   'row_time': '2022-04-30T10:20:00.000Z'
# MAGIC  }","games"),
# MAGIC (3,"{
# MAGIC   'game_id': '0003',
# MAGIC   'title': 'God of War',
# MAGIC   'company': 'Santa Monica Studio',
# MAGIC   'platform': ['PlayStation 4', 'PlayStation 5'],
# MAGIC   'row_action': 'insert',
# MAGIC   'row_time': '2022-04-30T10:20:00.000Z'
# MAGIC  }","games"),
# MAGIC (4,"{
# MAGIC   'game_id': '0004',
# MAGIC   'title': 'Minecraft',
# MAGIC   'company': 'Mojang Studios',
# MAGIC   'platform': ['PC', 'Xbox', 'PlayStation', 'Nintendo Switch'],
# MAGIC   'row_action': 'insert',
# MAGIC   'row_time': '2022-04-30T10:20:00.000Z'
# MAGIC  }","games"),
# MAGIC (5,"{
# MAGIC   'game_id': '0005',
# MAGIC   'title': 'Fortnite',
# MAGIC   'company': 'Epic Games',
# MAGIC   'platform': ['PC', 'Xbox', 'PlayStation', 'Nintendo Switch'],
# MAGIC   'row_action': 'insert',
# MAGIC   'row_time': '2022-04-30T10:20:00.000Z'
# MAGIC  }","games"),
# MAGIC (6,"{
# MAGIC   'game_id': '0006',
# MAGIC   'title': 'Call of Duty: Warzone',
# MAGIC   'company': 'Activision',
# MAGIC   'platform': ['PC', 'Xbox','PlayStation'],
# MAGIC   'row_action': 'insert',
# MAGIC   'row_time': '2022-04-30T10:20:00.000Z'
# MAGIC  }","games"),
# MAGIC (7,"{
# MAGIC   'game_id': '0007',
# MAGIC   'title': 'Grand Theft Auto V',
# MAGIC   'company': 'Rockstar North',
# MAGIC   'platform': ['PC', 'Xbox', 'PlayStation'],
# MAGIC   'row_action': 'insert',
# MAGIC   'row_time': '2022-04-30T10:20:00.000Z'
# MAGIC  }","games"),
# MAGIC (8,"{
# MAGIC   'game_id': '0008',
# MAGIC   'title': 'Animal Crossing: New Horizons',
# MAGIC   'company': 'Nintendo',
# MAGIC   'platform': ['Nintendo Switch'],
# MAGIC   'row_action': 'insert',
# MAGIC   'row_time': '2022-04-30T10:20:00.000Z'
# MAGIC  }","games"),
# MAGIC (9,"{
# MAGIC   'game_id': '0009',
# MAGIC   'title': 'Resident Evil Village',
# MAGIC   'company': 'Capcom',
# MAGIC   'platform': ['PC', 'Xbox', 'PlayStation'],
# MAGIC   'row_action': 'insert',
# MAGIC   'row_time': '2022-04-30T10:20:00.000Z'
# MAGIC  }","games"),
# MAGIC (10,"{
# MAGIC   'game_id': '0010',
# MAGIC   'title': 'Cyberpunk 2077',
# MAGIC   'company': 'CD Projekt RED',
# MAGIC   'platform': ['PC', 'Xbox', 'PlayStation'],
# MAGIC   'row_action': 'insert',
# MAGIC   'row_time': '2022-04-30T10:20:00.000Z'
# MAGIC  }","games");

# COMMAND ----------

# MAGIC %md
# MAGIC This method will be attached (executed) on every microbatch that is going to be performed in the streaming from the bronze to silver layer.
# MAGIC
# MAGIC P.D: To run the query in the microbatch, we have to use the `sparkSession.sql()` method.

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Defining method to add at every microbatch
def batch_merge_operation_cdc(microBatchDF, batchID):
    
    # Creating window operation
    window_operation = Window.partitionBy("game_id").orderBy(F.col("row_time").desc())

    # Creating temporary view from bronze layer with the updates ranked
    (microBatchDF.filter(F.col("row_action").isin(['insert','update']))
                .withColumn("action_rank",F.rank().over(window_operation))
                .filter("action_rank == 1")
                .drop("action_rank")
                .createOrReplaceTempView("games_raw_ranked_updates")
    )

    # Performing MERGE operation
    merge_query = """
        MERGE INTO games_silver a
        USING games_raw_ranked_updates b 
        ON a.game_id = b.game_id
            WHEN MATCHED AND a.row_time < b.row_time
                THEN UPDATE SET *
            WHEN NOT MATCHED
                THEN INSERT *
    """

    microBatchDF.sparkSession.sql(merge_query)

# COMMAND ----------

# MAGIC %md
# MAGIC Creating the games table in the **Silver Layer** `games_silver`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS games_silver 
# MAGIC (
# MAGIC   game_id STRING,
# MAGIC   title STRING,
# MAGIC   company STRING,
# MAGIC   platform ARRAY<STRING>,
# MAGIC   row_time TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC In this method, let's create the stream query with the following actions:
# MAGIC - Reading the `games_raw` table and filtering for the topic `games`.
# MAGIC - While writing through the stream, every microbatch is going to execution the `batch_merge_operation_cdc` method.
# MAGIC - The `availableNow` allows us to process all the data available with multiple batches.

# COMMAND ----------

# Performing Streaming from Bronze to Silver layer
def process_data_to_silver():
    
    schema = "game_id STRING, title STRING, company STRING, platform Array<STRING>, row_action STRING, row_time TIMESTAMP"

    stream_query = (
        spark.readStream.table("games_raw")
            .filter("row_topic == 'games'")
            .select(F.from_json(F.col("row_json"),schema).alias("v"))
            .select("v.*")
            .writeStream
            .foreachBatch(batch_merge_operation_cdc)
            .option("checkpointLocation","dbfs:/mnt/cdc_demo/checkpoints/games_silver")
            .trigger(availableNow=True)
            .start()
    )

    stream_query.awaitTermination()

process_data_to_silver()

# COMMAND ----------

# MAGIC %md
# MAGIC Verify the data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM games_silver ORDER BY game_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Let's add some modifications. Notice that the `row_action` include the value *update*.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO games_raw VALUES
# MAGIC (11,"{
# MAGIC   'game_id': '0003',
# MAGIC   'title': 'God of War (2018)',
# MAGIC   'company': 'Santa Monica Studio',
# MAGIC   'platform': ['PlayStation 4', 'PlayStation 5'],
# MAGIC   'row_action': 'update',
# MAGIC   'row_time': '2022-05-01T08:17:00.000Z'
# MAGIC  }","games"),
# MAGIC  (12,"{
# MAGIC   'game_id': '0009',
# MAGIC   'title': 'Resident Evil 8: Village',
# MAGIC   'company': 'Capcom',
# MAGIC   'platform': ['PC', 'Xbox', 'PlayStation 4','PlayStation 5'],
# MAGIC   'row_action': 'insert',
# MAGIC   'row_time': '2022-05-01T10:20:00.000Z'
# MAGIC  }","games");

# COMMAND ----------

# MAGIC %md
# MAGIC Proccessing again the data

# COMMAND ----------

process_data_to_silver()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM games_silver 
# MAGIC WHERE game_id IN ('0003','0009');

# COMMAND ----------

# MAGIC %md
# MAGIC #### Activating CDF (Change Data Feed)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE games_silver
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %md
# MAGIC With the `DESCRIBE EXTENDED` statement we can verify the activation of the Change Data Feed feature.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED games_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC Checking the multiple versions of `games_silver`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY games_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC Let's modify another row

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO games_raw VALUES
# MAGIC (13,"{
# MAGIC   'game_id': '0010',
# MAGIC   'title': 'Cyberpunk 2077',
# MAGIC   'company': 'CD Projekt RED',
# MAGIC   'platform': ['PC', 'Xbox', 'PlayStation 4','Playstation 5'],
# MAGIC   'row_action': 'update',
# MAGIC   'row_time': '2022-05-02T10:25:00.000Z'
# MAGIC  }","games");

# COMMAND ----------

process_data_to_silver()

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, with the `table_changes` table we can verify the changes we performed.
# MAGIC
# MAGIC Notice the column `_change_type`:
# MAGIC - `update_preimage`: contains the values before the latest update
# MAGIC - `update_postimage`: contains the values after the latest update
# MAGIC
# MAGIC The `_commit_version` is associated to the version where we perfomed the latest update. (You can use `DESCRIBE HISTORY` as well).

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY games_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM table_changes("games_silver", 4)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Follow me on:
# MAGIC - GitHub: https://github.com/luismirandad27 
# MAGIC - Medium: https://medium.com/@lmirandad27
# MAGIC - LinkedIn: https://www.linkedin.com/in/lmirandad27/
