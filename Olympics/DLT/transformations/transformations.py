from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr


## creating LDPL for coaches
@dp.table
def source_table_coaches():
    df = spark.readStream.table('olympics.silver.silver_coaches')
    return df
@dp.view
def view_coaches():
    df = spark.readStream.table('live.source_table_coaches')
    df = df.fillna('unknown')
    return df
@dp.table
def coaches():
    df = spark.readStream.table('live.view_coaches')
    return df

## creating LDPL for events
@dp.table
def source_table_events():
    df = spark.readStream.table('olympics.silver.silver_events')
    return df
@dp.view
def view_events():
    df = spark.readStream.table('live.source_table_events')
    df = df.fillna('unknown')
    return df
@dp.table
def events():
    df = spark.readStream.table('live.view_events')
    return df

## creating LDPL for nocs
@dp.table
def source_table_nocs():
    df = spark.readStream.table('olympics.silver.silver_nocs')
    return df
@dp.view
def view_nocs():
    df = spark.readStream.table('live.source_table_nocs')
    df = df.fillna('unknown')
    return df
@dp.table
def nocs():
    df = spark.readStream.table('live.view_nocs')
    return df
## AutoCDC for Athelets
@dp.view
def source_athletes():
    df = spark.readStream.table('olympics.silver.silver_athletes')
    return df

dp.create_streaming_table('athletes')
dp.create_auto_cdc_flow(
  target = "athletes",
  source = "source_athletes",
  keys = ["athlete_id"],
  sequence_by = col("height"),
  stored_as_scd_type = "2"
)

