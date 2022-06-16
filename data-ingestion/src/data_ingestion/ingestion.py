from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *

# ---------- Part II: Business Logic (for Part I, see data_ingestion/config.py) ---------- #

class Ingester:
    """Data Ingester Python Class"""

    def __init__(self, spark: SparkSession, parameters: "dict[str, str]"):
        self.spark = spark
        self.parameters = parameters
        return

    @staticmethod
    def replace_invalid_chars(column_name: str) -> str:
        """Replace prohibited characters in column names to be compatible with Apache Parquet"""
        INVALID_CHARS = [" ", ",", ";", "\n", "\t", "=", "-", "{", "}", "(", ")"]
        UNDERSCORE_CANDIDATES = [" ", ",", ";", "\n", "\t", "=", "-"] # let's replace these with underscores

        for und_char in UNDERSCORE_CANDIDATES:
            if und_char in column_name:
                column_name = column_name.replace(und_char, '_')
        for inv_char in INVALID_CHARS:
            if inv_char in column_name:
                column_name = column_name.replace(inv_char, '')
        return column_name

    def fix_columns(self, df: DataFrame) -> DataFrame:
        """Clean up a Spark DataFrame's column names"""
        # HINT: you don't need to do use .withColumnRenamed a dozen times - one-liner solution possible ;)
        newColumns = [Ingester.replace_invalid_chars(x) for x in df.columns]
        fixed_df = df.toDF(*newColumns)
        return fixed_df

    def run(self) -> None:
        """
        You can of course reduce the code repetition shown below.
        However, this is a clear way for a beginner to see what the job is doing.
        """
        kwargs = {"format": "csv", "sep": ",", "inferSchema": "true", "header": "true"}

        co2_df = self.spark.read.load(self.parameters["co2_input_path"], **kwargs) # kwargs = keyword arguments. Python pro tip ;)
        co2_fixed = self.fix_columns(co2_df).coalesce(1)
        co2_fixed.write.format("parquet").mode("overwrite").save(self.parameters["co2_output_path"])

        country_temps_df = self.spark.read.load(self.parameters["temperatures_country_input_path"], **kwargs)
        country_temps_fixed = self.fix_columns(country_temps_df).coalesce(1)
        country_temps_fixed.write.format("parquet").mode("overwrite").save(self.parameters["temperatures_country_output_path"])

        global_temps_df = self.spark.read.load(self.parameters["temperatures_global_input_path"], **kwargs)
        global_temps_fixed = self.fix_columns(global_temps_df).coalesce(1)
        global_temps_fixed.write.format("parquet").mode("overwrite").save(self.parameters["temperatures_global_output_path"])

        # Today's your lucky day! Schema Inference detects the "Date" column in global_temps_df as TimestampType
        # You might not always be this lucky though, so passing explicit schemas to the load function might be preferred sometimes
        return
