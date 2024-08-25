from langchain_openai.chat_models import ChatOpenAI
from langchain_community.agent_toolkits import SparkSQLToolkit, create_spark_sql_agent
from langchain_community.utilities.spark_sql import SparkSQL
from pyspark.sql import SparkSession
import glob
import shutil
import os

class PysparkAgent:
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.schema = "gdelt"
        if os.path.exists('./spark-warehouse'):
            shutil.rmtree('./spark-warehouse')
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.schema}")
        self.spark.sql(f"USE {self.schema}")
        self.spark.sql(f"USE {self.schema}")
        self.gdelt_data = "./files"
        self.cameo_data = "./files/cameo.csv"
        self.data = self.load_data_on_spark()
        self.agent_executor = self.get_agent_executor()

    def load_data_on_spark(self):
        gkg_files = glob.glob(self.gdelt_data + '/**/*export.CSV', recursive=True)
        csv_files = [file for file in gkg_files if file.lower().endswith('.csv')]
        events_df = self.spark.read.csv(csv_files, sep='\t', header=True, inferSchema=True)
        cameo_df = self.spark.read.csv(self.gdelt_data, sep='\t', header=True, inferSchema=True)
        cameo_df = cameo_df.withColumnRenamed("EventCode", "EventCode_join")
        merged_df = events_df.join(cameo_df, events_df['EventCode'] == cameo_df['EventCode_join'], "left")
        merged_df = merged_df.drop("EventCode_join")
        merged_df.write.saveAsTable("events")
        return merged_df

    def get_agent_executor(self):
        spark_sql = SparkSQL(schema=self.schema)
        llm = ChatOpenAI(temperature=0, model="gpt-4o-mini")
        toolkit = SparkSQLToolkit(db=spark_sql, llm=llm)
        agent_executor = create_spark_sql_agent(llm=llm, toolkit=toolkit, max_iterations=10)
        return agent_executor
