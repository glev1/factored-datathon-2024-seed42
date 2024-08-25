import os
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import SparkSession

from pyspark.sql import SparkSession

from pyspark.sql.window import Window
from pyspark.sql import functions as F


current_dir = os.path.dirname(os.path.abspath(__file__))
models_dir = os.path.join(current_dir, 'models')
model_path = os.path.join(models_dir, 'final_model')

# Initialize Spark session
spark = SparkSession.builder.appName("SanctionsPrediction").getOrCreate()

# Define the sanction codes
sanction_codes = [
    '1312', # Threaten to boycott, embargo, or sanction
    '132',  # Threaten with administrative sanctions, not specified below
    '163',  # Impose embargo, boycott, or sanctions
    '172',  # Impose administrative sanctions, not specified below
    '1241', # Refuse to ease administrative sanctions
    '1244'  # Refuse to ease economic sanctions, boycott, or embargo
]


def preprocess(data):
    # Convert incoming data to Spark DataFrame
    df = spark.createDataFrame(data)

    # Validate if 'EventCode' column is present
    if 'EventCode' not in df.columns:
        raise ValueError("The 'EventCode' column is missing from the input data.")

    # Create a column to verify if the country had a previous sanction
    df = df.orderBy(['Actor2CountryCode', 'Date'])

    # Create a window specification that defines how to look at previous rows within the same target country
    window_spec = Window.partitionBy('Actor2CountryCode').orderBy('Date')

    # Create a lagged column that checks if there was a previous sanction event for the target country
    df = df.withColumn(
        'PreviousSanction',
        F.lag('EventCode').over(window_spec)
    )

    # Create a binary flag 'HadPreviousSanction' that indicates if the target country had been sanctioned before
    df = df.withColumn(
        'HadPreviousSanction',
        F.when(F.col('PreviousSanction').isNotNull(), 1).otherwise(0)
    )

    # Select relevant columns
    selected_columns = [
    #	'Actor1CountryCode', 'Actor2CountryCode', #TODO add longitude and latitude
        'Date',
        'EventRootCode', 'QuadClass', 'NumMentions', 'NumSources',
        'NumArticles', 'AvgTone', 'EventCode', 'GoldsteinScale',
    ]

    df = df.select(selected_columns).dropna(subset=['GoldsteinScale'])

    # 1. Create a binary column 'SanctionAction' to indicate if the event is imposing or threatening sanctions
    df = df.withColumn(
        'SanctionActionImpose', 
        F.when(df['EventCode'].isin('163', '172'), 1)
        .otherwise(0)
    )

    # 1. Create a binary column 'SanctionAction' to indicate if the event is imposing or threatening sanctions
    df = df.withColumn(
        'SanctionActionThreaten', 
        F.when(df['EventCode'].isin('1312', '132'), 1)
        .otherwise(0)
    )

    # 3. Calculate the 'TotalMediaCoverage' by summing 'NumMentions', 'NumSources', and 'NumArticles'
    df = df.withColumn(
        'TotalMediaCoverage',
        df['NumMentions'] + df['NumSources'] + df['NumArticles']
    )

    # 3. Calculate the 'TotalMediaCoverage' by summing 'NumMentions', 'NumSources', and 'NumArticles'
    df = df.withColumn(
        'VerbalConflict',
        F.when(df['QuadClass'] == 3, 1).otherwise(0)
    )

    # 3. Calculate the 'TotalMediaCoverage' by summing 'NumMentions', 'NumSources', and 'NumArticles'
    df = df.withColumn(
        'MaterialConflict',
        F.when(df['QuadClass'] == 4, 1).otherwise(0)
    )

    # Assemble features into a vector
    feature_columns = [col for col in df.columns if col != "GoldsteinScale"]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    df = assembler.transform(df)

    return df

def postprocess(prediction):
    # Implement any postprocessing steps here
    return {"prediction": prediction[0]}

def predict(data):
    # Preprocess the data
    df = preprocess(data)

    final_model = GBTRegressor.load(models_dir)

    # Make predictions
    predictions = final_model.transform(df)

    # Select relevant columns
    result = predictions.select("prediction", "GoldsteinScale", "features")
    return result


if __name__ == "__main__":
    
    final_model = GBTRegressor.load(model_path)

    print("Model loaded successfully")