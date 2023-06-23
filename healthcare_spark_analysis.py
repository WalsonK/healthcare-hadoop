import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def get_data():
    spark_session = SparkSession.builder.master("local").appName("healthcare").getOrCreate()

    healthcare_schema = (StructType()
                         .add("case_id", "integer")
                         .add("Hospital_code", "integer")
                         .add("Hospital_type_code", "string")
                         .add("City_Code_Hospital", "integer")
                         .add("Hospital_region_code", "string")
                         .add("Available Extra Rooms in Hospital", "integer")
                         .add("Department", "string")
                         .add("Ward_Type", "string")
                         .add("Ward_Facility_Code", "string")
                         .add("Bed Grade", "float")
                         .add("patientid", "integer")
                         .add("City_Code_Patient", "float")
                         .add("Type of Admission", "string")
                         .add("Severity of Illness", "string")
                         .add("Visitors with Patient", "integer")
                         .add("Age", "string")
                         .add("Admission_Deposit", "float")
                         .add("Stay", "string")
                         )

    healthcare_data = spark_session.read.csv("hdfs://localhost:9000/user/walson/healthcare/train_data.csv",
                                             schema=healthcare_schema, header=True)

    healthcare_data.show(5)
