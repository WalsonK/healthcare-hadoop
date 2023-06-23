import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


class Healthcare:
    def __init__(self):
        self.spark_session = SparkSession.builder.master("local").appName("healthcare").getOrCreate()
        self.path = "hdfs://localhost:9000/user/walson/healthcare/"

    def get_data(self, filename):
        schema_dictionary = {"train_data.csv": (StructType()
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
                             }

        healthcare_data = self.spark_session.read.csv(self.path + filename, schema=schema_dictionary.get(filename),
                                                      header=True)

        healthcare_data.show(5)
