import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


class Healthcare:
    def __init__(self):
        self.spark_session = SparkSession.builder.master("local").appName("healthcare").getOrCreate()
        self.path = "hdfs://localhost:9000/user/walson/healthcare/"
        self.healthcare_data = None

    def get_data(self, filename):
        file_schema = (StructType()
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

        self.healthcare_data = self.spark_session.read.csv(self.path + filename, schema=file_schema, header=True)

    def get_distinct_values(self, column_name):
        if self.healthcare_data is None:
            raise AttributeError("Healthcare_data undefined")
        return self.healthcare_data.select(column_name).distinct().collect()
