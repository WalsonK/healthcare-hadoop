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
        assert self.healthcare_is_present()
        datas = self.healthcare_data.select(column_name).distinct().collect()
        return [datas[i][0] for i in range(len(datas))]

    def get_max(self, column_name):
        assert self.healthcare_is_present()
        return self.healthcare_data.select(column_name).agg({column_name: "max"}).collect()[0][0]

    def get_min(self, column_name):
        assert self.healthcare_is_present()
        return self.healthcare_data.select(column_name).agg({column_name: "min"}).collect()[0][0]

    def get_mean(self, column_name):
        assert self.healthcare_is_present()
        return self.healthcare_data.select(column_name).agg({column_name: "mean"}).collect()[0][0]

    def healthcare_is_present(self):
        return self.healthcare_data is not None
