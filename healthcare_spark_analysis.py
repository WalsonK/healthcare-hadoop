import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, expr, split


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

    def get_distinct_values(self, column_name, datas):
        assert self.healthcare_is_present()
        datas = datas.select(column_name).distinct()
        return [datas[i][0] for i in range(len(datas))]

    def get_max(self, column_name):
        assert self.healthcare_is_present()
        return self.healthcare_data.select(column_name).agg({column_name: "max"}).show()

    def get_min(self, column_name):
        assert self.healthcare_is_present()
        return self.healthcare_data.select(column_name).agg({column_name: "min"}).show()

    def get_mean(self, column_name):
        assert self.healthcare_is_present()
        return self.healthcare_data.select(column_name).agg({column_name: "mean"}).show()

    def healthcare_is_present(self):
        return self.healthcare_data is not None

    def get_column_by_hospital(self, hospital_code, column_name):
        assert self.healthcare_is_present()
        return self.get_hospital_informations(hospital_code).select(column_name)

    def get_hospital_informations(self, hospital_code):
        assert self.healthcare_is_present()
        return self.healthcare_data.filter(self.healthcare_data.Hospital_code == hospital_code)

    def get_distinct_values_by_hospital(self, hospital_code, column_name):
        assert self.healthcare_is_present()
        datas = self.get_column_by_hospital(hospital_code, column_name)
        return self.get_distinct_values(column_name, datas)

    def get_min_by_column_in_hospital(self, hospital_code, column_name):
        assert self.healthcare_is_present()
        datas = self.get_column_by_hospital(hospital_code, column_name)
        return datas.agg({column_name: "min"}).show()

    def get_max_by_column_in_hospital(self, hospital_code, column_name):
        assert self.healthcare_is_present()
        datas = self.get_column_by_hospital(hospital_code, column_name)
        return datas.agg({column_name: "max"}).show()

    def get_mean_by_column_in_hospital(self, hospital_code, column_name):
        assert self.healthcare_is_present()
        datas = self.get_column_by_hospital(hospital_code, column_name)
        return datas.agg({column_name: "mean"}).show()

    def get_stats_by_column(self, hospital_code, column_name):
        assert self.healthcare_is_present()
        datas = self.get_column_by_hospital(hospital_code, column_name)
        dictionary = {}
        for key in datas:
            dictionary[key[0]] = dictionary.get(key[0], 0) + 1

        return dict(sorted(dictionary.items(), key=lambda item: item[1], reverse=True))

    def get_stay_by_admission(self, hospital_code):
        assert self.healthcare_is_present()
        datas = self.get_hospital_informations(hospital_code)

        def transform_stay(value):
            start, end = value.split('-')
            return list(range(int(start), int(end) + 1))

        transform_stay_udf = udf(transform_stay, IntegerType())

        df = datas.withColumn('Stay', transform_stay_udf(col('Stay')))

        median_by_admission = df.groupBy('Type of Admission').agg(
            expr('percentile_approx(Stay, 0.5)').alias('Median_Stay'))

        median_by_admission.show()

        return datas.groupBy("Stay")

    def get_column_types(self):
        assert self.healthcare_is_present()
        return self.healthcare_data.dtypes

    def get_stats_by_hospital(self, hospital_code):
        assert self.healthcare_is_present()
        hospital = self.get_hospital_informations(hospital_code)

    def add_columns_based_on_split(self, column, sep='-', cast_type='integer'):
        assert self.healthcare_is_present()
        splits = split(self.healthcare_data[column], sep)
        self.healthcare_data = self.healthcare_data.withColumn(f'{column}Min', splits.getItem(0))
        self.healthcare_data = self.healthcare_data.withColumn(f'{column}Min', col(f'{column}Min').cast(cast_type))
        self.healthcare_data = self.healthcare_data.withColumn(f'{column}Max', splits.getItem(1))
        self.healthcare_data = self.healthcare_data.withColumn(f'{column}Max', col(f'{column}Max').cast(cast_type))
        self.healthcare_data = self.healthcare_data.withColumn(f'{column}Mean', (col(f'{column}Min') + col(f'{column}Max')) / 2.0)
        self.healthcare_data = self.healthcare_data.drop(column)
