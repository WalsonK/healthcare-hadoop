import healthcare_spark_analysis

if __name__ == '__main__':
    healthcare_instance = healthcare_spark_analysis.Healthcare()
    healthcare_instance.get_data("train_data.csv")
    print(healthcare_instance.get_distinct_values("Department"))
