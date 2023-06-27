import healthcare_spark_analysis

if __name__ == '__main__':
    healthcare_instance = healthcare_spark_analysis.Healthcare()
    healthcare_instance.get_data("train_data.csv")
    # healthcare_instance.healthcare_data.show(5)
    # print(healthcare_instance.get_min_by_column_in_hospital(8, "Age"))
    healthcare_instance.add_columns_based_on_split("Age")
    healthcare_instance.add_columns_based_on_split("Stay")
    healthcare_instance.healthcare_data.show()
    print(healthcare_instance.get_column_types())
