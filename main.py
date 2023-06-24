import healthcare_spark_analysis

if __name__ == '__main__':
    healthcare_instance = healthcare_spark_analysis.Healthcare()
    healthcare_instance.get_data("train_data.csv")
    # healthcare_instance.healthcare_data.show(5)
    print(healthcare_instance.get_max("City_Code_Hospital"))
