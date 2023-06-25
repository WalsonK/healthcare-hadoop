import healthcare_spark_analysis

if __name__ == '__main__':
    healthcare_instance = healthcare_spark_analysis.Healthcare()
    healthcare_instance.get_data("train_data.csv")
    healthcare_instance.healthcare_data.show(5)
    print(healthcare_instance.get_mean_by_column_in_hospital(8, "Available Extra Rooms in Hospital"))
    # print(healthcare_instance.get_stats_by_column(8, 'Department'))