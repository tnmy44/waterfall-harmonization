with DAG():
    all_cols = Task(
        task_id = "all_cols", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "all_cols", "sourceName" : "waterfall_harmonizer_cdm", "sourceType" : "Table"}
    )
