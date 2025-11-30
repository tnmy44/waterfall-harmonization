with DAG():
    gsmbs_raw_source = Task(
        task_id = "gsmbs_raw_source", 
        component = "Dataset", 
        table = {"name" : "gsmbs_raw_source", "sourceType" : "Table", "sourceName" : "waterfall_harmonizer_cdm", "alias" : ""}
    )
    gsmbs_expected_output = Task(
        task_id = "gsmbs_expected_output", 
        component = "Dataset", 
        table = {"name" : "gsmbs_expected_output", "sourceType" : "Table", "sourceName" : "waterfall_harmonizer_cdm", "alias" : ""}
    )
