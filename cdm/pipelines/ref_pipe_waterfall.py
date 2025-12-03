with DAG():
    gsmbs_expected_output = Task(
        task_id = "gsmbs_expected_output", 
        component = "Dataset", 
        table = {"name" : "gsmbs_expected_output", "sourceType" : "Table", "sourceName" : "waterfall_harmonizer_cdm", "alias" : ""}
    )
    ref_pipe_waterfall__sqlstatement_1 = Task(
        task_id = "ref_pipe_waterfall__sqlstatement_1", 
        component = "Model", 
        modelName = "ref_pipe_waterfall__sqlstatement_1"
    )
