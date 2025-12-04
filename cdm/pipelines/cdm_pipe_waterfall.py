Schedule = Schedule(cron = "* 0 2 * * * *", timezone = "GMT", emails = ["email@gmail.com"], enabled = False)
SensorSchedule = SensorSchedule(enabled = False)

with DAG(Schedule = Schedule, SensorSchedule = SensorSchedule):
    all_cols = Task(
        task_id = "all_cols", 
        component = "Dataset", 
        table = {
          "name": "all_cols", 
          "sourceType": "Table", 
          "sourceName": "waterfall_harmonizer_cdm1", 
          "alias": "", 
          "additionalProperties": None
        }
    )
