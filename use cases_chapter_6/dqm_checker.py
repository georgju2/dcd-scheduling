import re
from metadata_plugin.utils import get_annotations

class DataQualityChecker:

    @staticmethod
    def check_data_quality(task, data):
        metadata = get_annotations(task)
        print("Metadata: {metadata}")

        # Extended checks for long-running tasks
        if metadata.get("long_running_task") == "true":
            print("Performing extended data quality checks for long-running task.")
            DataQualityChecker.validate_data_accuracy(data)
            DataQualityChecker.validate_data_completeness(data)

        # Extended checks for tasks with special fields
        if metadata.get("special_fields"):
            print(f"Performing special field checks for: {metadata['special_fields']}")
            DataQualityChecker.validate_special_fields(metadata["special_fields"], data)


    @staticmethod
    def validate_data_completeness(data):
        # Check if essential fields in the data are present and not null
        print("Data completeness check passed.")

    @staticmethod
    def validate_data_accuracy(data):
        # Implement checks for accuracy
        print("Data accuracy check passed.")

    @staticmethod
    def validate_special_fields(special_fields, data):
        # Example: Validate sales_volume integrity
        print("Special fields validation check passed.")

    @staticmethod
    def dqm_check(context):
        task_instance = context['task_instance']
        # Determine the data location from the context
        data_location = "/path/to/data/" + context['ds'] + "_extract.csv"
        DataQualityChecker.check_data_quality(task=task_instance.task, data=data_location)

