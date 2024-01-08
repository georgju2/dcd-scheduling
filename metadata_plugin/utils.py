import functools
import csv
import os
from airflow.models import Variable
from datetime import datetime

# Define where the metadata files are stored
METADATA_PATH = "/home/ubuntu/airflow/metadata"
if not os.path.exists(METADATA_PATH):
    os.makedirs(METADATA_PATH)

# Valid metadata attributes
ALLOWED_METADATA_ATTRIBUTES = ["category", "desc", "dependency", "source_db","target_db", "execution_strategy", "queue_priority", "upstream", "downstream", "failure_contact", "special_fields", "long_running_task", "data_sensitivity_level", "exceptions", "sla", "last_modified_by", "log_performance"]

# Validation for categories allowed
ALLOWED_CATEGORIES = ["LoadToCSV", "SalesTransform", "ShippingTransform", "ExtractFromAWS", "LoadToReportDB", "ExtractFromDB", "ExtractFromCSV", "SQLTransform", "FooBarTransform", "LoadToCloud"]


def annotate(**kwargs):
    for key in kwargs:
        if key not in ALLOWED_METADATA_ATTRIBUTES:
            raise ValueError(f"Invalid metadata attribute: {key}")
        if key == "category" and kwargs[key] not in ALLOWED_CATEGORIES:
            raise ValueError(f"Invalid category value: {kwargs[key]}")
    
    def decorator(func):
        metadata = {key: kwargs.get(key, "") for key in ALLOWED_METADATA_ATTRIBUTES}
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            # Fetch the dag_id from Airflow's Variables
            dag_id = Variable.get("dag_id", default_var=None)

            # Save the annotation to CSV when the task is executed
            save_annotation_to_csv(dag_id, func.__name__, metadata)
            return func(*args, **kwargs)
        

        wrapper._annotation = metadata
        return wrapper
    
    return decorator


def save_annotation_to_csv(dag_id, task_name, metadata):
    """
    Save the given metadata to a CSV file based on the DAG name and the current datetime.
    """
    # Set the unique filename variable
    metadata_filename = f"{dag_id}_{datetime.now().strftime('%Y_%m_%d_%H_%M')}.csv"

    # Construct a unique filename based on dag_id and the current datetime
    csv_file = os.path.join(METADATA_PATH, metadata_filename)

    with open(csv_file, 'a') as csvfile:
        formatted_metadata = "\n".join([f'{key}: {value}' for key, value in metadata.items() if value])
        csvfile.write(f'task: {task_name}\n{formatted_metadata}\n\n----------------------------------------------\n\n')


def get_annotations(task):
    if hasattr(task, 'python_callable'):
        callable_ = task.python_callable
        return getattr(callable_, "_annotation", None)
    return None


def extract_annotations_from_tasks(task_list):
    """
    Extracts metadata from the list of tasks.
    """
    metadata_list = []
    for task in task_list:
        metadata = get_annotations(task)
        if metadata:
            formatted_metadata = "<br>".join([f'{key}: {value}' for key, value in metadata.items() if value])
            metadata_entry = f'task: {task.task_id}<br>{formatted_metadata}'
            metadata_list.append(metadata_entry)
    # Add a break between each task's metadata
    return ["<br>-------------------------------<br><br>" + item for item in metadata_list]


def set_annotations(taskgroup, tasks):
    """
    Set annotations from the provided tasks to the given TaskGroup's tooltip.

    Parameters:
    - taskgroup (TaskGroup): The TaskGroup to which annotations should be set.
    - tasks (list): List of tasks from which to extract annotations.
    """

    # Store the dag_id to Airflow's Variables
    Variable.set("dag_id", taskgroup.dag.dag_id)

    annotations = extract_annotations_from_tasks(tasks)
    taskgroup.tooltip = "<br>".join(annotations)

