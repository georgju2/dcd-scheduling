from airflow.plugins_manager import AirflowPlugin

from metadata_plugin.decorators import annotate
from metadata_plugin.utils import get_annotations

class MetadataPlugin(AirflowPlugin):
    name = "metadata_plugin"
    operators = []
    hooks = []
    executors = []
    macros = [annotate]
    admin_views = []
    flask_blueprints = []
    menu_links = []

