from metadata_plugin.utils import get_annotations

class ExecutionManager:

    @staticmethod
    def determine_execution_strategies(metadata):
        """
        Determines the execution strategy for a task based on its annotations.
        """
        strategy = metadata.get('execution_strategy', 'default_sitrategy')
        action = None
        print(f"Strategy from task: {strategy}")

        # Define actions based on strategy
        if strategy == 'in_order':
            action = 'wait_for_prior_tasks'
        elif strategy == 'in_parallel':
            action = 'proceed_without_waiting'
        elif strategy == 'latest_only':
            action = 'skip_if_newer_exists'
        else:
            action = 'default_action'

        return action

    @staticmethod
    def handle_task_restart(context):
        """
        Handler called when a task fails and needs to restart.
        """
        task_instance = context['task_instance']
        metadata = get_annotations(task_instance.task)
        
        if not metadata:
            return
        else:
            action = ExecutionManager.determine_execution_strategies(metadata)
            print(f"Action determined for retry based on strategy: {action}")
            return action


