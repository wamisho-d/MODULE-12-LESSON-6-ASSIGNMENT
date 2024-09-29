from typing import List, Dict, Any
# Define the structure of a task using type hints
Task = Dict[str, Any]

def schedule_tasks(task_hierarchy: Task) -> List[Task]:
    """
    Function to schedule tasks based on the hierarchy and priority.
    Each task will be executed after its subtasks and based on priority.

    :param task_hierarchy: The nested task hierarchy.
    :return: A flat list of tasks in the order they should be executed.
    """

    # Helper function to recursively flatten and prioritize tasks.
    def flatten_tasks(task: Task) -> List[Task]:
        flat_list = []
        # Handle subtasks first, recursively 
        for substack in task.get('substack', []):
            flat_list.extend(flatten_tasks(substack))
        # Add the current task itself after its substacks
        flat_list.append(task)
        return flat_list
    # Flatten the task hierarchy 
    all_tasks = flatten_tasks(task_hierarchy)

    # Sort tasks by priority; tasks without priority will get a very low priority
    sorted_tasks = sorted(all_tasks, key=lambda x: x.get('priority', float('inf')))

    return sorted_tasks

# Example Useage
task_hierarchy = {
    "id": 1,
    "name": "Main Task",
    "subtasks": [
        {
            "id": 2,
            "name": "Subtask A",
            "priority": 1,
            "subtasks": []
        },
        {
            "id": 3,
            "name": "Substack B",
            "priority": 2,
            "subtasks": []
        }
    ],
    "priority": 3
}

result = schedule_tasks(task_hierarchy)
for task in result:
    print(f"Task {task['id']}: {task['name']} with priority {task.get('priority', 'None')}")
