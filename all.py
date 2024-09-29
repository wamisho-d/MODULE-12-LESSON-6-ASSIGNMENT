# Task 1: Design the Task Scheduler Function
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

# Task 2: Implement Task Scheduling Logic
from collections import defaultdict, deque
import heapq

class Task:
    def __init__(self, id, priority, dependencies=None):
        self.id = id
        self.priority = priority
        self.dependencies = dependencies if dependencies else []
        self.subtasks = []
def schedule_tasks(tasks):
    # Step 1: Build the graph of tasks
    graph = defaultdict(list) # Adjacency list for task dependencies
    indegree = defaultdict(int) # Track number of incoming edges (dependencies)

    for task in tasks:
        for dep in task. dependencies:
            graph[dep.id].append(task.id)
            indegree[task.id] += 1
    # Step 2: Use a min-heap to prioritize tasks based on priority
    # Min-heap will store tasks as tuples (priority, task_id)
    priority_queue = []

    # Initialize the queue with tasks that have no dependencies (indegree == 0)
    for task in tasks:
        if indegree[task.id] == 0:
            heapq.heappush(priority_queue, (task.priority, task.id))
    
    # Step 3: Perform the topological sort with priority consideration 
    sorted_tasks = []
    while priority_queue:
        # Get the task with the highest priority and no unmet dependencies
        current_priority, current_task_id = heapq.heappop(priority_queue)

        # Add the current task to the schedule 
        sorted_tasks.append(current_task_id)

        # Schedule all tasks that depend on the current task
        for neighbor in graph[current_task_id]:
            indegree[neighbor] -= 1
            if indegree[neighbor] == 0:
                # push the task into the heap based on its priority
                for task in tasks:
                    if task.id == neighbor:
                        heapq.heappush(priority_queue, (task.priority, neighbor))
                        break
        return sorted_tasks
# Example Useage
# Define tasks
task1 = Task(id='Task1', priority=1) # Highest priority
task2 = Task(id='Task2', priority=2, dependencies=[task1]) 
task3 = Task(id='Task3', priority=3, dependencies=[task2]) 
task4 = Task(id='Task4', priority=1) 

# Schedule the tasks
tasks = [task1, task2, task3, task4]
schedule = schedule_tasks(tasks)

print("Scheduled task order:", schedule)

# Task 3: Test the Task Scheduler Function
from collections import defaultdict, deque

class Task:
    def __init__(self, name, priority):
        self.name = name
        self.priority = priority
        self.dependencies = []
    
    def add_dependency(self, task):
        self.dependencies.append(task)

# Function to schedule tasks based on dependencies and priorities
def schedule_tasks(tasks):
    indegree = defaultdict(int)
    graph = defaultdict(list)

    # Build the graph and compute indegree
    for task in tasks:
        for dependency in task.dependencies:
            graph[dependency.name].append(task.name)
            indegree[task.name] += 1
    
    # Min-heap (priority queue) for processing tasks by priority
    task_queue = deque([task.name for task in tasks if indegree[task.name] == 0])
    task_priority_map = {task.name: task.priority for task in tasks}

    # Sort the task queue by priority (lower priority number first)
    task_queue = deque(sorted(task_queue, key=lambda t: task_priority_map[t]))

    result = []

    while task_queue:
        task_name = task_queue.popleft()
        result.append(task_name)

        for dependent_task in graph[task_name]:
            indegree[dependent_task] -= 1
            if indegree[dependent_task] == 0:
                task_queue.append(dependent_task)
        # Re-sort by priority after each insertion
        task_queue = deque(sorted(task_queue, key=lambda t: task_priority_map[t]))
    if len(result) != len(tasks):
        raise ValueError("There is a cyclic dependency in the task graph.")
    
    return result

# Test case helper function
def test_schedule_tasks(tasks, expected_order):
    try:
        result = schedule_tasks(tasks)
        assert result == expected_order, f"Test failed! Expected {expected_order}, but got {result}."
        print(f"Test passed! Result: {result}")
    except ValueError as e:
        print(f"Error: {e}")

# Define tasks for testing 

# Test Case 1: Basic Hierarchy with simple Dependecies 
task_A = Task("A", 2)
task_B = Task("B", 1)
task_B.add_dependency(task_A)
task_1 = [task_A, task_B]
test_schedule_tasks(tasks_1, ["A", "B"])

# Test Case 2: Nested dependencies with Different Priorities
task_C = Task("C", 1)
task_D = Task("D", 3)
task_E = Task("E", 2)
task_D.add_dependency(task_C)
task_E.add_dependency(task_D)
task_2 = [task_C, task_D, task_E]
test_schedule_tasks(tasks_2, ["C", "D", "E"])

# Test Case 3: Nested dependencies with Different Priorities
task_F = Task("F", 2)
task_G = Task("G", 1)
task_H = Task("H", 3)
task_3 = [task_F, task_G, task_H]
test_schedule_tasks(tasks_3, ["F", "G", "H"])

# Test Case 4: Nested dependencies with Different Priorities
task_I = Task("I", 3)
task_J = Task("J", 2)
task_K = Task("K", 1)
task_J.add_dependency(task_I)
task_K.add_dependency(task_J)
task_4 = [task_I, task_J, task_K]
test_schedule_tasks(tasks_4, ["I", "J", "K"])
# Test Case 5: Nested dependencies with Different Priorities
task_L = Task("L", 2)
task_M = Task("M", 1)
task_M.add_dependency(task_M)
task_E.add_dependency(task_L)
task_5 = [task_L, task_M]
test_schedule_tasks(tasks_5, None) # Expecting an error due to cyclic dependency


# Task 4: from collections import defaultdict, deque
import heapq
from collections import defaultdict

class Task:
    def __init__(self, name, priority):
        self.name = name
        self.priority = priority
        self.dependencies = []
    
    def add_dependency(self, task):
        self.dependencies.append(task)

# Function to schedule tasks based on dependencies and priorities using min-heap
def schedule_tasks(tasks):
    indegree = defaultdict(int)
    graph = defaultdict(list)

    # Build the graph and compute indegree
    for task in tasks:
        for dependency in task.dependencies:
            graph[dependency.name].append(task.name)
            indegree[task.name] += 1
    
    # Min-heap for processing tasks by priority
    heap = []
    
    # Insert tasks with no dependencies (indegree 0) into the heap
    for task in tasks:
        if indegree[task.name] == 0:
            heapq.heappush(heap, (task.priority, task.name))
    task_priority_map = {task.name: task.priority for task in tasks}
    result = []

    # Process tasks using the min_heap
    while heap:
        priority, task_name = heapq.heappop(heap)
        result.append(task_name)

        for dependent_task in graph[task_name]:
            indegree[dependent_task] -= 1
            if indegree[dependent_task] == 0:
                heapq.heappush(heap, (task_priority_map[dependent_task], dependent_task))
       
    if len(result) != len(tasks):
        raise ValueError("There is a cyclic dependency in the task graph.")
    
    return result

# Test case helper function
def test_schedule_tasks(tasks, expected_order):
    try:
        result = schedule_tasks(tasks)
        assert result == expected_order, f"Test failed! Expected {expected_order}, but got {result}."
        print(f"Test passed! Result: {result}")
    except ValueError as e:
        print(f"Error: {e}")

# Define tasks for testing 

# Test Case 1: Basic Hierarchy with simple Dependecies 
task_A = Task("A", 2)
task_B = Task("B", 1)
task_B.add_dependency(task_A)
task_1 = [task_A, task_B]
test_schedule_tasks(tasks_1, ["A", "B"])

# Test Case 2: Nested dependencies with Different Priorities
task_C = Task("C", 1)
task_D = Task("D", 3)
task_E = Task("E", 2)
task_D.add_dependency(task_C)
task_E.add_dependency(task_D)
task_2 = [task_C, task_D, task_E]
test_schedule_tasks(tasks_2, ["C", "D", "E"])

# Test Case 3: Nested dependencies with Different Priorities
task_F = Task("F", 2)
task_G = Task("G", 1)
task_H = Task("H", 3)
task_3 = [task_F, task_G, task_H]
test_schedule_tasks(tasks_3, ["F", "G", "H"])

# Test Case 4: Nested dependencies with Different Priorities
task_I = Task("I", 3)
task_J = Task("J", 2)
task_K = Task("K", 1)
task_J.add_dependency(task_I)
task_K.add_dependency(task_J)
task_4 = [task_I, task_J, task_K]
test_schedule_tasks(tasks_4, ["I", "J", "K"])
# Test Case 5: Nested dependencies with Different Priorities
task_L = Task("L", 2)
task_M = Task("M", 1)
task_M.add_dependency(task_M)
task_E.add_dependency(task_L)
task_5 = [task_L, task_M]
test_schedule_tasks(tasks_5, None) # Expecting an error due to cyclic dependency




















