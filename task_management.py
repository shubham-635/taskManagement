import time
from typing import List
import concurrent.futures

completedTasks = []


class Task:

    def __init__(self, id: str, dependencies: List[str], processingTime: int, priority: int):
        self.id = id
        self.dependencies = dependencies
        self.processingTime = processingTime
        self.priority = priority

    def execute_task(self):
        time.sleep(self.processingTime)
        completedTasks.append(self.id)
        print(f"Task {self.id} completed.")
        print(f"Completed Tasks Count: {len(completedTasks)}")


class TaskSchedular:

    def __init__(self, max_concurrency: int):
        self.max_concurrency = max_concurrency
        self.tasks: List[Task] = []

    def add_tasks(self, tasks: list):
        """
        Adding Tasks to a Class initialized List in the order of priority
        """
        for task in tasks:
            self.tasks.append(Task(id=task["id"], dependencies=task["dependencies"], processingTime=task["processingTime"], priority=task["priority"]))
        self.tasks.sort(key=lambda t: t.priority)
        return "Tasks Added"

    def run(self):
        # Creating a ThreadPool to Execute Tasks Concurrently, While also sharing same Memory Space for Tasks Tracking
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
            while self.tasks:
                for task in self.tasks[:]:
                    if all(dep in completedTasks for dep in task.dependencies):
                        executor.submit(task.execute_task)
                        self.tasks.remove(task)
                concurrent.futures.as_completed()
        return "Stopping Schedular"


if __name__ == "__main__":
    tasks = [
        {"id": "task1", "priority": 1, "dependencies": [], "processingTime": 2},
        {"id": "task2", "priority": 2, "dependencies": ["task1"], "processingTime": 1},
        {"id": "task3", "priority": 1, "dependencies": ["task1"], "processingTime": 3},
        {"id": "task4", "priority": 3, "processingTime": 1, "dependencies": ["task2", "task3"]}
    ]
    SchedularObj = TaskSchedular(max_concurrency=2)
    SchedularObj.add_tasks(tasks)
    SchedularObj.run()
