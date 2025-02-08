import asyncio
from concurrent.futures import ThreadPoolExecutor

completedTasks = []

class Task:

    def __init__(self, id: str, dependencies: str, processingTime: int, priority: int):
        self.id = id
        self.dependencies = dependencies
        self.processingTime = processingTime
        self.priority = priority

    def execute_task(self):
        # Using Asyncio To Create A non-blocking Sleep 
        asyncio.sleep(self.processingTime)
        completedTasks.append(self.id)
        print(f"Task {self.id} completed.")


class TaskSchedular:

    def __init__(self, max_concurrency: int):
        self.max_concurrency = max_concurrency
        self.tasks = []
        self.incomplete_priorities = []

    def finish_task_recursively(self, executor):
        for task in self.incomplete_priorities:
            if all(depencency in completedTasks and task["id"] not in completedTasks for depencency in task["dependencies"]):
                executor.run(Task(id=task["id"], dependencies=task["dependencies"],
                                  priority=task["priority"], processingTime=task["processingTime"]))
                print(f"Tasks Completed: {len(completedTasks)}")
            else:
                pass
        if len(self.tasks) != len(completedTasks):
            self.finish_task_recursively(executor)
        return 

    def add_tasks(self, tasks: list):
        """
        Adding Tasks to a Class initialized List in the oder of priority
        """"
        for task in tasks:
            self.tasks.insert(task["priority"]-1, task)
        return "Tasks Added"

    def run(self):
        # Creating a ThreadPool to Execute Tasks Concurrently, While also sharing same Memory Space for Tasks Tracking
        with ThreadPoolExecutor(self.max_concurrency) as executor:
            for task in self.tasks:
                # Differiantiating the Task/s with Zero Dependencies to Start in the Beginning
                if not task["dependencies"]:
                    executor.run(Task(id=task["id"], dependencies=task["dependencies"], 
                                      priority=task["priority"], processingTime=task["processingTime"]))
                    print(f"Tasks Completed: {len(completedTasks)}")
                
                else:
                    # Executing Tasks with All its Dependencies Finished Already
                    if all(depencency in completedTasks for depencency in task["dependencies"]):
                        executor.run(Task(id=task["id"], dependencies=task["dependencies"],
                                          priority=task["priority"], processingTime=task["processingTime"]))
                        print(f"Tasks Completed: {len(completedTasks)}")
                    # Adding the Tasks with incomplete depencencies to another Class List
                    else:
                        self.incomplete_priorities.append(task)
            # Finished unexecuted Task Recursively while Making sure All the depencies are Executed Prior to the Task
            self.finish_task_recursively(executor)
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
