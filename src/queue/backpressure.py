import psutil

class QueueCapacityController:
    def __init__(self, initial_capacity: int, min_capacity: int = 100):
        self.initial_capacity = initial_capacity
        self.current_capacity = initial_capacity
        self.min_capacity = min_capacity
        self.memory_threshold = 85.0

    def adjust_capacity(self) -> int:
        memory_usage = psutil.virtual_memory().percent
        if memory_usage > self.memory_threshold:
            # Contract capacity when memory pressure is high
            self.current_capacity = max(self.min_capacity, int(self.current_capacity * 0.5))
        else:
            # Slowly recover capacity when memory pressure is normal
            if self.current_capacity < self.initial_capacity:
                self.current_capacity = min(self.initial_capacity, int(self.current_capacity * 1.2) + 1)
                
        return self.current_capacity

    def get_capacity(self) -> int:
        return self.adjust_capacity()
