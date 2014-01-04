from collections import deque

# http://docs.python.org/3.3/library/collections.html#collections.deque
class Pool:
    def __init__(self):
        self.__queue__ = deque()

    def __len__(self):
        return len(self.__queue__)

    def insert(self, obj):
        self.__queue__.append(obj)

    def insert_with_priority(self, obj):
        self.__queue__.appendleft(obj)

    def insert_all(self, list_of_objs):
        self.__queue__.extend(list_of_objs)

    def insert_all_with_priority(self, list_of_objs):
        self.__queue__.extendleft(list_of_objs)

    def pop(self):
        self.__queue__.popleft()

