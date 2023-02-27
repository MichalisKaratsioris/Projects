# def rotate(nums: list, k: int) -> list:
#     l = len(nums)
#     return nums[l - k:] + nums[:l - k]
#
#
# # def rotate(nums: list, k: int) -> list:
# #     nums_copy = nums.copy()
# #     for i in range(len(nums)):
# #         nums_copy[i - k] = nums[i]
# #     return nums_copy
#
#
# print(rotate([1, 2, 3, 4, 5, 6, 7], 3))


# def num_substrings(s: str) -> int:
#     result = 0
#     for i in range(len(s) - 3):
#         if s[i] == 's':
#             result += 1
#     return result
#
#
# s = "asderyologeccsavso"
# print(num_substrings(s))

# def dig_sum(nums: list) -> int:
#     result = 0
#     for num in nums:
#         if num // 100 == 0:
#             continue
#         num_str = str(num)
#         d_str = num_str[-3]
#         d = int(d_str)
#         result += d
#     return result
#
#
# arr = [123, 23, 7777677,  69, 420, 1]
# print(dig_sum(arr))

# def arr_str_find(arr: list, s: str) -> list:
#     result = []
#     for phrase in arr:
#         phrase = phrase.replace(s, s.upper())
#         result.append(phrase)
#     return result
#
#
# arr = ["ez egy peldamondasd hehehe yolo", "ez is egy peldamondat, de nincs benne semmi", "ez megint egy peldamondat tobb asd-dal is. Itt van meg egy asd, mert miert ne"]
# s = "asd"
# print(arr_str_find(arr, s))


# message = "Welcome"
# # message[0] = 'p'
# for i in range(len(message)):
#     if i % 2 == 0:
# #         message[i] = 1
#         message = message.replace(message[i], "0", 1)
# print(message)


class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def print_info(self):
        print(f"Name: {self.name}, Age: {self.age}")


class Employee(Person):
    def __init__(self, name, age, job_title):
        super().__init__(name, age)
        self.job_title = job_title

    def print_info(self):
        # print(f"Name: {self.name}, Age: {self.age}, Job Title: {self.job_title}")
        # print(f" Name: {self.name}\n Age: {self.age}\n Job Title: {self.job_title}")
        super().print_info()
        print(f"Job Title: {self.job_title}")


e = Employee("John Doe", 30, "Software Engineer")
# e.print_info()


class BankAccount:
    def __init__(self, balance):
        self.__balance = balance

    def get_balance(self):
        return self.__balance

    def deposit(self, amount):
        self.__balance += amount

    def withdraw(self, amount):
        if self.__balance >= amount:
            self.__balance -= amount
            return True
        else:
            return False


b = BankAccount(100)
# print("First: ", b.get_balance())
# print("Second: ", b._BankAccount__balance)
# b._BankAccount__balance = 200
# print("Third: ", b._BankAccount__balance)
print(b.__dict__)
print(e.__dict__)

from abc import ABC, abstractmethod


# Interface
class Shape(ABC):
    @abstractmethod
    def area(self):
        pass

    @abstractmethod
    def perimeter(self):
        pass


# Abstract class
class Polygon(Shape):
    def __init__(self, num_of_sides):
        self.__num_of_sides = num_of_sides

    # Encapsulation
    def get_num_of_sides(self):
        return self.__num_of_sides

    # Abstraction
    @abstractmethod
    def calculate_area(self):
        pass


# Inheritance
class Triangle(Polygon):
    def __init__(self, base, height):
        super().__init__(3)
        self.__base = base
        self.__height = height

    # Polymorphism
    def area(self):
        return self.calculate_area()

    def perimeter(self):
        return 3 * self.__base

    def calculate_area(self):
        return 0.5 * self.__base * self.__height


class Rectangle(Polygon):
    def __init__(self, length, width):
        super().__init__(4)
        self.__length = length
        self.__width = width

    # Polymorphism
    def area(self):
        return self.calculate_area()

    def perimeter(self):
        return 2 * (self.__length + self.__width)

    def calculate_area(self):
        return self.__length * self.__width

