

# PYTHON FULL-SPECTRUM ASSESSMENT 

# Section 1: Python Basics & Control Flow

# Q1. Write a Python program to print all odd numbers between 10 and 50.


def print_odd_numbers(start, end):
    print(f"Odd numbers between {start} and {end}:")
    for num in range(start, end + 1):
        if num % 2 != 0:
            print(num, end=" ")

print_odd_numbers(10, 50)


# Q2. Create a function that returns whether a given year is a leap year.

def is_leap_year(year):
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)

# Example:
print(is_leap_year(2025))  # Output: False


# Q3. Write a loop that counts how many times the letter a appears in a given string.

def count_a(string):
    count = 0
    for char in string:
        if char.lower() == 'a':
            count += 1
    return count

# Example:
print(count_a("Data Analysis and AI"))  # Output: 5


# Section 2: Collections (Lists, Tuples, Sets, Dicts)
 
# Q4. Create a dictionary from the following lists:
# keys = ['a', 'b', 'c']
# values = [100, 200, 300]


keys = ['a', 'b', 'c']
values = [100, 200, 300]

# Creating dictionary using zip()
my_dict = dict(zip(keys, values))
print("Dictionary:", my_dict)



# Q5. From a list of employee salaries, extract:
# The maximum salary
# All salaries above average
# A sorted version in descending order


salaries = [50000, 60000, 55000, 70000, 52000]

# Maximum salary
max_salary = max(salaries)

# Average salary
average_salary = sum(salaries) / len(salaries)

# Salaries above average
above_avg_salaries = [s for s in salaries if s > average_salary]

# Sorted salaries in descending order
sorted_desc = sorted(salaries, reverse=True)

print("Maximum salary:", max_salary)
print("Salaries above average:", above_avg_salaries)
print("Salaries in descending order:", sorted_desc)


# Q6. Create a set from a list and remove duplicates. Show the difference between two sets:
# a = [1, 2, 3, 4]
# b = [3, 4, 5, 6]


a = [1, 2, 3, 4]
b = [3, 4, 5, 6]

# Converting it to sets (removing duplicates)
set_a = set(a)
set_b = set(b)

# Show difference: elements in a but not in b
difference = set_a.difference(set_b)

print("Set A:", set_a)
print("Set B:", set_b)
print("Difference (A - B):", difference)


# Section 3: Functions & Classes

# Q7. Write a class Employee with __init__ , display() , and is_high_earner() methods.
# An employee is a high earner if salary > 60000.

class Employee:
    def __init__(self, employee_id, name, department, salary):
        self.employee_id = employee_id
        self.name = name
        self.department = department
        self.salary = salary

    def display(self):
        print(f"ID: {self.employee_id}, Name: {self.name}, Department: {self.department}, Salary: {self.salary}")

    def is_high_earner(self):
        return self.salary > 60000


# Q8. Create a class Project that inherits from Employee and adds project_name and hours_allocated .

class Project(Employee):
    def __init__(self, employee_id, name, department, salary, project_name, hours_allocated):
        super().__init__(employee_id, name, department, salary)
        self.project_name = project_name
        self.hours_allocated = hours_allocated

    def display_project(self):
        self.display()
        print(f"Project: {self.project_name}, Hours Allocated: {self.hours_allocated}")



# Q9. Instantiate 3 employees and print whether they are high earners.

# Creating 3 Employee objects

emp1 = Employee(1, "Ali", "HR", 50000)
emp2 = Employee(2, "Neha", "IT", 60000)
emp3 = Employee(4, "Sara", "IT", 70000)

# Checking and displaying whether they are high earners employees = [emp1, emp2, emp3]

for emp in employees:
    emp.display()
    print("High Earner:", emp.is_high_earner())
    print()

# Section 4: File Handling

# Q10. Write to a file the names of employees who belong to the 'IT' department.

employees = [
    {"EmployeeID": 1, "Name": "Ali", "Department": "HR"},
    {"EmployeeID": 2, "Name": "Neha", "Department": "IT"},
    {"EmployeeID": 3, "Name": "Ravi", "Department": "Finance"},
    {"EmployeeID": 4, "Name": "Sara", "Department": "IT"},
    {"EmployeeID": 5, "Name": "Vikram", "Department": "HR"},
]

# Write names of IT department employees to a file

with open("it_employees.txt", "w") as file:
    for emp in employees:
        if emp["Department"] == "IT":
            file.write(emp["Name"] + "\n")

# After this, a file named it_employees.txt will contain:

Neha
Sara


# Q11. Read from a text file and count the number of words.

def count_words_in_file(filename):
    try:
        with open(filename, "r") as file:
            content = file.read()
            words = content.split()
            return len(words)
    except FileNotFoundError:
        print("File not found.")
        return 0

# Example:

word_count = count_words_in_file("sample.txt")
print("Total words:", word_count)

# Section 5: Exception Handling

# Q12. Write a program that accepts a number from the user and prints the square. Handle
the case when input is not a number.


try:
    user_input = input("Enter a number: ")
    number = float(user_input)
    square = number ** 2
    print(f"The square of {number} is {square}")
except ValueError:
    print("Invalid input. Please enter a numeric value.")


# Q13. Handle a potential ZeroDivisionError in a division function.

def safe_divide(a, b):
    try:
        result = a / b
        print(f"Result: {result}")
    except ZeroDivisionError:
        print("Error: Division by zero is not allowed.")

# Example usage:

safe_divide(10, 2)   # Valid
safe_divide(5, 0)    # Will trigger exception


# Section 6: Pandas – Reading & Exploring CSVs

# Q14. Load both employees.csv and projects.csv using Pandas.

import pandas as pd

# Load the datasets
employees_df = pd.read_csv("employees.csv")
projects_df = pd.read_csv("projects.csv")



# Q15. Display:

# First 2 rows of employees:

print(employees_df.head(2))


# Unique values in the Department column:

print(employees_df['Department'].unique())


# Average salary by department:

avg_salary = employees_df.groupby('Department')['Salary'].mean()
print(avg_salary)



# Q16. Add a column TenureInYears = current year - joining year.

from datetime import datetime

# Convert JoiningDate to datetime

employees_df['JoiningDate'] = pd.to_datetime(employees_df['JoiningDate'])

# Calculate tenure

current_year = datetime.now().year
employees_df['TenureInYears'] = current_year - employees_df['JoiningDate'].dt.year

print(employees_df[['Name', 'JoiningDate', 'TenureInYears']])


# Section 7: Data Filtering, Aggregation, and Sorting

# Q17. From employees.csv , filter all IT department employees with salary > 60000.

import pandas as pd

employees_df = pd.read_csv("employees.csv")

filtered_it_employees = employees_df[
    (employees_df['Department'] == 'IT') & (employees_df['Salary'] > 60000)
]

print(filtered_it_employees)



# Q18. Group by Department and get:
# Count of employees
# Total Salary
# Average Salary


department_stats = employees_df.groupby('Department').agg(
    EmployeeCount=('EmployeeID', 'count'),
    TotalSalary=('Salary', 'sum'),
    AverageSalary=('Salary', 'mean')
)

print(department_stats)


# Q19. Sort all employees by salary in descending order.

sorted_employees = employees_df.sort_values(by='Salary', ascending=False)
print(sorted_employees)


# Section 8: Joins & Merging

# Q20. Merge employees.csv and projects.csv on EmployeeID to show project
allocations.




merged_df = pd.merge(employees_df, projects_df, on='EmployeeID', how='inner')
print(merged_df)



# Q21. List all employees who are not working on any project (left join logic).

left_join_df = pd.merge(employees_df, projects_df, on='EmployeeID', how='left')
not_assigned_df = left_join_df[left_join_df['ProjectID'].isna()]
print(not_assigned_df[['EmployeeID', 'Name', 'Department']])



# Q22. Add a derived column TotalCost = HoursAllocated * (Salary / 160) in the merged
dataset.


# Use left join to include all employees and related project info

merged_df = pd.merge(employees_df, projects_df, on='EmployeeID', how='left')

# Calculate TotalCost only for rows where HoursAllocated is not null

merged_df['TotalCost'] = merged_df.apply(
    lambda row: row['HoursAllocated'] * (row['Salary'] / 160)
    if pd.notnull(row['HoursAllocated']) else None, axis=1
)

print(merged_df[['EmployeeID', 'Name', 'ProjectName', 'HoursAllocated', 'Salary', 'TotalCost']])


