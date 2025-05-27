   PYTHON BASICS

VARIABLES, DATA TYPES, OPERATORS
#1. Digit Sum Calculator Ask the user for a number and calculate the sum of its
digits. Example: 753 → 7 + 5 + 3 = 15
num = input("Enter a number: ")
digit_sum = sum(int(d) for d in num)
print("Digit sum:", digit_sum)
#2. Reverse a 3-digit Number Input a 3-digit number and print it reversed. Input:
123 → Output: 321
num = input("Enter a 3-digit number: ")
if len(num) == 3 and num.isdigit():
    print("Reversed:", num[::-1])
else:
    print("Please enter a valid 3-digit number.")
#3. Unit Converter Build a converter that takes meters and converts to:
centimeters
feet
inches
meters = float(input("Enter distance in meters: "))
print("Centimeters:", meters * 100)
print("Feet:", meters * 3.28084)
print("Inches:", meters * 39.3701)
#4. Percentage Calculator Input marks of 5 subjects and calculate total, average,
and percentage. Display grade based on the percentage.
marks = [float(input(f"Enter marks for subject {i+1}: ")) for i in range(5)]
total = sum(marks)
average = total / 5
percentage = (total / 500) * 100

print("Total:", total)
print("Average:", average)
print("Percentage:", percentage)

if percentage >= 90:
    grade = 'A'
elif percentage >= 75:
    grade = 'B'
elif percentage >= 60:
    grade = 'C'
elif percentage >= 50:
    grade = 'D'
else:
    grade = 'F'

print("Grade:", grade)

CONDITIONALS
#5. Leap Year Checker A year is a leap year if it’s divisible by 4 and (not
divisible by 100 or divisible by 400).
year = int(input("Enter a year: "))
if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
    print("Leap year")
else:
    print("Not a leap year")
#6. Simple Calculator Input two numbers and an operator ( + - * / ) and perform the
operation using if...elif...else .
a = float(input("Enter first number: "))
b = float(input("Enter second number: "))
op = input("Enter operator (+, -, *, /): ")

if op == '+':
    print("Result:", a + b)
elif op == '-':
    print("Result:", a - b)
elif op == '*':
    print("Result:", a * b)
elif op == '/':
    if b != 0:
        print("Result:", a / b)
    else:
        print("Cannot divide by zero")
else:
    print("Invalid operator")
#7. Triangle Validator Given 3 side lengths, check whether they can form a valid
triangle.
a = float(input("Enter side A: "))
b = float(input("Enter side B: "))
c = float(input("Enter side C: "))

if a + b > c and a + c > b and b + c > a:
    print("Valid triangle")
else:
    print("Not a valid triangle")
#8. Bill Splitter with Tip Ask total bill amount, number of people, and tip
percentage. Show final amount per person.
total = float(input("Enter total bill amount: "))
people = int(input("Number of people: "))
tip_percent = float(input("Tip percentage: "))

tip = total * (tip_percent / 100)
final_amount = total + tip
per_person = final_amount / people

print(f"Each person should pay: {per_person:.2f}")

LOOPS
#9. Find All Prime Numbers Between 1 and 100 Use a nested loop to check
divisibility.
for num in range(2, 101):
    is_prime = True
    for i in range(2, int(num**0.5)+1):
        if num % i == 0:
            is_prime = False
            break
    if is_prime:
        print(num, end=' ')
#10. Palindrome Checker Ask for a string and check whether it reads the same
backward.
text = input("Enter a string: ")
if text == text[::-1]:
    print("Palindrome")
else:
    print("Not a palindrome")
#11. Fibonacci Series (First N Terms) Input n , and print first n terms of the
Fibonacci sequence.
n = int(input("Enter how many terms: "))
a, b = 0, 1
for _ in range(n):
    print(a, end=' ')
    a, b = b, a + b
#12. Multiplication Table (User Input) Take a number and print its table up to 10:
5 x 1 = 5
5 x 2 = 10
...
num = int(input("Enter a number: "))
for i in range(1, 11):
    print(f"{num} x {i} = {num*i}")

#13. Number Guessing Game
Generate a random number between 1 to 100
Ask the user to guess
Give hints: "Too High", "Too Low"
Loop until the correct guess
import random
target = random.randint(1, 100)
guess = None

while guess != target:
    guess = int(input("Guess the number (1-100): "))
    if guess < target:
        print("Too low!")
    elif guess > target:
        print("Too high!")
    else:
        print("Correct!")
#14. ATM Machine Simulation
Balance starts at
10,000

Menu: Deposit / Withdraw / Check Balance / Exit
Use a loop to keep asking
Use conditionals to handle choices
balance = 10000

while True:
    print("\n1. Deposit\n2. Withdraw\n3. Check Balance\n4. Exit")
    choice = input("Choose an option: ")

    if choice == '1':
        amt = float(input("Enter deposit amount: "))
        balance += amt
    elif choice == '2':
        amt = float(input("Enter withdrawal amount: "))
        if amt <= balance:
            balance -= amt
        else:
            print("Insufficient funds")
    elif choice == '3':
        print("Current balance:", balance)
    elif choice == '4':
        break
    else:
        print("Invalid option")
#15. Password Strength Checker
Ask the user to enter a password
Check if it's at least 8 characters
Contains a number, a capital letter, and a symbol
import re

password = input("Enter a password: ")

if (len(password) >= 8 and
    re.search(r"\d", password) and
    re.search(r"[A-Z]", password) and
    re.search(r"[!@#$%^&*(),.?\":{}|<>]", password)):
    print("Strong password")
else:
    print("Weak password. Try again.")
#16. Find GCD (Greatest Common Divisor)
Input two numbers
Use while loop or Euclidean algorithm
a = int(input("Enter first number: "))
b = int(input("Enter second number: "))

while b:
    a, b = b, a % b

print("GCD is", a)
