LISTS

#1. List of Squares Create a list of squares of numbers from 1 to 20.

squares = [i**2 for i in range(1, 21)]
print(squares)


#2. Second Largest Number Find the second largest number in a list without using
sort() .

nums = [10, 5, 8, 20, 20, 15]
first = second = float('-inf')
for num in nums:
    if num > first:
        second = first
        first = num
    elif first > num > second:
        second = num
print("Second largest:", second)


#3. Remove Duplicates Write a program to remove all duplicate values from a list
while preserving order.

lst = [1, 2, 3, 2, 1, 4]
seen = set()
result = []
for item in lst:
    if item not in seen:
        result.append(item)
        seen.add(item)
print(result)


#4. Rotate List Rotate a list to the right by k steps. Example: [1, 2, 3, 4, 5]
rotated by 2 → [4, 5, 1, 2, 3]

lst = [1, 2, 3, 4, 5]
k = 2
k %= len(lst)
rotated = lst[-k:] + lst[:-k]
print(rotated)


#5. List Compression From a list of numbers, create a new list with only the even
numbers doubled.

nums = [1, 2, 3, 4, 5, 6]
compressed = [x * 2 for x in nums if x % 2 == 0]
print(compressed)

TUPLES

#6. Swap Values Write a function that accepts two tuples and swaps their contents.

def swap_tuples(t1, t2):
    return t2, t1

a = (1, 2, 3)
b = (4, 5, 6)
a, b = swap_tuples(a, b)
print("Tuple A:", a)
print("Tuple B:", b)


#7. Unpack Tuples Unpack a tuple with student details: (name, age, branch, grade)
and print them in a sentence.

student = ("Alice", 20, "CSE", "A")
name, age, branch, grade = student
print(f"{name} is {age} years old, studies in {branch}, and got grade {grade}.")


#8. Tuple to Dictionary Convert a tuple of key-value pairs into a dictionary.
Example: (("a", 1), ("b", 2)) → {"a": 1, "b": 2}

pairs = (("a", 1), ("b", 2))
d = dict(pairs)
print(d)

SETS

#9. Common Items Find the common elements in two user-defined lists using sets.

list1 = [1, 2, 3, 4]
list2 = [3, 4, 5, 6]
common = set(list1) & set(list2)
print("Common items:", common)


#10. Unique Words in Sentence Take a sentence from the user and print all unique
words.

sentence = input("Enter a sentence: ")
unique_words = set(sentence.split())
print("Unique words:", unique_words)


#11. Symmetric Difference Given two sets of integers, print elements that are in one
set or the other, but not both.

s1 = {1, 2, 3}
s2 = {3, 4, 5}
sym_diff = s1 ^ s2
print("Symmetric difference:", sym_diff)

#12. Subset Checker Check if one set is a subset of another.

a = {1, 2}
b = {1, 2, 3, 4}
print("A is subset of B:", a.issubset(b))

DICTIONARIES

#13. Frequency Counter Count the frequency of each character in a string using a
dictionary.

text = "banana"
freq = {}
for char in text:
    freq[char] = freq.get(char, 0) + 1
print(freq)

#14. Student Grade Book Ask for names and marks of 3 students. Then ask for a name
and display their grade ( >=90: A , >=75: B , else C).

grades = {}
for _ in range(3):
    name = input("Enter name: ")
    mark = int(input("Enter mark: "))
    if mark >= 90:
        grade = 'A'
    elif mark >= 75:
        grade = 'B'
    else:
        grade = 'C'
    grades[name] = grade

query = input("Enter student name to check grade: ")
print(f"Grade for {query}:", grades.get(query, "Not found"))


#15. Merge Two Dictionaries Merge two dictionaries. If the same key exists, sum the
values.

d1 = {"a": 10, "b": 20}
d2 = {"b": 5, "c": 15}
merged = d1.copy()
for key, val in d2.items():
    merged[key] = merged.get(key, 0) + val
print(merged)


#16. Inverted Dictionary Invert a dictionary’s keys and values. Input: {"a": 1, "b":
2} → Output: {1: "a", 2: "b"}

d = {"a": 1, "b": 2}
inverted = {v: k for k, v in d.items()}
print(inverted)

#17. Group Words by Length Input a list of words. Create a dictionary where the key
is word length and the value is a list of words of that length.

words = input("Enter words separated by space: ").split()
grouped = {}
for word in words:
    length = len(word)
    grouped.setdefault(length, []).append(word)
print(grouped)
