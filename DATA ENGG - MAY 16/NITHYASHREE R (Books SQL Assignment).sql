SQL ASSIGNMENT (Books)
Creating Database:
CREATE DATABASE LibraryDB;
USE LibraryDB;
Creating Table:
CREATE TABLE Book (
    BookID INT PRIMARY KEY,
    Title VARCHAR(100),
    Author VARCHAR(100),
    Genre VARCHAR(50),
    Price DECIMAL(10, 2),
    PublishedYear INT,
    Stock INT
);
Inserting Data:
INSERT INTO Book (BookID, Title, Author, Genre, Price, PublishedYear, Stock) VALUES
(1, 'The Alchemist', 'Paulo Coelho', 'Fiction', 300, 1988, 50),
(2, 'Sapiens', 'Yuval Noah Harari', 'Non-Fiction', 500, 2011, 30),
(3, 'Atomic Habits', 'James Clear', 'Self-Help', 400, 2018, 25),
(4, 'Rich Dad Poor Dad', 'Robert Kiyosaki', 'Personal Finance', 350, 1997, 20),
(5, 'The Lean Startup', 'Eric Ries', 'Business', 450, 2011, 15);

1.	CRUD Operations:
Add a new book: Insert a book titled "Deep Work" by Cal Newport, Genre Self-Help, Price 420, Published Year 2016, Stock 35.

INSERT INTO Book (BookID, Title, Author, Genre, Price, PublishedYear, Stock)
VALUES (6, 'Deep Work', 'Cal Newport', 'Self-Help', 420, 2016, 35);
Update book price: Increase the price of all Self-Help books by 50.
UPDATE Book
SET Price = Price + 50
WHERE Genre = 'Self-Help';
Delete a book: Remove the book with BookID = 4 (Rich Dad Poor Dad).
DELETE FROM Book
WHERE BookID = 4;
Read all books: Display all books sorted by Title in ascending order.
SELECT * FROM Book
ORDER BY Title ASC;
2.	Sorting and Filtering:
Sort by price: List books sorted by Price in descending order.
SELECT * FROM Book
ORDER BY Price DESC;

Filter by genre: Display all books belonging to the Fiction genre.
SELECT * FROM Book
WHERE Genre = 'Fiction';

Filter with AND condition: List all Self-Help books priced above 400.
SELECT * FROM Book
WHERE Genre = 'Self-Help' AND Price > 400;

Filter with OR condition: Retrieve all books that are either Fiction or published after 2000.
SELECT * FROM Book
WHERE Genre = 'Fiction' OR PublishedYear > 2000;

3.	 Aggregation and Grouping:
Total stock value: Calculate the (Price * Stock).
SELECT SUM(Price * Stock) AS TotalStockValue
FROM Book;

Average price by genre: Calculate the average price of books grouped by Genre.
SELECT Genre, AVG(Price) AS AveragePrice
FROM Book
GROUP BY Genre;

Total books by author: Count the number of books written by Paulo Coelho.
SELECT COUNT(*) AS TotalBooksByCoelho
FROM Book
WHERE Author = 'Paulo Coelho';
4.	 Conditional and Pattern Matching:
Find books with a keyword in title: List all books whose Title contains the word "The".
SELECT * FROM Book
WHERE Title LIKE '%The%';
Filter by multiple conditions: Display all books by Yuval Noah Harari priced below 600.
SELECT * FROM Book
WHERE Author = 'Yuval Noah Harari' AND Price < 600;
Find books within a price range: List books priced between 300 and 500.
SELECT * FROM Book
WHERE Price BETWEEN 300 AND 500;
5.	 Advanced Queries:
Top 3 most expensive books: Display the top 3 books with the highest price.
SELECT * FROM Book
ORDER BY Price DESC
LIMIT 3;

Books published before a specific year: Find all books published before the year 2000.
SELECT * FROM Book
WHERE PublishedYear < 2000;

Group by Genre: Calculate the total number of books in each Genre.
SELECT Genre, COUNT(*) AS TotalBooks
FROM Book
GROUP BY Genre;

Find duplicate titles: Identify any books having the same title.
SELECT Title, COUNT(*) AS Count
FROM Book
GROUP BY Title
HAVING COUNT(*) > 1;

6.	Join and Subqueries:
Author with the most books: Find the author who has written the maximum number of books.
SELECT Author, COUNT(*) AS BookCount
FROM Book
GROUP BY Author
ORDER BY BookCount DESC
LIMIT 1;

Oldest book by genre: Find the earliest published book in each genre.
SELECT Genre, Title, Author, PublishedYear
FROM Book b
WHERE PublishedYear = (
    SELECT MIN(PublishedYear)
    FROM Book
    WHERE Genre = b.Genre
);

















