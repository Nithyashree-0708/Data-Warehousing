  SQL ASSIGNMENT


DATABASE CREATION:

CREATE DATABASE ProductDB;
USE ProductDB;

TABLE CREATION:

	CREATE TABLE Product (
   	 ProductID INT PRIMARY KEY,
   	 ProductName VARCHAR(100),
   	 Category VARCHAR(50),
   	 Price DECIMAL(10, 2),
    	StockQuantity INT,
    	Supplier VARCHAR(100)
);

DATA INSERTION:

INSERT INTO Product (ProductID, ProductName, Category, Price, StockQuantity, Supplier) VALUES
(1, 'Laptop', 'Electronics', 70000, 50, 'TechMart'),
(2, 'Office Chair', 'Furniture', 5000, 100, 'HomeComfort'),
(3, 'Smartwatch', 'Electronics', 15000, 200, 'GadgetHub'),
(4, 'Desk Lamp', 'Lighting', 1200, 300, 'BrightLife'),
(5, 'Wireless Mouse', 'Electronics', 1500, 250, 'GadgetHub');


1.	CRUD Operations:

Add a New Product: 

INSERT INTO Product (ProductID, ProductName, Category, Price, StockQuantity, Supplier)
VALUES (6, 'Gaming Keyboard', 'Electronics', 3500, 150, 'TechMart');

Update Product Price: 

UPDATE Product
SET Price = Price * 1.10
WHERE Category = 'Electronics';

Delete a Product: 

DELETE FROM Product
WHERE ProductID = 4;

Read All Products: 

SELECT * FROM Product
ORDER BY Price DESC;

2.	Sorting and Filtering:

  	Sort products by stock quantity:
Display the list of products sorted by StockQuantity in ascending order.

SELECT * FROM Product
ORDER BY StockQuantity ASC;

Filter products by category:
List all products belonging to the Electronics category.
SELECT * FROM Product
WHERE Category = 'Electronics';

Filter products with AND condition:
Retrieve all Electronics products priced above 5000.
SELECT * FROM Product
WHERE Category = 'Electronics' AND Price > 5000;

Filter products with OR condition:
List all products that are either Electronics or priced below 2000.

SELECT * FROM Product
WHERE Category = 'Electronics' OR Price < 2000;

3.	Aggregation and Grouping:

Calculate total stock value:
Find the total stock value (Price * StockQuantity) for all products.

SELECT SUM(Price * StockQuantity) AS TotalStockValue
FROM Product;

Average price of each category:
Calculate the average price of products grouped by Category.

SELECT Category, AVG(Price) AS AveragePrice
FROM Product
GROUP BY Category;

Total number of products by supplier:
Count the total number of products supplied by GadgetHub.

SELECT COUNT(*) AS TotalProducts
FROM Product
WHERE Supplier = 'GadgetHub';

4.	 Conditional and Pattern Matching:

Find products with a specific keyword:
Display all products whose ProductName contains the word "Wireless".

SELECT * FROM Product
WHERE ProductName LIKE '%Wireless%';

Search for products from multiple suppliers:
Retrieve all products supplied by either TechMart or GadgetHub.

SELECT * FROM Product
WHERE Supplier IN ('TechMart', 'GadgetHub');

Filter using BETWEEN operator:
List all products with a price between 1000 and 20000.

	SELECT * FROM Product
WHERE Price BETWEEN 1000 AND 20000;

5.	Advanced Queries:
Products with high stock:
Find products where StockQuantity is greater than the average stock quantity.

SELECT * FROM Product
WHERE StockQuantity > (
    	SELECT AVG(StockQuantity) FROM Product
);

Get top 3 expensive products:
Display the top 3 most expensive products in the table.

SELECT * FROM Product
ORDER BY Price DESC
LIMIT 3;

Find duplicate supplier names:
Identify any duplicate supplier names in the table.

SELECT Supplier, COUNT(*) AS Occurrences
FROM Product
GROUP BY Supplier
HAVING COUNT(*) > 1;


Product summary:
Generate a summary that shows each Category with the number of products and the total stock value.

SELECT Category,
       	COUNT(*) AS NumberOfProducts,
      	 SUM(Price * StockQuantity) AS TotalStockValue
FROM Product
GROUP BY Category;


6.	 Join and Subqueries (if related tables are present):
Supplier with most products:
Find the supplier who provides the maximum number of products.

SELECT Supplier, COUNT(*) AS ProductCount
FROM Product
GROUP BY Supplier
ORDER BY ProductCount DESC
LIMIT 1;

Most expensive product per category:
List the most expensive product in each category.

SELECT Category, ProductName, Price
FROM Product P
WHERE Price = (
    	SELECT MAX(Price)
    	FROM Product
   	 WHERE Category = P.Category
);



