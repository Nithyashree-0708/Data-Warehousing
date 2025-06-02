-- 1. Create Schema & Tables


-- Creating a schema


CREATE SCHEMA retailsalesdashboard ;

USE retailsalesdashboard ;


-- Products Table
CREATE TABLE products (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10, 2)
);

-- Stores Table
CREATE TABLE stores (
    store_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    region VARCHAR(50)
);

-- Employees Table
CREATE TABLE employees (
    employee_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    position VARCHAR(50),
    store_id INT,
    FOREIGN KEY (store_id) REFERENCES stores(store_id)
);

-- Sales Table
CREATE TABLE sales (
    sale_id INT PRIMARY KEY AUTO_INCREMENT,
    product_id INT,
    store_id INT,
    employee_id INT,
    quantity INT,
    sale_date DATE,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (store_id) REFERENCES stores(store_id),
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);



-- 2. CRUD Operations

-- INSERT
INSERT INTO products (name, category, price) VALUES ('Laptop', 'Electronics', 80000.00), 
('Smartphone', 'Electronics', 50000.00),
('Headphones', 'Accessories', 3000.00),
('LED TV', 'Electronics', 60000.00),
('Air Conditioner', 'Home Appliances', 40000.00);



INSERT INTO stores (name, region) VALUES ('iPlanet', 'Chennai'),
('Reliance Digital', 'Bangalore'),
('Croma', 'Mumbai'),
('Sathya Stores', 'Coimbatore'),
('Viveks', 'Chennai');



INSERT INTO employees (name, position, store_id) VALUES ('Alice', 'Salesperson', 1),
('Nithya', 'Manager', 2),
('Deepa', 'Salesperson', 3),
('Vivek', 'Cashier', 4),
('Rekha', 'Salesperson', 5);


INSERT INTO sales (product_id, store_id, employee_id, quantity, sale_date) VALUES (1, 1, 1, 2, '2025-06-01'),
(2, 2, 2, 1, '2025-06-01'),
(3, 3, 3, 5, '2025-06-02'),
(4, 4, 4, 3, '2025-06-02'),
(5, 5, 5, 1, '2025-06-03');

-- READ
SELECT * FROM sales WHERE sale_date = '2025-06-01';

-- UPDATE
UPDATE products SET price = 90000.00 WHERE product_id = 1;

-- DELETE
DELETE FROM employees WHERE employee_id = 1;



-- 3. Stored Procedure: Daily Sales per Store

DELIMITER $$

CREATE PROCEDURE GetDailySales(IN input_store_id INT, IN input_date DATE)
BEGIN
    SELECT 
        s.store_id, 
        s.name AS store_name, 
        SUM(sa.quantity * p.price) AS total_sales
    FROM 
        sales sa
    JOIN products p ON sa.product_id = p.product_id
    JOIN stores s ON sa.store_id = s.store_id
    WHERE 
        sa.store_id = input_store_id AND
        sa.sale_date = input_date
    GROUP BY s.store_id, s.name;
END$$

DELIMITER ;



-- 4. Indexing for Search

-- Index for product name
CREATE INDEX idx_product_name ON products(name);

-- Index for region
CREATE INDEX idx_store_region ON stores(region);

