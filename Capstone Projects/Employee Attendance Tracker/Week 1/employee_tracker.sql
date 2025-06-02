-- 1. Create Schema & Tables


-- Creating a schema


CREATE SCHEMA employeeattendancetracker ;

USE employeeattendancetracker ;


-- Employee Table
CREATE TABLE Employee (
    employee_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    department VARCHAR(100),
    role VARCHAR(100)
);

-- Attendance Table
CREATE TABLE Attendance (
    attendance_id INT PRIMARY KEY AUTO_INCREMENT,
    employee_id INT,
    date DATE,
    clock_in TIME,
    clock_out TIME,
    FOREIGN KEY (employee_id) REFERENCES Employee(employee_id)
);

-- Tasks Table
CREATE TABLE Tasks (
    task_id INT PRIMARY KEY AUTO_INCREMENT,
    employee_id INT,
    task_name VARCHAR(255),
    status ENUM('Assigned', 'In Progress', 'Completed'),
    deadline DATE,
    FOREIGN KEY (employee_id) REFERENCES Employee(employee_id)
);



-- 2. CRUD Operations:

INSERT INTO Employee (name, department, role) VALUES 
('Vivek', 'IT', 'Full Stack Developer'),
('Nithya', 'HR', 'Hiring Manager'),
('Deepa', 'Finance', 'Senior Executive');


INSERT INTO Attendance (employee_id, date, clock_in, clock_out) VALUES
(1, '2025-06-01', '09:00:00', '17:30:00'),
(2, '2025-06-01', '09:30:00', '18:00:00'),
(3, '2025-06-01', '08:45:00', '17:00:00');


-- Clock-in 

INSERT INTO Attendance (employee_id, date, clock_in)
VALUES (1, CURDATE(), CURTIME());


-- Clock-out 

UPDATE Attendance
SET clock_out = CURTIME()
WHERE employee_id = 1 AND date = CURDATE();




INSERT INTO Tasks (employee_id, task_name, status, deadline) VALUES
(1, 'Develop Login API', 'Completed', '2025-06-05'),
(2, 'Conduct Interview Drive', 'In Progress', '2025-06-07'),
(3, 'Prepare Financial Report', 'Assigned', '2025-06-10');


-- Update Task Status

UPDATE Tasks
SET status = 'Completed'
WHERE task_id = 2;


-- 3. Stored Procedure: Calculate Total Working Hours

DELIMITER //

CREATE PROCEDURE GetWorkingHours(IN emp_id INT)
BEGIN
    SELECT employee_id,
           SUM(TIMESTAMPDIFF(MINUTE, clock_in, clock_out))/60 AS total_hours
    FROM Attendance
    WHERE employee_id = emp_id
    GROUP BY employee_id;
END //

DELIMITER ;


-- Usage:

CALL GetWorkingHours(1);


