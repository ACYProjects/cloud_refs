-- You have two tables: Employees and Departments.

-- Employees table:
-- employee_id (INT, Primary Key)
-- employee_name (VARCHAR)
-- department_id (INT, Foreign Key to Departments)
-- salary (DECIMAL)
-- Departments table:
-- department_id (INT, Primary Key)
-- department_name (VARCHAR)
-- Write a SQL query that returns the department_name, the average_salary for that department, and the highest_salary in that department. Only include departments where the average salary is greater than $60,000. Order the results by average_salary in descending order.

-- Expected Output Columns:

-- department_name
-- average_salary
-- highest_salary

//
SELECT
    d.department_name,
    AVG(e.salary) AS average_salary,
    MAX(e.salary) AS highest_salary
FROM
    Employees e
JOIN
    Departments d ON e.department_id = d.department_id
GROUP BY
    d.department_name
HAVING
    AVG(e.salary) > 60000
ORDER BY
    average_salary DESC;

-- You have two tables: Employees and Departments.

-- Employees table:
-- employee_id (INT, Primary Key)
-- employee_name (VARCHAR)
-- department_id (INT, Foreign Key to Departments)
-- salary (DECIMAL)
-- Departments table:
-- department_id (INT, Primary Key)
-- department_name (VARCHAR)
-- Write a SQL query that returns the department_name, the average_salary for that department, and the highest_salary in that department. Only include departments where the average salary is greater than $60,000. Order the results by average_salary in descending order.

-- Expected Output Columns:

-- department_name
-- average_salary
-- highest_salary

SELECT
    e.employee_name,
    e.salary
FROM
    Employees e
JOIN (
    SELECT
        department_id,
        AVG(salary) AS avg_dept_salary
    FROM
        Employees
    GROUP BY
        department_id
) AS dept_avg ON e.department_id = dept_avg.department_id
WHERE
    e.salary > dept_avg.avg_dept_salary;

-- Description:

-- You have two tables: Products and Sales.

-- Products table:
-- product_id (INT, Primary Key)
-- product_name (VARCHAR)
-- category_id (INT, Foreign Key to Categories)
-- Sales table:
-- sale_id (INT, Primary Key)
-- product_id (INT, Foreign Key to Products)
-- sale_amount (DECIMAL)
-- Categories table:
-- category_id (INT, Primary Key)
-- category_name (VARCHAR)
-- Write a SQL query to find the top 2 products (by total sale_amount) within each category_name. If a category has fewer than 2 products, list all of them. Order the results by category_name and then by total_sales in descending order.

  
  SELECT
    e.employee_name,
    e.salary
FROM
    Employees e
JOIN (
    SELECT
        department_id,
        AVG(salary) AS avg_dept_salary
    FROM
        Employees
    GROUP BY
        department_id
) AS dept_avg ON e.department_id = dept_avg.department_id
WHERE
    e.salary > dept_avg.avg_dept_salary;

//
WITH ProductSales AS (
    SELECT
        p.product_id,
        p.product_name,
        c.category_name,
        SUM(s.sale_amount) AS total_sales
    FROM
        Products p
    JOIN
        Sales s ON p.product_id = s.product_id
    JOIN
        Categories c ON p.category_id = c.category_id
    GROUP BY
        p.product_id, p.product_name, c.category_name
),
RankedProductSales AS (
    SELECT
        product_id,
        product_name,
        category_name,
        total_sales,
        ROW_NUMBER() OVER (PARTITION BY category_name ORDER BY total_sales DESC) AS rn
    FROM
        ProductSales
)
SELECT
    category_name,
    product_name,
    total_sales
FROM
    RankedProductSales
WHERE
    rn <= 2
ORDER BY
    category_name, total_sales DESC;


-- You have the Employees table from previous exercises.

-- Write a SQL query to list all employees, their department_name, salary, and their rank within their respective department based on salary (highest salary gets rank 1). If two employees in the same department have the same salary, they should receive the same rank, and there should be a gap in the ranking for the next distinct salary.

-- Expected Output Columns:

-- employee_name
-- department_name
-- salary
-- salary_rank_in_department

SELECT
    e.employee_name,
    d.department_name,
    e.salary,
    RANK() OVER (PARTITION BY d.department_name ORDER BY e.salary DESC) AS salary_rank_in_department
FROM
    Employees e
JOIN
    Departments d ON e.department_id = d.department_id
ORDER BY
    d.department_name, salary_rank_in_department;

-- Description:

-- You have the Sales and Products tables. Assume the Sales table also has a sale_date column (DATE).

-- Sales table:
-- sale_id (INT, Primary Key)
-- product_id (INT, Foreign Key to Products)
-- sale_amount (DECIMAL)
-- sale_date (DATE)
-- Write a SQL query to calculate the running total of sale_amount for each product_name, ordered by sale_date.

-- Expected Output Columns:

-- product_name
-- sale_date
-- sale_amount
-- cumulative_sales

SELECT
    p.product_name,
    s.sale_date,
    s.sale_amount,
    SUM(s.sale_amount) OVER (PARTITION BY p.product_name ORDER BY s.sale_date) AS cumulative_sales
FROM
    Sales s
JOIN
    Products p ON s.product_id = p.product_id
ORDER BY
    p.product_name, s.sale_date;


-- You have a Customers table and the Sales table (from Exercise 5, but assume customer_id instead of product_id).

-- Customers table:
-- customer_id (INT, Primary Key)
-- customer_name (VARCHAR)
-- Sales table:
-- sale_id (INT, Primary Key)
-- customer_id (INT, Foreign Key to Customers)
-- sale_amount (DECIMAL)
-- sale_date (DATE)
-- Write a SQL query to find the top 3 customers based on their total sale_amount. Use DENSE_RANK() so that if multiple customers have the same total spend, they share the same rank, and there are no gaps in the ranking sequence.

WITH CustomerTotalSales AS (
    SELECT
        c.customer_id,
        c.customer_name,
        SUM(s.sale_amount) AS total_spend
    FROM
        Customers c
    JOIN
        Sales s ON c.customer_id = s.customer_id
    GROUP BY
        c.customer_id, c.customer_name
),
RankedCustomers AS (
    SELECT
        customer_name,
        total_spend,
        DENSE_RANK() OVER (ORDER BY total_spend DESC) AS customer_rank
    FROM
        CustomerTotalSales
)
SELECT
    customer_name,
    total_spend,
    customer_rank
FROM
    RankedCustomers
WHERE
    customer_rank <= 3
ORDER BY
    customer_rank, customer_name;
