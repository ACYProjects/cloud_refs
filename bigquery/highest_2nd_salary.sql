WITH Ranked_Employees AS (
  SELECT
  department,
  salary
  DENSE_RANK() OVER (PARTITION BY department ORDER BY Salary DESC) as salary_rank
  FROM dataset.employees
  )
SELECT 
  department,
  MAX(CASE WHEN rank = 1 THEN salary END) AS highest_salary,
  MAX(CASE WHEN rank = 2 THEN salary END) AS second_highest_salary
FROM Ranked_Employees
GROUP BY department
