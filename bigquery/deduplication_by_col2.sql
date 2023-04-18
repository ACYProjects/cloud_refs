SELECT *

FROM ( SELECT *,
      ROW_NUMBER() OVER (PARTITION BY col1) row_number
      FROM Accidents
)

WHERE row_number = 1
