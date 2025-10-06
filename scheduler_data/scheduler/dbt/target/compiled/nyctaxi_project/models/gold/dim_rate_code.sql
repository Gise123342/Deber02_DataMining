



SELECT * FROM (
    SELECT 1 AS rate_code_id, 'Standard rate' AS rate_code_desc UNION ALL
    SELECT 2, 'JFK' UNION ALL
    SELECT 3, 'Newark' UNION ALL
    SELECT 4, 'Nassau or Westchester' UNION ALL
    SELECT 5, 'Negotiated fare' UNION ALL
    SELECT 6, 'Group ride' UNION ALL
    SELECT 99, 'Unknown'
)