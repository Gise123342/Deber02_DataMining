




SELECT * FROM (
    SELECT 1 AS trip_type, 'Street-hail' AS trip_type_desc UNION ALL
    SELECT 2 AS trip_type, 'Dispatch' AS trip_type_desc UNION ALL
    SELECT 99 AS trip_type, 'Unknown' AS trip_type_desc
)