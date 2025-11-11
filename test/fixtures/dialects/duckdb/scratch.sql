-- work file for random tests
-- select columns(c -> c not in ['extraction_id', 'org_id']) from t;
SELECT 'Math' IN ('CS', 'Math'); -- works
SELECT 42 IN (SELECT unnest([32, 42, 52]) AS x); -- works
-- below all fail
SELECT 'Hello' IN 'Hello World';
select 1 in [1,2,3];
SELECT 'key1' IN MAP {'key1': 50, 'key2': 75};