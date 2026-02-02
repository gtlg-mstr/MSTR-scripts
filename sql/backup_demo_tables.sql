-- First, ensure the target schema exists
CREATE SCHEMA IF NOT EXISTS adt;

-- Generate ALTER TABLE statements for tables with specific prefix
-- Replace 'your_prefix_' with your actual prefix
SELECT 'ALTER TABLE ' || schemaname || '.' || tablename || ' SET SCHEMA adt;'
FROM pg_tables 
WHERE schemaname = 'public'  -- or your current schema
AND tablename LIKE 'your_prefix_%';

-- Execute the generated statements manually, or use DO block:
DO $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN 
        SELECT schemaname, tablename 
        FROM pg_tables 
        WHERE schemaname = 'public' 
        AND tablename LIKE 'your_prefix_%'
    LOOP
        EXECUTE 'ALTER TABLE ' || rec.schemaname || '.' || rec.tablename || ' SET SCHEMA adt';
        RAISE NOTICE 'Moved table % to adt schema', rec.tablename;
    END LOOP;
END $$;
