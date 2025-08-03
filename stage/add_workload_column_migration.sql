-- Migration script to add workload column to existing pbench_runs table
-- Run this script on existing databases that already have the pbench_runs table

-- Add workload column if it doesn't exist
ALTER TABLE pbench_runs 
ADD COLUMN IF NOT EXISTS workload varchar(255) null;

-- Create index on workload column for better query performance
CREATE INDEX IF NOT EXISTS pbench_runs_workload_index ON pbench_runs (workload);
