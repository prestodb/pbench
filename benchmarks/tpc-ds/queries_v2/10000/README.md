TPC-DS Query Generation Information
====================================
As per TPC-DS Specification v4.0.0 at https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-DS_v4.0.0.pdf

- Engine: presto
- Scale Factor: 1 / 1000 / 10000 (in GB)
- Maximum Stream ID: 20 (generates streams s0 to s20, total 21 streams)
- Random Seed: 01271612345
- Include Variants: false

Sample Output Structure for 10000
---------------------------------
```
10000/
  s0/
  s1/
  s2/
  ...
  s20/
```
Each stream directory contains:
- qNN.sql: Individual query files (NN = 01-99, e.g., q01.sql, q10.sql)

Template Fixes Applied:
------------------------
- 40 total fixes for Presto C++ compatibility
- Date arithmetic converted to INTERVAL syntax
- Subquery aliases added where required
- See TPCDS_FIXES_SUMMARY_PRESTO.md for complete details

Notes:
------
- Seed format per TPC-DS spec: mmddhhmmsss (timestamp of load end time)
- Each stream has different query parameters because of different seed
- All queries for all 21 streams were generated using TPC-DS provided dsqgen tool at one go
- All queries were tested with IBM watsonx.data Presto C++
- Queries in stream s0 are to be used for power run aka single-stream run
- Queries in stream s1 through sN are to be used for throughput test 1 with N streams
- Queries in stream sN+1 through s2*N are to be used for throughput test 2 with N streams
- Queries in a particular stream must be run in exact sequence as mentioned in the specification pdf Appendix D: Query Ordering

For more information, see:
- TPC-DS Specification v4.0.0 at https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-DS_v4.0.0.pdf
- TPCDS_FIXES_SUMMARY_PRESTO.md

