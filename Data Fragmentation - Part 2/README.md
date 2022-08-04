# Assignment Overview
### Fragmenting a database means requires query processing to fetch the required data in multiple fragments or even all fragments.

## Project Instructions
- Implement the point and range queries in Python for  the (range and round-robin) fragmented  tables from the previous assignment.

- For simplicity, a metadata table with the names and number of fragments has been created. Typically you should now how to create this table yourself.

## Files
- Range and round-robin fragmentation functions, as well as the metadata table, are included in the `interface.py`, so you just need to complete the point and range query functions. The results should be returned as an output text file.
- A few queries are added to `tester.py`, which validates the output text files of both queries.  