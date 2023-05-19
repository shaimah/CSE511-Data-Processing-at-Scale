# Assignment Overview
### The benefits of parallel computing extend to Distributed Database Systems. Leading e-commerce companies use a distributed database as it allows mutltiple servers to run queries by their customers. 

### Given that portions of the data are stored in different sites, both data fragmentation and replication models are used to enable such data distribution. 

### This assignment is an implementation of data fragmentation and partitioning of a given user movie ratings database using round-robin and range partitioning.

## Project Instructions
- Connect to PostgresSQL using the Psycopg2 library in Python. This enables loading the .dat movie rating file into the database.
- Implement the range and round-robin functions, which should iterate through each tuple in the main table relation and place it into the right table fragment.
- Implement additional range- and round-robin-insert functions that enables the insertion from new tuples (not from the main relation table) into the right table fragment. This requires either creating a metatable to keep count of the number of partitions or you can simply access the database and check the number of partitions.

## Files
- The given [dataset](https://files.grouplens.org/datasets/movielens/ml-10m-README.html) contains over 10M ratings applied to 10,681 movies by 71,567 users of the online movie recommender service MovieLens. 
- `Interface.py` contains the main script. `test_data.txt` is the test dataset, and `tester.py` with ` testHelper.py` maybe used to ensure that the implemented functions are working as intented.
-  You only need the `ratings.dat` to complete the assignment. However, in case you want additional attributes such as the movie title or movie tags, you can use `movies.dat` and `tags.dat` to join the extra attributes. 
Note: the `ratings.dat` is to big to uploaded, but you may acccess it [here](https://files.grouplens.org/datasets/movielens/ml-1m.zip).
