# Relational-Database
Relational database Manager

Record Based File Manager(rbf) folder contains the code that implements File and Access Maanger and the record manager.
It includes code for file operations(creation, deletion etc) and record level operations(add/delete/update/scan record/records).

Relational Manager(rm) contains the code for table operations(Creation/Deletion of a table, insert/delete/update a record in a table, 
table scan etc).

Index Manager(ix) contains the code for creation/deletion of indexes(Done using B+ tree) on the fields of the table and adding/removing data to/from the indices. 
It also supports scan using conditional operators.

Query Engine(qe) supports basic SQL query operations such as selection, projection,aggregate, join on the table and the index created using the above


