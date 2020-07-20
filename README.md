# DBMS_Transformer
**Team**: Waiyu Lam; Wenda Xu; Ye Wang; Zhiwei Zhang; Chu-Hung Cheng   
**Instructor**: [Mohammad Sadoghi](https://expolab.org/)

# Table of Contents 

- [Objectives](#OBJECTIVES)
- [Implementation Details](#IMPLEMENTATION)
  * [Lstore](#LSTORE)
  * [Lstore Fundamentals](#LSTORE_FUNDAMENTALS)
    + [Data Modeling](#Data_Modeling)
    + [Bufferpool Management & Durability](#Bufferpool_Management)
    + [Indexing](#Indexing)
    + [Query Interface](#Query_Interface)
    + [Transaction Semantics & Multithreading Concurrency Control](#Transaction_Semantics)
- [Milestones](#Milestones)
- [Usage](#Usage)
- [Presentation](#Presentation)
- [Sources](#Sources)

## OBJECTIVES  
The overall goal of this milestone is to create a multi-threaded, in-memory
database, based on L-Store, capable of performing simple SQL-like operations    

## IMPLEMENTATION
### LSTORE 
Lineage-based Data Store (L-Store) is a solution that combines the real-time
processing of transactional and analytical workloads within a single unified
engine by introducing a novel update-friendly lineage-based storage
architecture. By exploiting the lineage, we will develop a contention-free and
lazy staging of columnar data from a write-optimized (tail data) form into a
read-optimized (base data) form in a transactionally consistent approach that
supports querying and retaining current and historic data.

### LSTORE_FUNDAMENTALS 
#### Data_Modeling 
1. **Data Storage** : 
   - The key idea of L-Store is to separate the original version of a record
     inserted into the database (a **base record**) and the subsequent updates
     to it (**tail records**). Records are stored in physical pages where a page
     is basically a fixed-size contiguous memory chunk. The **base records** are
     stored in read-only pages called **base pages**. Each **base page** is
     associated with a set of append-only pages called **tail pages** that will
     hold the corresponding tail records, namely, any updates to a record will
     be added to the tail pages and will be maintained as a tail record.The base
     page (or tail page) is a logical concept because physically each base page
     (or tail page) consists of a set of physical pages (4K each, for example),
     one for each column.
   - Data storage in L-Store is **columnar**, meaning that instead of storing
     all fields of a record contiguously, data from different records for the
     same column are stored together.The idea behind this layout is that most
     update and read operations will only affect a small set of columns; hence,
     separating the storage for each column would reduce the amount of
     contention and I/O.Also, the data in each column tends to be homogeneous,
     the data on each page can be compressed more effectively. 
2. **Contention free Merge**: 
   - To alleviate the performance degradation, the tail pages (or tail records)
     are periodically merged with their corresponding base pages (or base
     records) in order to bring base pages “almost”up-to-date.The merge process
     is designed to be contention-free, meaning that it will not interfere with
     reads or writes to the records being merged and does not hinder the normal
     operation of the database.

Lstore Architecture: ![alt
text](https://github.com/waiyulam/Transformer_DBMS/blob/master/Visual/Lstore_architecture.png
"Lstore Architecture")

#### Bufferpool_Management
1. **Bufferpool Management**: 
   - To keep track of data whether, in memory (or disk), we require to have a
     page directory that maps RIDs to pages in memory (or disk) to allow fast
     retrieval of records. Recall records are stored in pages, and records are
     partitioned across page ranges. Given a RID, the page directory returns the
     location of the certain record inside the page within the page range. The
     efficiency of this data structure is a key factor in performance
2. **Durability**:
   - L-Store is no exception and its design is compatible with persisting data
     on disk and coping with limited size bufferpool.
   - The database has open() and close() functions to ensure all data are read
     from and written back to disk respectively.

#### Indexing 
   - we are using B+tree to implement our database indexing . A B+ tree is a
     balanced binary search tree that follows a multi-level index format. The
     leaf nodes of a B+ tree denote actual data pointers. B+ tree ensures that
     all leaf nodes remain at the same height, thus balanced. Additionally, the
     leaf nodes are linked using a link list; therefore, a B+ tree can support
     random access as well as sequential access.

#### Query_Interface
   - simple query capabilitiesthat provided the standard SQL-like
     functionalities, which is also similar to Key-Value Stores (NoSQL). For
     this milestone, you need to provide select, insert, update, delete of a
     single key along with a simple aggregation query, namely, to return the
     summation of a single column for a range of keys.

#### Transaction_Semantics
   1. **Transaction Semantics**: create the concept of the multi-statement
      transaction with the property of either all statements (operations) are
      successfully executed and transaction commits or none will and the
      transaction aborts (i.e., atomicity). 
   2. **Concurrency Protocol**: In this project, we are implementing two type of
      concurrency control and do comparison
      1. **Strict Two Phase Locking**: 
         - **Lock manager** is simple reader-writer lock allow several readers to hold
         the lock simultaneously and XOR one writer 
         - **Strict two phase protocols** divides the execution phase of a transaction
         into two parts. In the first part, when the transaction starts executing,
         it seeks permission for the locks it requires. After acquiring all the
         locks in the first phase, the transaction continues to execute normally.
         Strict-2PL holds all the locks until the commit point and releases all the
         locks at a time.
         - **No wait policy** allow deadlock protectiong and failure lock acquiring
         would cause transaction to be aborted rather than wait
         - **Cons**: many aborts due to high contention and non-deterministic
         concurrency control from transaction execution

      2. **QUECC**:
         - Queue-Oriented, Control-Free, Concurrency Architecture
         - A two parallel & independent phases of priority-driven planning &
         execution. Phase 1: Deterministic priority-based planning of transaction
         operations in parallel. Phase 2: Priority driven execution of plans in
         parallel
         - **Pros**: Efficient, parallel and deterministic in-memory transaction
         processing; Eliminates almost all aborts by resolving transaction conflicts
         a priori; Works extremely well under high-contention workloads

Quecc Architecture: ![alt
text](https://github.com/waiyulam/Transformer_DBMS/blob/master/Visual/Quecc_architecture.png
"Quecc Architecture")

   3. **Experimental Analysis** 
      We focus on evaluating three metrics: throughput, latency, and abort
      percentage. The abort percentage is computed as the ratio between the total
      number of aborted transaction to the sum of the total number of attempted
      transaction (i.e., both aborted and committed transactions).   
      
   Effect of varing contention: ![alt
      text](https://github.com/waiyulam/Transformer_DBMS/blob/master/Visual/Varying_Contention.png
      "Effect of varing contention")

   Effect of worker threads: ![alt
   text](https://github.com/waiyulam/Transformer_DBMS/blob/master/Visual/Varying_Worker_threads.png
   "Effect of worker threads")

## Milestones
1. For the [first
   milestone](https://expolab.org/ecs165a-winter2020/milestones/Milestone1.pdf),
   we will focus on a simplified in-memory (volatile) implementation that
   provides basic relational data storage and querying capabilities. 
2. For the [second
   milestone](https://expolab.org/ecs165a-winter2020/milestones/Milestone2.pdf),
   we will focus on data durability by persisting data on disk (non-volatile)
   and merging the base and tail data
3. For the [third
   milestone](https://expolab.org/ecs165a-winter2020/milestones/Milestone3.pdf),
   we will focus on concurrency and multi-threaded transaction processing.

## Usage 
See Testers for more basic usage of API for lstore database

```python     
import lstore # open database # load table # create table 
db = Database()
db.open('ECS165') # open database 
grades_table = db.create_table('Grades', 5, 0) # create table 
query = Query(grades_table) # create query interface 
query.insert(*args) # insert query 
grades_table.index.create_index(column) # create indexing on specified column
query.update(*args) # update query 
query.sum(*args) # sum query 
query.increment(*args) # increment query 
query.delete(*args) # delete query 
transaction_workers = [] # create transaction workers 
transaction_workers.append(TransactionWorker([]))
transaction = Transaction() # create transaction 
transaction.add_query(query.select, key, 0, [1, 1, 1, 1, 1]) # add one query operation
transaction_workers[].add_transaction(transaction)
db.close() # close database 
```

## Presentation 
1. [Milestone 1: Single-threaded, In-memory
   L-Store](https://drive.google.com/open?id=1sBGjw7HLsv2Hcuy7kA9Z3uK_3b8soYnCuUd_p84hVPQ)
2. [Milestone 2 : Single-threaded, In-memory & Durable
   L-Store](https://drive.google.com/open?id=1faFBJ7XC9DWvd8-cljDpxTuciCkxt9LnNHNbiPlib2A)
3. [Milestone 3 : Multi-threaded, In-memory & Durable
   L-Store](https://drive.google.com/open?id=1zYeYk9-AYiOHw--W7h7ANNJQe0KjU_Lhd13TAmS3h68)

## Sources  
1. [Sadoghi, Mohammad & Bhattacherjee, Souvik & Bhattacharjee, Bishwaranjan &
   Canim, Mustafa. (2018). L-Store: A Real-time OLTP and OLAP System.
   10.5441/002/edbt.2018.65.](https://github.com/waiyulam/Lstore-Transformer/edit/master/lstore.pdf) 
2. Qadah, Thamir & Sadoghi, Mohammad. (2018). QueCC: A Queue-oriented,
   Control-free Concurrency Architecture. 10.1145/3274808.3274810. 
