# DBMS_Transformer
## QueCC
There are two tester files in the m3_tester folder: `contention_tester.py` and `quecc_tester.py`.

In `contention_tester.py`, it includes 5000 `SELECT` and 5000 `UPDATE`, evenly separated into 8 `transaction_worker`. In line 41,
```python
contention = 2000
```
you can change it from 2 to 2000 for different contention ratio and observe the reporting throughput in the end.

In `quecc_tester.py`, it includes two tester types: `One_Thread_Tester()` and `Simple_Tester()`, both included 8 single-type queries. In `One_Thread_Tester()`, it only has one thread and one transaction that includes 8 queries. In `Simple_Tester()`, it has two `transaction_worker`, two transactions per worker and two queries per transaction. You can only test one query type of one tester type at a time by commenting all other testers.

One sample result is displayed in our presentation, where at first it will display the distribution of our eight queries, including command type, command ID, and so on. In the end, it will display the result of the tester, which also includes information of command type, ID, and so on.