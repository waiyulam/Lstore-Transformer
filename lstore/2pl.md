# DBMS_Transformer
## Two-Phase-Locking
There are two tester files in the m3_tester folder: `contention_tester.py` and `transaction_tester.py`.

In `contention_tester.py`, it includes 5000 `SELECT` and 5000 `INCREMENT`, evenly separated into 8 `transaction_worker`. In line 41,
```python
contention = 2000
```
you can change it from 2 to 2000 for different contention ratio and observe the reporting throughput in the end.

In the provided tester `transaction_tester.py`, it tests the correctness of our implementation.
