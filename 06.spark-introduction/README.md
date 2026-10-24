# Spark RDD lab

Download `bronze/users.csv` and `bronze/orders.csv` from the assigned Onyxia
bucket into the working directory, then run:

```bash
python ../../06.spark-introduction/lab_rdd.py
```

The script prints real counts, samples, partitions, and lineage. Open the
Spark UI at `http://localhost:4040` during execution to inspect stages and
shuffle sizes. `textFile` operates on physical lines, so the quoted multiline
user address creates extra lines. The UUID filter recovers enough of each
user record for the join but cannot reconstruct the address. Orders have one
physical line per record and can be split safely for this dataset.

`reduceByKey` combines quantities within partitions before shuffling;
`groupByKey` shuffles every quantity and then sums. `reduceByKey` also performs
the manual latest-order deduplication. A join may shuffle both keyed RDDs
unless they share compatible partitioning. Avoid `collect()` on the full join
for large data because it transfers all rows to the driver. The script only
collects the small per-user aggregate to verify both methods agree.
