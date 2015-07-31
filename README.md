# Various HBase Tests

# FullGcTest
After adding a large row, scanning back that row winds up being empty. After a few attempts it will succeed (all attempts over the same data on an hbase getting no other writes). 

Looking at logs, it seems this happens when there is memory pressure on the client and there are several Full GCs that happen. Then messages that indicate that region locations are being removed from the local client cache:

```
2015-07-31 12:50:24,647 [main] DEBUG org.apache.hadoop.hbase.client.HConnectionManager$HConnectionImplementation  - Removed 192.168.1.131:50981 as a location of big_row_1438368609944,,1438368610048.880c849594807bdc7412f4f982337d6c. for tableName=big_row_1438368609944 from cache
```

Blaming the GC may sound crazy, but if the test is run with -Xms4g -Xmx4g then it always passes on the first scan attempt. Maybe the pause is enough to remove something from the cache, or the client is using weak references somewhere?

More info http://mail-archives.apache.org/mod_mbox/hbase-user/201507.mbox/%3CCAE8tVdnFf%3Dob569%3DfJkpw1ndVWOVTkihYj9eo6qt0FrzihYHgw%40mail.gmail.com%3E


# Running

Have a local hbase running with a large heap (I tested with 10g).

To run the test, make sure you have gradle 2.5, then do 

```
gradle run
```

Or 

```
gradle fatJar
java -Xmx1900m -verbose:gc -jar build/libs/hbase-debugging.jar
```

This will connect to the local hbase, create a new table, add single
large row, then try to scan the table. This usually fails for me
with no results on the first attempt or 2, then succeeds. 

The ouput will have lines like this when the scan fails:

```
Unexpected cell count in scan: 0/99.
Did NOT find the row after 1 attempts. Sleeping and will re-scan.
```

When successful:

```
Scan successful after 2 attempts.
```

I tested with the following client/server combinations:

Repro'ed in:

- 0.98.12 client/server
- 0.98.13 client 0.98.12 server
- 0.98.13 client/server
- 1.1.0 client 0.98.13 server
- 0.98.13 client and 1.1.0 server
- 0.98.12 client and 1.1.0 server

NOT repro'ed in

- 1.1.0 client/server

