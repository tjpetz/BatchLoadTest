# BatchLoadTest

This simple demo program shows the impact of batch loading a set of records from a file.
Various batch sizes are run in sequence to measure the performance of the batch size on 
the speed of the load.

Any CSV file can be used for testing.  The specific file used for testing in this instance
is from https://catalog.data.gov/dataset/places-local-data-for-better-health-census-tract-data-2020-release-3e822.

While the optimal batch size will likely vary based on the configuration of your SQL Server.  In the test
instance of SQL Server 2022 Developer Edition running on a small VM with 6GB or RAM the optimal batch size
was 2048.  Above this size the performance did not improved and dropped off a bit for very large batch sizes.
Below a batch size of 1024 there was a marked drop off in performance.