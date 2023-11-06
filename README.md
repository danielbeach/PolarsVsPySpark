# PolarsVsPySpark
can Polars crunch 27GBs of data faster than Pyspark?

This code goes with a Substack post about ...
27GBs of flat files stored in s3 … Spark vs Polars on a Linode (cloud) instance.
(data from Backblaze open source hard drive data set) https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data#downloadingTheRawTestData

Rust via Polars -> `34` seconds
PySpark -> `83` seconds

Ubuntu 6CPU and 16GB RAM machine
