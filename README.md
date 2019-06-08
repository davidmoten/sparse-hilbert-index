# sparse-hilbert-index
Java library to create and search random access files (including in S3) using the space-filling hilbert index (sparse) 

Supports
* sorts input file based on hilbert index
* creates sparse hilbert index in separate file
* enables random access search of sorted input file using index file
* S3 supports `Range` request header so can do random access