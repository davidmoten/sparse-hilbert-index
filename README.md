# sparse-hilbert-index
<a href="https://travis-ci.org/davidmoten/sparse-hilbert-index"><img src="https://travis-ci.org/davidmoten/sparse-hilbert-index.svg"/></a><br/>
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.davidmoten/sparse-hilbert-index/badge.svg?style=flat)](https://maven-badges.herokuapp.com/maven-central/com.github.davidmoten/sparse-hilbert-index)<br/>
[![codecov](https://codecov.io/gh/davidmoten/sparse-hilbert-index/branch/master/graph/badge.svg)](https://codecov.io/gh/davidmoten/sparse-hilbert-index)<br/>

Java library to create and search random access files (including in S3) using the space-filling hilbert index (sparse). More sensationally described as **Turn Amazon S3 into a spatio-temporal database!**

**Status**: *deployed to Maven Central*

<img align="right" src="src/docs/david-hilbert.png"/>

Range queries are straightforward with a B/B+ tree with a single dimension. Range queries across 2 spatial dimensions are normally handled by R-tree data structures. Range queries across 3 or more dimensions are usually implemented by mapping the space to a single dimensional space-filling curve and then making use of B/B+ tree indexes.

Space filling curves include the Hilbert curve, Z-Order curve, Peano curve and many others.

The Hilbert Curve is generally favoured for multi-dimensional range queries over the Z-Order Curve for instance because of its superior clustering properties (the Hilbert index offers efficiencies for clustered data). 

This library offers revolutionary **ease-of-use** to create and search the Hilbert Curve index via random access storage (like flat files or AWS S3 or Azure Blob Storage).

Querying spatio-temporal data is potentially a terrific application for a Hilbert index. this library supports any number of dimensions and is not opionated about the real life significance of the individual dimensions though it will work better for querying if the ordinates vary (lots of fixed values means more data needs to be scanned in searches).

Critical to the creation of this library were these libraries by the same author:

* [big-sorter](https://github.com/davidmoten/big-sorter)
* [hilbert-curve](https://github.com/davidmoten/sparse-hilbert-curve)

**Features**
* sorts input file based on hilbert index (sorts arbitrarily large files using [big-sorter](https://github.com/davidmoten/big-sorter))
* creates sparse hilbert index in separate file
* enables random access search of sorted input file using index file
* S3 supports `Range` request header so can do random access
* streaming search api for efficiency
* supports 2 or more numeric dimensions of any range

**Status**: *in development*

## Getting started
Add this maven dependency to your pom.xml:

```xml
<dependency>
  <groupId>com.github.davidmoten</groupId>
  <artifactId>sparse-hilbert-curve</artifactId>
  <version>VERSION_HERE</version>
</dependency>

```
## Example (binary)

Suppose we have a 400MB binary input file with 11.4m ship position fixed-size records (35 bytes each) around Australia for 24 hours and lat, long and time are in those records (stored as float, float, long). I'm going to use a hilbert curve with 10 bits per dimension and 3 dimensions to make a hilbert curve index for this file, store the sorted file and its index and test the speed that I can do spatio-temporal queries on this data.

### Step 1: Define your serialization method
In our case we are reading fixed size byte array records so we can use one of the built-in `Serializer`s:
```java
Serializer<byte[]> serializer = Serializer.fixedSizeRecord(35);
```
### Step 2: Define how the spatio-temporal point is determined for each record
So given a 35 byte array we calculate the lat, long and time fields as below:

```java
Function<byte[], double[]> pointMapper = b -> {
        ByteBuffer bb = ByteBuffer.wrap(b);
        bb.position(4);
        float lat = bb.getFloat();
        float lon = bb.getFloat();
        long time = bb.getLong();
        return new double[] { lat, lon, time };
    };
```
### Step 3: Prepare the sorted file and the index for deployment to S3

```java
File input = ...
File output = ...
File idx = ...

Index<byte[]> index = Index
  Index
    .serializer(serializer) 
    .pointMapper(pointMapper) 
    .input(input) 
    .output(output) 
    .bits(10) 
    .dimensions(3) 
    .createIndex() 
    .write(idx);
```
Output:
```
2019-06-11 22:01:57.7+1000 starting sort
2019-06-11 22:01:58.4+1000 total=100000, sorted 100000 records to file big-sorter693675191372766305 in 0.641s
2019-06-11 22:01:58.9+1000 total=200000, sorted 100000 records to file big-sorter3205436568395107978 in 0.557s
2019-06-11 22:01:59.4+1000 total=300000, sorted 100000 records to file big-sorter3777831275849039312 in 0.486s
...
2019-06-11 22:03:14.6+1000 total=11500000, sorted 100000 records to file big-sorter6542117355878068811 in 0.556s
2019-06-11 22:03:15.3+1000 total=11600000, sorted 100000 records to file big-sorter2584017367351262913 in 0.688s
2019-06-11 22:03:15.8+1000 total=11665226, sorted 65226 records to file big-sorter6344821858069260750 in 0.430s
2019-06-11 22:03:15.8+1000 completed inital split and sort, starting merge
2019-06-11 22:03:15.9+1000 merging 100 files
2019-06-11 22:04:16.2+1000 merging 17 files
2019-06-11 22:04:22.3+1000 merging 2 files
2019-06-11 22:04:30.3+1000 sort of 11665226 records completed in 152.635s
```
### Step 4: Upload the sorted data and the index file 
Righto, now you have the `output` and `idx` files that you can deploy to an S3 bucket (or a local filesystem if you want). For convenience you might use a public S3 bucket to test some non-sensitive data without authentication.

### Step 5: Search the remote data file using the index
Let's do a search on the data and for demonstration purposes we'll reread the Index from the idx file.

```java
// stream the index file from S3
InputStream inputStream = ...
Index<byte[]> index = 
  Index
    .serializer(serializer)
    .pointMapper(pointMapper)
    .read(inputStream);
// corners of the query box for an hour in the Sydney region
double[] a = new double[] { -33.68, 150.86, t1 };
double[] b = new double[] { -34.06, 151.34, t2 };
long count = index.search(a, b).url(url).count().get();
```

The default index size is 10k entries which produces a file of about 80K.

Here are some sample runtimes for three scenarios when we search the Sydney region for 1 hour:
* search a local file (SSD): 59ms
* full scan of a local file (SSD): 932ms 
* search a remote file (S3): 326ms + 483ms to load index file = 809ms

The default index size is 10k entries which produces a file of about 80K.

**Test environment**
* Bandwidth to S3 is 4.6MB/s for the test
* Time to First Byte (TTFB) thought to be 150-210ms
* all calls made serially (no concurrency)

**Search speed**

| Region | Time Window (hours) |Found |  Elapsed Time (ms) |
| :---         |     ---:      |  --: |         ---: |
| Sydney   | 1     |  1667  | 326|
| Sydney     | 24      | 35337      | 2517 |
| Brisbane     | 1      | 38319      | 875 |
| Queensland     | 1      | 166229      | 3258 |
| Tasmania     | 1      | 6255      | 609 |
| Tasmania     | 6      | 60562      | 3245 |

In short if your search window is reasonably limited in time and space (so that you are not returning some large proportion of the total dataset) then performance is good (326ms for the Sydney area search would provide a good user experience in a UI for example). Interestingly I found that if one dimension is unconstrained (like time) then search performance is still good. When two dimensions are unconstrained (searching a large geographic area for a small time window) then search performance is more on a par with a full file scan.

## Example (CSV)
Here's an example with CSV (making use of the [*csv-commons*](https://github.com/apache/commons-csv) dependency):

```java
Serializer<CSVRecord> serializer = Serializer.csv( 
  CSVFormat.DEFAULT
   .withFirstRecordAsHeader()
   .withRecordSeparator("\n"),
  StandardCharsets.UTF_8);

Function<CSVRecord, double[]> pointMapper = rec -> {
        // random access means the csv reader doesn't
        // read the header so we have to use index positions
        double lat = Double.parseDouble(rec.get(1));
        double lon = Double.parseDouble(rec.get(2));
        double time = Long.parseLong(rec.get(3));
        return new double[] { lat, lon, time };
    };
    
Index
  .serializer(serializer)
  .pointMapper(pointMapper)
  .input(new File("input.csv"))
  .output(new File("sorted.csv"))
  .bits(10)
  .dimensions(3)
  .numIndexEntries(10000)
  .createIndex(new File("sorted.csv.idx"));
```

## Streaming
This library uses streaming apis to ensure efficiency and to close resources automatically. `java.util.stream.Stream` objects are single use only and do not implement the concept of disposal so a synchronous-only reusable stream library with disposal semantics called [kool](https://github.com/davidmoten/kool) is used.

Streaming is useful so that for instance when reading a chunk from the source file as soon as the calculated hilbert index for a read record is greater than the max hilbert index searched for we stop reading and close the InputStream. It also enables user defined cancellation by being able to use `results.filter(x -> likeable(x)).first()` for example to cancel reading as soon as a record is found that matches a criterion.

## Index Creation Algorithm
Here is some more implementation detail for this library. The steps for index creation are:

1. Scan the input data to obtain the range of values used in each dimension. Each dimensions range will be used to map each dimension on to the range of values used by an ordinate of the hilbert curve (0..2<sup>bits</sup>-1).

2. Sort the data based on the hilbert index of each point mapped from each record.  Note that the hilbert index is calculated using the java library [hilbert-curve](https://github.com/davidmoten/sparse-hilbert-curve). This library can calculate 3 million indexes a second so we don't store the hilbert index with the associate record but instead calculate it on-demand.

3. Create a sparse index (a binary file) for the sorted data file. This is essentially a map of index value to file position.

## Why would I use this library?
That's a good question! Especially as AWS offer Athena on CSV files (and other formats) in S3 buckets that can can do a full scan of a 2GB CSV file in 1.5 seconds! 

The *sparse-hilbert-index* approach may appeal when you consider the costs of running many many indexed searches across a lot of data. Athena costs are low ($US5/TB data scanned for queries) but may become significant at some scale. In some cases the consequent power consumption from doing a lot of full scan searches may also be ethically challenging. I think it's hard to compete with Athena on big file search but there may be some edge cases that favour *sparse-hilbert-index*! 

To add fuel to the fire, Athena supports the Parquet format which can be indexed such that every page has min-max statistics. If you sort the data on the field you want to query (in our case we would add a calculated hilbert index column) then Athena can do indexed lookups itself (untested). Athena still has to look at the statistics for every page (1Mb by default) so it's not quite as efficient theoretically as *sparse-hilbert-index* that knows exactly what pages to search. This is clearly a topic for further investigation!

If you have a use case that favours *sparse-hilbert-index* in some way then let me know!
