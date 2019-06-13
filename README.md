# sparse-hilbert-index
Java library to create and search random access files (including in S3) using the space-filling hilbert index (sparse). More sensationally described as *Turn Amazon S3 into a spatio-temporal database!*.

**Features**
* sorts input file based on hilbert index (sorts arbitrarily large files using [big-sorter](https://github.com/davidmoten/big-sorter))
* creates sparse hilbert index in separate file
* enables random access search of sorted input file using index file
* S3 supports `Range` request header so can do random access
* streaming search api for efficiency

**Status**: *in development*

## Getting started
TODO

## Algorithm

## Example

Suppose we have a 400MB binary input file with 11.4m ship position fixed-size records (35 bytes each) around Australia for one day and lat, long and time are in those records (stored as float, float, long). I'm going to use a hilbert curve with 10 bits per dimension and 3 dimensions to make a hilbert curve index for this file, store the sorted file and its index and test the speed that I can do spatio-temporal queries on this data.

### Step 1: Prepare the sorted file and the index for deployment to S3

```java
Serializer<byte[]> serializer = Serializer.fixedSizeRecord(35);
Function<byte[], double[]> pointMapper = b -> {
        ByteBuffer bb = ByteBuffer.wrap(b);
        bb.position(4);
        float lat = bb.getFloat();
        float lon = bb.getFloat();
        long time = bb.getLong();
        return new double[] { lat, lon, time };
    };
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
### Step 2: Upload the sorted data and the index file 
Righto, now you have the `output` and `idx` files that you can deploy to an S3 bucket (or a local filesystem if you want). For convenience you might use a public S3 bucket to test some non-sensitive data without authentication.

### Step 3: Search the remote data file using the index
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

## Streaming
This library uses streaming apis to ensure efficiency and to close resources automatically. `java.util.stream.Stream` objects are single use only and do not implement the concept of disposal so a synchronous-only reusable stream library with disposal semantics called [kool](https://github.com/davidmoten/kool) is used.

Streaming is useful so that for instance when reading a chunk from the source file as soon as the calculated hilbert index for a read record is greater than the max hilbert index searched for we stop reading and close the InputStream. It also enables user defined cancellation by being able to use `results.filter(x -> likeable(x)).first()` for example to cancel reading as soon as a record is found that matches a criterion.

## Why would I use this library?
That's a good question! Especially as AWS offer Athena on CSV files (and other formats) in S3 buckets that can can do a full scan of a 2GB file in 1.5 seconds! 

The *sparse-hilbert-index* approach may appeal when you consider the costs of running many many indexed searches across 100s of 2GB files in parallel. Athena costs are low ($US5/TB data scanned for queries). In some cases the consequent power consumption from doing a lot of full scan searches may also be ethically challenging. I think it's hard to compete with Athena on big file search but there may be some edge cases that favour *sparse-hilbert-index*! 

To add fuel to the fire, Athena supports the Parquet format which can be indexed such that every page has min-max statistics. If you sort the data on the field you want to query (in our case we would add a calculated hilbert index column) then Athena can do indexed lookups itself (untested). Athena still has to look at the statistics for every page (1Mb by default) so it's not quite as efficient theoretically as *sparse-hilbert-index* that knows exactly what pages to search. This is clearly a topic for further investigation!

If you have a use case that favours *sparse-hilbert-index* in some way then let me know!
