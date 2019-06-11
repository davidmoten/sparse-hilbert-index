# sparse-hilbert-index
Java library to create and search random access files (including in S3) using the space-filling hilbert index (sparse). More sensationally described as *Turn Amazon S3 into a spatio-temporal database!*.

Features
* sorts input file based on hilbert index
* creates sparse hilbert index in separate file
* enables random access search of sorted input file using index file
* S3 supports `Range` request header so can do random access

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
Bounds bounds = Bounds.create(a, b);
long count = index.search(bounds, url).count().get();

```
Here are some sample runtimes for three scenarios:
* search a local file (SSD)
* full scan of a local file (SSD)
* search a remote file (S3)

**Test environment**
* Bandwidth to S3 is 4.6MB/s for the test
* Time to First Byte (TTFB) though to be 150-210ms
* all calls made serially (no concurrency)

| Region | Time Window (hours) |Found |  Elapsed Time (ms) |
| :---         |     ---:      |  --: |         ---: |
| Sydney   | 1     |  1667  | 326|
| Sydney     | 24      | 35337      | 2517 |
| Brisbane     | 1      | 38319      | 875 |
| Queensland     | 1      | 166229      | 3258 |
| Tasmania     | 1      | 6255      | 609 |
| Tasmania     | 6      | 60562      | 3245 |

