---
sidebar_position: 13
---

# Benchmark Results

Live benchmark comparisons between Dekaf and Confluent.Kafka, automatically updated on every commit to main.

**Last Updated:** 2026-02-04 17:30 UTC

:::info
These benchmarks run on GitHub Actions (ubuntu-latest) using BenchmarkDotNet. 
**Ratio < 1.0 means Dekaf is faster than Confluent.Kafka**
:::

## Producer Benchmarks

Comparing Dekaf vs Confluent.Kafka for message production across different scenarios.

| Method                  | Categories    | MessageSize | BatchSize | Mean        | Error       | StdDev      | Median      | Ratio | RatioSD | Gen0     | Gen1    | Allocated  | Alloc Ratio |
|------------------------ |-------------- |------------ |---------- |------------:|------------:|------------:|------------:|------:|--------:|---------:|--------:|-----------:|------------:|
| **Confluent_ProduceBatch**  | **BatchProduce**  | **100**         | **100**       |  **6,195.1 μs** |   **129.34 μs** |    **85.55 μs** |  **6,208.2 μs** |  **1.00** |    **0.02** |        **-** |       **-** |  **106.55 KB** |        **1.00** |
| Dekaf_ProduceBatch      | BatchProduce  | 100         | 100       |  1,430.9 μs |    96.92 μs |    57.68 μs |  1,427.8 μs |  0.23 |    0.01 |        - |       - |   45.13 KB |        0.42 |
|                         |               |             |           |             |             |             |             |       |         |          |         |            |             |
| **Confluent_ProduceBatch**  | **BatchProduce**  | **100**         | **1000**      |  **7,340.5 μs** |    **51.09 μs** |    **30.40 μs** |  **7,353.3 μs** |  **1.00** |    **0.01** |  **62.5000** | **46.8750** | **1062.83 KB** |        **1.00** |
| Dekaf_ProduceBatch      | BatchProduce  | 100         | 1000      |  4,661.4 μs |   232.27 μs |   121.48 μs |  4,642.7 μs |  0.64 |    0.02 |  31.2500 |       - |  512.07 KB |        0.48 |
|                         |               |             |           |             |             |             |             |       |         |          |         |            |             |
| **Confluent_ProduceBatch**  | **BatchProduce**  | **1000**        | **100**       |  **6,579.8 μs** |    **72.04 μs** |    **47.65 μs** |  **6,588.7 μs** |  **1.00** |    **0.01** |   **7.8125** |       **-** |  **194.04 KB** |        **1.00** |
| Dekaf_ProduceBatch      | BatchProduce  | 1000        | 100       |  2,966.9 μs |    27.13 μs |    14.19 μs |  2,964.5 μs |  0.45 |    0.00 |        - |       - |   60.24 KB |        0.31 |
|                         |               |             |           |             |             |             |             |       |         |          |         |            |             |
| **Confluent_ProduceBatch**  | **BatchProduce**  | **1000**        | **1000**      | **12,167.3 μs** |   **161.23 μs** |    **84.33 μs** | **12,131.6 μs** |  **1.00** |    **0.01** | **109.3750** | **46.8750** | **1937.84 KB** |        **1.00** |
| Dekaf_ProduceBatch      | BatchProduce  | 1000        | 1000      | 25,918.9 μs |   333.20 μs |   220.39 μs | 25,935.3 μs |  2.13 |    0.02 |  62.5000 | 31.2500 |  1441.3 KB |        0.74 |
|                         |               |             |           |             |             |             |             |       |         |          |         |            |             |
| **Confluent_FireAndForget** | **FireAndForget** | **100**         | **100**       |    **129.7 μs** |    **13.98 μs** |     **7.31 μs** |    **132.2 μs** |  **1.00** |    **0.08** |   **2.6855** |       **-** |   **44.56 KB** |        **1.00** |
| Dekaf_FireAndForget     | FireAndForget | 100         | 100       |    246.6 μs |   140.46 μs |    92.91 μs |    288.9 μs |  1.91 |    0.70 |   0.2441 |       - |    6.64 KB |        0.15 |
|                         |               |             |           |             |             |             |             |       |         |          |         |            |             |
| **Confluent_FireAndForget** | **FireAndForget** | **100**         | **1000**      |  **1,317.5 μs** |   **102.44 μs** |    **53.58 μs** |  **1,332.2 μs** |  **1.00** |    **0.06** |  **25.3906** |       **-** |  **421.77 KB** |        **1.00** |
| Dekaf_FireAndForget     | FireAndForget | 100         | 1000      |  1,620.3 μs | 1,650.78 μs | 1,091.89 μs |    802.9 μs |  1.23 |    0.79 |   3.9063 |       - |    75.2 KB |        0.18 |
|                         |               |             |           |             |             |             |             |       |         |          |         |            |             |
| **Confluent_FireAndForget** | **FireAndForget** | **1000**        | **100**       |          **NA** |          **NA** |          **NA** |          **NA** |     **?** |       **?** |       **NA** |      **NA** |         **NA** |           **?** |
| Dekaf_FireAndForget     | FireAndForget | 1000        | 100       |  2,450.8 μs |    25.57 μs |    15.22 μs |  2,450.5 μs |     ? |       ? |   0.9766 |       - |   27.58 KB |           ? |
|                         |               |             |           |             |             |             |             |       |         |          |         |            |             |
| **Confluent_FireAndForget** | **FireAndForget** | **1000**        | **1000**      |          **NA** |          **NA** |          **NA** |          **NA** |     **?** |       **?** |       **NA** |      **NA** |         **NA** |           **?** |
| Dekaf_FireAndForget     | FireAndForget | 1000        | 1000      | 24,453.9 μs |   892.88 μs |   590.58 μs | 24,162.3 μs |     ? |       ? |  15.6250 |       - |   277.1 KB |           ? |
|                         |               |             |           |             |             |             |             |       |         |          |         |            |             |
| **Confluent_ProduceSingle** | **SingleProduce** | **100**         | **100**       |  **5,434.4 μs** |    **17.76 μs** |    **11.74 μs** |  **5,435.2 μs** |  **1.00** |    **0.00** |        **-** |       **-** |    **1.19 KB** |        **1.00** |
| Dekaf_ProduceSingle     | SingleProduce | 100         | 100       |  1,100.6 μs |     1.48 μs |     0.88 μs |  1,100.6 μs |  0.20 |    0.00 |        - |       - |    3.73 KB |        3.13 |
|                         |               |             |           |             |             |             |             |       |         |          |         |            |             |
| **Confluent_ProduceSingle** | **SingleProduce** | **100**         | **1000**      |  **5,445.0 μs** |    **27.06 μs** |    **17.90 μs** |  **5,451.5 μs** |  **1.00** |    **0.00** |        **-** |       **-** |    **1.19 KB** |        **1.00** |
| Dekaf_ProduceSingle     | SingleProduce | 100         | 1000      |  1,102.9 μs |    14.37 μs |     7.52 μs |  1,100.2 μs |  0.20 |    0.00 |        - |       - |    3.73 KB |        3.13 |
|                         |               |             |           |             |             |             |             |       |         |          |         |            |             |
| **Confluent_ProduceSingle** | **SingleProduce** | **1000**        | **100**       |  **5,443.1 μs** |    **26.41 μs** |    **17.47 μs** |  **5,446.4 μs** |  **1.00** |    **0.00** |        **-** |       **-** |    **2.06 KB** |        **1.00** |
| Dekaf_ProduceSingle     | SingleProduce | 1000        | 100       |  1,361.9 μs |    44.20 μs |    29.24 μs |  1,368.1 μs |  0.25 |    0.01 |        - |       - |    3.73 KB |        1.81 |
|                         |               |             |           |             |             |             |             |       |         |          |         |            |             |
| **Confluent_ProduceSingle** | **SingleProduce** | **1000**        | **1000**      |  **5,425.3 μs** |    **15.54 μs** |    **10.28 μs** |  **5,424.5 μs** |  **1.00** |    **0.00** |        **-** |       **-** |    **2.06 KB** |        **1.00** |
| Dekaf_ProduceSingle     | SingleProduce | 1000        | 1000      |  1,352.6 μs |    41.67 μs |    27.57 μs |  1,356.7 μs |  0.25 |    0.00 |        - |       - |    3.79 KB |        1.84 |

Benchmarks with issues:
  ProducerBenchmarks.Confluent_FireAndForget: Job-IEXEXW(IterationCount=10, RunStrategy=Throughput, WarmupCount=3) [MessageSize=1000, BatchSize=100]
  ProducerBenchmarks.Confluent_FireAndForget: Job-IEXEXW(IterationCount=10, RunStrategy=Throughput, WarmupCount=3) [MessageSize=1000, BatchSize=1000]


## Consumer Benchmarks

Comparing Dekaf vs Confluent.Kafka for message consumption.

| Method               | Categories | MessageCount | MessageSize | Mean    | Error    | StdDev   | Ratio | Allocated  | Alloc Ratio |
|--------------------- |----------- |------------- |------------ |--------:|---------:|---------:|------:|-----------:|------------:|
| **Confluent_ConsumeAll** | **ConsumeAll** | **100**          | **100**         | **3.180 s** | **0.0031 s** | **0.0005 s** |  **1.00** |   **74.92 KB** |        **1.00** |
| Dekaf_ConsumeAll     | ConsumeAll | 100          | 100         | 3.017 s | 0.0054 s | 0.0014 s |  0.95 |  119.18 KB |        1.59 |
|                      |            |              |             |         |          |          |       |            |             |
| **Confluent_ConsumeAll** | **ConsumeAll** | **100**          | **1000**        | **3.176 s** | **0.0031 s** | **0.0005 s** |  **1.00** |  **250.98 KB** |        **1.00** |
| Dekaf_ConsumeAll     | ConsumeAll | 100          | 1000        | 3.015 s | 0.0028 s | 0.0007 s |  0.95 |   305.3 KB |        1.22 |
|                      |            |              |             |         |          |          |       |            |             |
| **Confluent_ConsumeAll** | **ConsumeAll** | **1000**         | **100**         | **3.177 s** | **0.0023 s** | **0.0006 s** |  **1.00** |  **602.55 KB** |        **1.00** |
| Dekaf_ConsumeAll     | ConsumeAll | 1000         | 100         | 3.016 s | 0.0028 s | 0.0007 s |  0.95 |  365.15 KB |        0.61 |
|                      |            |              |             |         |          |          |       |            |             |
| **Confluent_ConsumeAll** | **ConsumeAll** | **1000**         | **1000**        | **3.177 s** | **0.0023 s** | **0.0006 s** |  **1.00** | **2368.19 KB** |        **1.00** |
| Dekaf_ConsumeAll     | ConsumeAll | 1000         | 1000        | 3.016 s | 0.0019 s | 0.0005 s |  0.95 | 3163.89 KB |        1.34 |
|                      |            |              |             |         |          |          |       |            |             |
| **Confluent_PollSingle** | **PollSingle** | **100**          | **100**         | **3.181 s** | **0.0030 s** | **0.0008 s** |  **1.00** |   **16.18 KB** |        **1.00** |
| Dekaf_PollSingle     | PollSingle | 100          | 100         | 3.014 s | 0.0070 s | 0.0011 s |  0.95 |   111.6 KB |        6.90 |
|                      |            |              |             |         |          |          |       |            |             |
| **Confluent_PollSingle** | **PollSingle** | **100**          | **1000**        | **3.181 s** | **0.0025 s** | **0.0006 s** |  **1.00** |   **17.94 KB** |        **1.00** |
| Dekaf_PollSingle     | PollSingle | 100          | 1000        | 3.015 s | 0.0056 s | 0.0014 s |  0.95 |  643.24 KB |       35.86 |
|                      |            |              |             |         |          |          |       |            |             |
| **Confluent_PollSingle** | **PollSingle** | **1000**         | **100**         | **3.182 s** | **0.0020 s** | **0.0005 s** |  **1.00** |   **15.62 KB** |        **1.00** |
| Dekaf_PollSingle     | PollSingle | 1000         | 100         | 3.014 s | 0.0031 s | 0.0008 s |  0.95 |  118.62 KB |        7.60 |
|                      |            |              |             |         |          |          |       |            |             |
| **Confluent_PollSingle** | **PollSingle** | **1000**         | **1000**        | **3.180 s** | **0.0038 s** | **0.0010 s** |  **1.00** |   **17.96 KB** |        **1.00** |
| Dekaf_PollSingle     | PollSingle | 1000         | 1000        | 3.015 s | 0.0021 s | 0.0003 s |  0.95 | 1158.58 KB |       64.51 |


## Protocol Benchmarks

Zero-allocation wire protocol serialization/deserialization.

:::tip
**Allocated = `-` means zero heap allocations** - the goal of Dekaf's design!
:::

| Method                           | Mean      | Error     | StdDev    | Allocated |
|--------------------------------- |----------:|----------:|----------:|----------:|
| &#39;Write 1000 Int32s&#39;              | 22.292 μs | 8.3130 μs | 4.9470 μs |         - |
| &#39;Write 100 Strings (100 chars)&#39;  | 11.611 μs | 3.9739 μs | 2.0784 μs |         - |
| &#39;Write 100 CompactStrings&#39;       | 14.960 μs | 0.6465 μs | 0.3847 μs |         - |
| &#39;Write 1000 VarInts&#39;             | 33.789 μs | 8.5268 μs | 5.0742 μs |         - |
| &#39;Read 1000 Int32s&#39;               | 23.912 μs | 8.7234 μs | 5.1912 μs |         - |
| &#39;Read 1000 VarInts&#39;              | 23.383 μs | 9.7344 μs | 5.7928 μs |         - |
| &#39;Write RecordBatch (10 records)&#39; | 16.494 μs | 1.2616 μs | 0.7508 μs |         - |
| &#39;Read RecordBatch (10 records)&#39;  |  5.074 μs | 0.7238 μs | 0.4787 μs |         - |


## Serializer Benchmarks

| Method                               | Mean        | Error      | StdDev   | Ratio | RatioSD | Allocated | Alloc Ratio |
|------------------------------------- |------------:|-----------:|---------:|------:|--------:|----------:|------------:|
| &#39;Serialize String (10 chars)&#39;        |  1,986.0 ns |   385.5 ns | 229.4 ns |  0.45 |    0.12 |         - |          NA |
| &#39;Serialize String (100 chars)&#39;       |  2,747.7 ns |   435.2 ns | 259.0 ns |  0.63 |    0.15 |         - |          NA |
| &#39;Serialize String (1000 chars)&#39;      |  1,894.7 ns |   722.4 ns | 477.8 ns |  0.43 |    0.15 |         - |          NA |
| &#39;Deserialize String&#39;                 |  3,077.3 ns |   618.6 ns | 368.1 ns |  0.70 |    0.18 |         - |          NA |
| &#39;Serialize Int32&#39;                    |    467.0 ns |   240.5 ns | 143.1 ns |  0.11 |    0.04 |         - |          NA |
| &#39;Serialize 100 Messages (key+value)&#39; | 29,704.4 ns |   889.2 ns | 465.1 ns |  6.79 |    1.55 |         - |          NA |
| &#39;ArrayBufferWriter + Copy&#39;           |  4,563.1 ns | 1,358.5 ns | 898.6 ns |  1.04 |    0.31 |         - |          NA |
| &#39;PooledBufferWriter Direct&#39;          |  3,089.9 ns |   179.5 ns | 106.8 ns |  0.71 |    0.16 |         - |          NA |


## Compression Benchmarks

| Method                  | Mean         | Error      | StdDev     | Median       | Allocated |
|------------------------ |-------------:|-----------:|-----------:|-------------:|----------:|
| &#39;Snappy Compress 1KB&#39;   |    18.127 μs | 13.3719 μs |  7.9574 μs |    13.444 μs |   77672 B |
| &#39;Snappy Compress 1MB&#39;   |   465.403 μs | 30.6585 μs | 18.2444 μs |   453.570 μs |   78392 B |
| &#39;Snappy Decompress 1KB&#39; |     8.938 μs |  0.7549 μs |  0.4493 μs |     8.815 μs |         - |
| &#39;Snappy Decompress 1MB&#39; | 2,262.965 μs | 79.7456 μs | 52.7468 μs | 2,240.738 μs |         - |


---

## How to Read These Results

- **Mean**: Average execution time
- **Error**: Half of 99.9% confidence interval
- **StdDev**: Standard deviation of all measurements
- **Ratio**: Performance relative to baseline (Confluent.Kafka)
  - `< 1.0` = Dekaf is faster
  - `> 1.0` = Confluent is faster
  - `1.0` = Same performance
- **Allocated**: Heap memory allocated per operation
  - `-` = Zero allocations (ideal!)

*Benchmarks are automatically run on every push to main.*