# Parallel Word Counting in a Text Corpus with MPI

This repository contains MPI-based implementations for parallel word counting in a text corpus.

The project extends the provided sequential baseline by implementing two parallel MPI solutions:

* **MPI Version 1:** Static workload distribution
* **MPI Version 2:** Improved load balancing strategy

The objective is to analyze the effect of parallelism and workload distribution on execution time, speedup, efficiency, and load balance.

---

# 1. Title and Team Information

* **Course:** Parallel Computing / MPI Lab
* **University:** Universidad del Norte
* **Team Members:**

  * Add team members here

---

# 2. Problem Description

The dataset is composed of:

* A query file named `consulta.txt`
* Multiple text files named `file_XXXX.txt`

The goal is to:

1. Count how many times each word from `consulta.txt` appears in the corpus
2. Merge the partial counts
3. Report the top 10 most frequent words

The repository includes:

* Dataset generator
* Sequential baseline implementation
* MPI implementation version 1
* MPI implementation version 2
* Automated experiment execution script

---

# 3. Environment and Execution Instructions

## Requirements

* Docker
* MPI (included inside the provided Docker image)
* Python 3
* `mpi4py`

---

## Dataset Generation

The dataset is generated using the provided `generator.py` script.

### Local execution

```bash
python generator.py
```

### Docker execution (Linux / macOS)

```bash
docker run --rm -v "$(pwd)":/app augustosalazar/slim-mpi:2 python /app/generator.py
```

### Docker execution (Windows)

```bash
docker run --rm -v "%cd%:/app" augustosalazar/slim-mpi:2 python /app/generator.py
```

The script creates a `dataset/` directory containing:

* `consulta.txt`
* Multiple files named `file_XXXX.txt`

The generated words are obtained from `spanish_words.info`.

---

---

# Sequential Baseline

The file `baseline_secuencial.py` contains the reference sequential implementation.

The program:

1. Reads the words from `consulta.txt`
2. Searches all corpus files
3. Counts word occurrences
4. Reports the top 10 most common words

The sequential implementation is used as:

* Correctness baseline
* Timing baseline
* Reference for speedup and efficiency calculations

---

## Execution

### Local execution

```bash
python baseline_secuencial.py
```

### Docker execution (Linux / macOS)

```bash
docker run --rm -v "$(pwd)":/app augustosalazar/slim-mpi:2 python /app/baseline_secuencial.py
```

### Docker execution (Windows)

```bash
docker run --rm -v "%cd%:/app" augustosalazar/slim-mpi:2 python /app/baseline_secuencial.py
```

---

## Example Output

```text
Tiempo de ejecución: 5.271514 segundos

Dataset procesado: dataset
Archivo de consulta: consulta.txt
Archivos procesados: 3000
Total de tokens leídos: 44951458
Total de ocurrencias encontradas: 3631778

Top 10 palabras de consulta en el corpus:
  a: 785774
  para: 392156
  sus: 228913
  otros: 105530
  ante: 99832
  unos: 88794
  otra: 83901
  vosotros: 61617
  mios: 58420
  tuya: 56635
```

---

# MPI Version 1

## Description

The first MPI implementation distributes files statically among processes.

### Workflow

1. Rank 0 reads `consulta.txt`
2. Query words are broadcast to all processes
3. Rank 0 obtains the list of corpus files
4. Files are distributed statically among processes
5. Each process counts local occurrences
6. Partial results are gathered in rank 0
7. Rank 0 builds the global result and prints the top 10

---

## Main MPI Operations Used

* `MPI.COMM_WORLD`
* `bcast`
* `gather`
* Rank and process size management

---

## Execution

### Example with 4 processes

```bash
mpiexec -n 4 python mpi1.py
```

---

# MPI Version 2

## Description

The second MPI implementation focuses on reducing load imbalance.

The first implementation showed that some processes finished much earlier than others because the workload distribution was static.

To improve this:

* A better workload distribution strategy was implemented
* The objective is to reduce idle time between processes
* More balanced execution times are expected

---

## Execution

```bash
mpiexec -n 4 python mpi2.py
```

---

# Automated Experiment Execution

A batch script is provided to execute all experiments automatically.

### Docker execution (Linux / macOS)

```bash
docker run --rm -v "$(pwd)":/app augustosalazar/slim-mpi:2 sh /app/run_all.sh
```

### Docker execution (Windows)

```bash
docker run --rm -v "%cd%:/app" augustosalazar/slim-mpi:2 sh /app/run_all.sh
```

---

# 4. Experimental Plan

The experiments were executed using different numbers of processes:

[
p \in {1, 2, 4, 8}
]

For each configuration:

1. At least 3 runs were performed
2. Total execution time was recorded
3. Local execution time per process was recorded
4. Average execution time was computed

---

# 5. Experimental Plan Execution

## 5.a Sequential Baseline Timing

TODO: Add sequential timing results.

---

## 5.b MPI Version 1 Timing Results

TODO: Add MPI version 1 timing tables and averages.

---

## 5.c Load Imbalance Evidence

TODO: Add evidence showing imbalance between process execution times.

---

## 5.d MPI Version 2 Correcting the Imbalance

TODO:

* Add MPI version 2 timing results
* Add speedup calculations
* Add efficiency calculations
* Add load balance comparison

---

# Metrics

## Speedup

[
S_p = \frac{T_{seq}}{T_p}
]

Where:

* (T_{seq}) is the sequential execution time
* (T_p) is the average parallel execution time using (p) processes

---

## Efficiency

[
E_p = \frac{S_p}{p}
]

---

# Current Progress

## Completed

* Dataset generation
* Sequential baseline execution
* MPI version 1 implementation
* MPI version 2 implementation
* Initial experimental testing
* Docker execution setup

## Pending

* Complete experimental tables
* Final performance analysis
* Speedup and efficiency plots
* Final conclusions

---

# Example Timing Table

| Processes | Run 1 | Run 2 | Run 3 | Average |
| --------- | ----- | ----- | ----- | ------- |
| 1         | TODO  | TODO  | TODO  | TODO    |
| 2         | TODO  | TODO  | TODO  | TODO    |
| 4         | TODO  | TODO  | TODO  | TODO    |
| 8         | TODO  | TODO  | TODO  | TODO    |

---

# 6. Analysis

## Did MPI improve execution time?

Initial tests suggest that the MPI implementations reduce execution time compared to the sequential baseline.

## Was the speedup linear?

The speedup was not perfectly linear due to:

* Communication overhead
* Synchronization costs
* Load imbalance

## Was there load imbalance?

Yes. In MPI version 1, some processes completed their assigned work earlier than others.

This was observed through differences in local execution times.

## Did MPI version 2 improve load balance?

Preliminary tests indicate better workload distribution and more balanced execution times among processes.

---

# Repository Structure

```text
.
├── baseline_secuencial.py
├── generator.py
├── mpi1.py
├── mpi2.py
├── run_all.sh
├── dataset/
├── results/
└── README.md
```

---

# 7. Conclusions

The project explores how parallel processing with MPI can improve text-processing workloads.

The first MPI version demonstrated the benefits of parallelization but also revealed load imbalance issues.

The second version attempts to reduce these issues through a better workload distribution strategy.

Final conclusions will be based on the experimental results and computed metrics.
