1. Title and Team Information

2. Problem Description

3. Environment and Execution Instructions

4. Experimental plan
    a. Sequential Baseline (what it does and how)

    b. MPI version 1 (what it does and how)

    c. The test procedure

5. Experimental plan execution
    a. Sequential baseline timing

    b. MPI version 1 timing results

    c. Load imbalance evidence

    d. Implementation of MPI Version 2 correcting the imbalance with its timing results
    (total execution time, speedup, efficiency, load balance)

6. Analysis

    a. Did the first MPI implementation improve execution time compared to the
    sequential baseline?

    b. Was the observed speedup linear?

    c. Is there evidence of load imbalance? How was it observed?

    d. Did the second implementation reduce load imbalance?

    e. Did the improved distribution strategy produce a real performance
    improvement?
    
    f. What limitations affected your experiment?

7.  Conclusions
State whether the parallel implementations improved execution time compared to
the sequential baseline, the most important problem observed in the first parallel
version, if the second version helped and how ending with a judgment based on
evidence