# key-value-store

A C++ key-value database implemented from scratch. The database is structured a two-level LSM-tree where each level is structured as a B+ tree for faster reads. Bloom filters are employed on each level of the LSM-tree for probabilities queries on whether a key is present (set membership). An LRU cache is also maintained in memory to speed up access to frequently searched pages.

Please find a writeup for this project [here](ProjectReport.pdf).

To compile code run: `make`

To run executable run: `make run`

To remove executable run: `make clean`

The unittests for all tests will be located in tests/tests run it by executing `./tests/tests`

The project was developed in a [private repo](https://github.com/jon23lam/csc443-project), so commit histories will not be available.
