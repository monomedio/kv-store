//
// Created by Sam Hui on 2023-11-22.
//

#ifndef CSC443_PROJECT_CONSTANTS_H
#define CSC443_PROJECT_CONSTANTS_H

#define INT64_T_SIZE sizeof(int64_t)

// 4KB page size
#define PAGE_SIZE 4096

// 8 byte integer key, 8 byte inger value
#define ENTRY_SIZE 16

// 4096 divided by 16 = 256
#define PAGE_NUM_ENTRIES 256

// Seed for hash function
#define SEED 1

// Allowed database types
#define SORTED_SST "sorted_sst"

#define BSST "BSST"

#define LSM_TREE "lsm_tree"

#endif  // CSC443_PROJECT_CONSTANTS_H
