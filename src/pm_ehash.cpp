#include"../include/pm_ehash.h"
#include <fstream>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

const char* META_NAME = "../data/pm_ehash_metadata";
const char* CATALOG_NAME = "../data/pm_ehash_catalog";
const char* PM_EHASH_DIRECTORY = "../data/";        // add your own directory path to store the pm_ehash

/**
 * @description: construct a new instance of PmEHash in a default directory
 * @param NULL
 * @return: new instance of PmEHash
 */
PmEHash::PmEHash() {
    bucketTable = new pm_bucket*[MAX_CATALOG_SIZE + 1];
    recover();
    destroyed = false;
}
/**
 * @description: persist and munmap all data in NVM
 * @param NULL
 * @return: NULL
 */
PmEHash::~PmEHash() {
    writeBack();
    delete []bucketTable;
}

void PmEHash::recover() {


//读入metadata
//
    size_t mapped_len;
    int is_pmem;
    metadata = (ehash_metadata*)pmem_map_file(META_NAME, sizeof(ehash_metadata), PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem);
    if (metadata == NULL) {
        perror("pmem_map_file");
        exit(1);
    }
    at_begin = false;
    if (metadata->max_file_id == 0) {
        at_begin = true;
        metadata->max_file_id = 3;
        metadata->catalog_size = DEFAULT_CATALOG_SIZE;
        metadata->global_depth = log(DEFAULT_CATALOG_SIZE) / log(2);
        persist(metadata, sizeof(ehash_metadata));
    }


    mapAllPage();

//初始化depth_count
    for (int i = 1; i <= MAX_GLOBAL_DEPTH; ++i) {
        depth_count[i] = 0;
    }
//读入bucket对应的pm_address
//初始化目录
//
    catalog.buckets_pm_address = (pm_address*)pmem_map_file(CATALOG_NAME,
    sizeof(pm_address) * metadata->catalog_size,
    PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem);
    if (catalog.buckets_pm_address == NULL) {
        perror("pmem_map_file");
        exit(1);
    }

    if (at_begin == false) {

        for(uint64_t i = 0; i < metadata->catalog_size; ++i) {
            pm_address page_slot = catalog.buckets_pm_address[i];
            int bucket_index = (page_slot.offset - DATA_PAGE_SLOT_NUM) / sizeof(pm_bucket);
            pm_bucket* new_bucket = &page_pointers[page_slot.fileId]->buckets[bucket_index];
            depth_count[new_bucket->local_depth]++; //该桶对应的深度数量加一
            vAddr2pmAddr[new_bucket] = page_slot;
            bucketTable[i] = new_bucket;
        }
        for (int i = 4; i < MAX_GLOBAL_DEPTH; ++i) {
            depth_count[i] = metadata->depth_count[i];
        }

    }
    else {
        depth_count[int(log(DEFAULT_CATALOG_SIZE) / log(2))] = DEFAULT_CATALOG_SIZE; //有16个深度为4的桶，其他深度为0

        for (int i = 0; i < DATA_PAGE_SLOT_NUM; ++i) {//DATA_PAGE_SLOT_NUM = DEFAULT_CATALOG_NUM
            pm_address page_slot(1, DATA_PAGE_SLOT_NUM + i * sizeof(pm_bucket));
            pm_bucket* new_bucket = &page_pointers[1]->buckets[i];
            new_bucket->local_depth = log(DEFAULT_CATALOG_SIZE) / log(2);
            persist(&new_bucket->local_depth, sizeof(new_bucket->local_depth));
            vAddr2pmAddr[new_bucket] = page_slot;
            bucketTable[i] = new_bucket;
        }
    }

}

void PmEHash::writeBack() {
    if (destroyed == true) return;
//  写回目录
    for (uint64_t i = 0; i < metadata->catalog_size; ++i) {
        catalog.buckets_pm_address[i] = vAddr2pmAddr[bucketTable[i]];

    }
    pmem_persist(catalog.buckets_pm_address, sizeof(pm_address*) * metadata->catalog_size);
    pmem_unmap(catalog.buckets_pm_address, sizeof(pm_address*) * metadata->catalog_size);
//  写回页表
    size_t mapped_len = sizeof(data_page);
    for (uint64_t i = 1; i < metadata->max_file_id; ++i) {
        if (page_in_memory[i] == false) continue;
//persist的作用是从cache到内存
        if (Is_pmem[i]) {
            //printf("Writing to pm\n");
            pmem_persist(page_pointers[i], mapped_len);
        }
        else {
            //printf("Not Writing to pm\n");
            pmem_msync(page_pointers[i], mapped_len);
        }
        pmem_unmap(page_pointers[i], mapped_len);
    }
//  写回metadata
    for (int i = 4; i < MAX_GLOBAL_DEPTH; ++i) {
        metadata->depth_count[i] = depth_count[i];
    }
    pmem_persist(metadata, sizeof(ehash_metadata));
    pmem_unmap(metadata, sizeof(ehash_metadata));

}

