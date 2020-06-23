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



/**
 * @description:
 * @param kv: 插入的键值对
 * @return: 0 = insert successfully, -1 = fail to insert(target data doesn't exist)
 */
int PmEHash::insert(kv new_kv_pair) {

    uint64_t temp;
    if (search(new_kv_pair.key, temp) == 0)
        return -1;

    pm_bucket* bucket = getFreeBucket(new_kv_pair.key);

    kv* freePlace = getFreeKvSlot(bucket);
    *freePlace = new_kv_pair;
    persist(freePlace, sizeof(freePlace));
    return 0;
}

/**
 * @description:
 * @param uint64_t: 要删除的目标键值对的键
 * @return: 0 = removing successfully, -1 = fail to remove(target data doesn't exist)
 */
int PmEHash::remove(uint64_t key) {
    //uint64_t temp;
    //if (search(key, temp) == -1) return -1;

    uint64_t hash_key = hashFunc(key);
    uint64_t mask = (1 << metadata->global_depth) - 1;
    uint64_t catalogID = hash_key & mask;

    pm_bucket* bucket = bucketTable[catalogID];
    for (int i = 0; i < BUCKET_SLOT_NUM / 8; ++i) {
        uint8_t bitmap = bucket->bitmap[i];
        for (int j = 0; j < 8; ++j) {
            if (((bitmap >> (7 - j)) & 1) == 1 && bucket->slot[i*8+j].key == key) {//找到桶中的一个kv对且key相匹配
                bucket->bitmap[i] &= ~(1 << (7- j)); //将该位置为空闲状态
                persist(&bucket->bitmap[i], sizeof(bucket->bitmap[i]));
                //是否合并交给mergeBucket自己决定
                mergeBucket(catalogID);
                return 0; //一个key只能对应一个value
            }
        }
    }
    return -1;

}
/**
 * @description: 更新现存的键值对的值
 * @param kv: 更新的键值对，有原键和新值
 * @return: 0 = update successfully, -1 = fail to update(target data doesn't exist)
 */
int PmEHash::update(kv kv_pair) {
    //uint64_t temp;
    //if (search(kv_pair.key, temp) == -1) return -1;
    pm_bucket* bucket = key_to_bucket(kv_pair.key);
    for (int i = 0; i < BUCKET_SLOT_NUM / 8; ++i) {
        uint8_t bitmap = bucket->bitmap[i];
        for (int j = 0; j < 8; ++j) {
            if (((bitmap >> (7 - j)) & 1) == 1 && bucket->slot[i*8+j].key == kv_pair.key) {//找到桶中的一个kv对且key相匹配
                bucket->slot[i*8+j].value = kv_pair.value;
                persist(&bucket->slot[i*8+j], sizeof(bucket->slot[i*8+j]));
                return 0; //一个key只能对应一个value
            }
        }
    }
    return -1;
}
/**
 * @description:
 * @param uint64_t: 查询的目标键
 * @param uint64_t&: 查询成功后返回的目标值
 * @return: 0 = search successfully, -1 = fail to search(target data doesn't exist)
 */
int PmEHash::search(uint64_t key, uint64_t& return_val) {

    uint64_t hash_key = hashFunc(key);
    uint64_t mask = (1 << metadata->global_depth) - 1;
    uint64_t catalogID = hash_key & mask;
    pm_bucket* bucket = bucketTable[catalogID];
    for (int i = 0; i < BUCKET_SLOT_NUM / 8; ++i) {
        uint8_t bitmap = bucket->bitmap[i];
        for (int j = 0; j < 8; ++j) {
            if (((bitmap >> (7 - j)) & 1) == 1 ) {//找到桶中的一个kv对且key相匹配
                if (bucket->slot[i*8+j].key == key) {
                    return_val = bucket->slot[i*8+j].value;
                    return 0;
                }

            }
        }
    }
    return -1;
}

/**
 * @description: 用于对输入的键产生哈希值，用于后续的取模求桶号操作
 * @param uint64_t: 输入的键
 * @return: 返回键的哈希值
 */
uint64_t PmEHash::hashFunc(uint64_t key) {
//https://www.cnblogs.com/thrillerz/p/4516769.html
    key = (~key) + (key << 21); // key = (key << 21) - key - 1;
    key = key ^ (key >> 24);
    key = (key + (key << 3)) + (key << 8); // key * 265
    key = key ^ (key >> 14);
    key = (key + (key << 2)) + (key << 4); // key * 21
    key = key ^ (key >> 28);
    key = key + (key << 31);
    return key;
}

/**
 * @description: 获得供插入的空闲的桶，无空闲桶则先分裂桶然后再返回空闲的桶
 * @param uint64_t: 带插入的键
 * @return: 空闲桶的虚拟地址
 */
pm_bucket* PmEHash::getFreeBucket(uint64_t key) {

    uint64_t hash_key = hashFunc(key);
    uint64_t mask = (1 << metadata->global_depth) - 1;
    uint64_t catalogID = hash_key & mask;

    pm_bucket* bucket = bucketTable[catalogID];
    for (int i = 0; i < BUCKET_SLOT_NUM / 8; ++i) {
        uint8_t bitmap = bucket->bitmap[i];
        for (int j = 0; j < 8; ++j) {
            if (((bitmap >> (7 - j)) & 1) == 0) {//找到桶中的第一个空位
                return bucket;
            }
        }
    }

    splitBucket(catalogID);
    //重新调用自己
    return getFreeBucket(key);

}

/**
 * @description: 获得空闲桶内第一个空闲的位置供键值对插入
 * @param pm_bucket* bucket
 * @return: 空闲键值对位置的虚拟地址
 */
kv* PmEHash::getFreeKvSlot(pm_bucket* bucket) {
    for (int i = 0; i < BUCKET_SLOT_NUM / 8; ++i) {
        uint8_t bitmap = bucket->bitmap[i];
        for (int j = 0; j < 8; ++j) {
            if (((bitmap >> (7 - j)) & 1) == 0) {
                bucket->bitmap[i] |= (1 << (7 - j)); //标记为被占用
                persist(&bucket->bitmap[i], sizeof(bucket->bitmap[i]));
                return &bucket->slot[i*8+j];
            }
        }
    }
    return NULL;
}


