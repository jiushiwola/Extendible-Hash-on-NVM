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

/**
 * @description: 桶满后进行分裂操作，可能触发目录的倍增
 * @param uint64_t: 目标桶在目录中的序号
 * @return: NULL
 */
void PmEHash::splitBucket(uint64_t bucket_id) {
    bucket_id = minTrueBucket(bucket_id);

    pm_bucket* bucket = bucketTable[bucket_id];
    bucket->local_depth += 1;
    persist(&bucket->local_depth, sizeof(bucket->local_depth));
//分裂桶和合并桶的时候某个深度的变化一次有两个
    depth_count[bucket->local_depth] += 2;
    depth_count[bucket->local_depth - 1] -= 1;

    pm_bucket* new_bucket = NULL;
    if (free_list.empty()) {
        allocNewPage();
        if (free_list.empty()) {
            cout << "page allocate wrong\n";
        }

    }

    new_bucket = free_list.front();
    free_list.pop();
//页表中对应的位置要置为true
    pm_address address = vAddr2pmAddr[new_bucket];
    int bucket_index = (address.offset - DATA_PAGE_SLOT_NUM) / sizeof(pm_bucket);
    page_pointers[address.fileId]->used[bucket_index] = true;
//判断是否需要倍增目录
    if (bucket->local_depth > metadata->global_depth) {
        extendCatalog();
    }

    new_bucket->local_depth = bucket->local_depth;
    uint64_t add = (1 << (bucket->local_depth - 1));
    uint64_t cur_id = bucket_id + add;
    bucketTable[cur_id] = new_bucket;
//这一步很重要，它能实时更新原来与cur_id指向相同桶的目录id指向的桶，（如果不及时更新信息就过时了）
    SetCatalogToSameBucket(cur_id, add, bucket, new_bucket);
//
    //遍历bucket，判断某一个键值对该留在bucket还是转移到new_bucket
    int k = 0; //new_bucket的slot下标
    for (int i = 0; i < BUCKET_SLOT_NUM; ++i) {
        uint64_t key = bucket->slot[i].key;
        key = hashFunc(key);
        if ((key >> (bucket->local_depth - 1)) & 1) { //最高位是1，在bucket里删除并插到new_bucket里
            new_bucket->slot[k] = bucket->slot[i];
            new_bucket->bitmap[k/8] |= (1 << (7 - (k % 8))); //new_bucket的位置为1
            bucket->bitmap[i/8] &= ~(1 << (7 - (i % 8)));  //bucket的位置为0
            k++;
        }

    }

    persist(bucket, sizeof(pm_bucket));
    persist(new_bucket, sizeof(pm_bucket));
    persist(&page_pointers[address.fileId]->used, sizeof(page_pointers[address.fileId]->used));

}

uint64_t PmEHash::minTrueBucket(uint64_t bucket_id) {
    for (uint64_t i = 4; i <= bucketTable[bucket_id]->local_depth; ++i) {

        if (bucketTable[bucket_id % (1 << i)] == bucketTable[bucket_id]) return bucket_id % (1 << i);
    }
    return 0;
}
/**
 * @description:
 * @param {type}
 * @return:
 */

void PmEHash::mergeBucket(uint64_t bucket_id) {
    uint64_t minId = minTrueBucket(bucket_id);
    pm_bucket* bucket = bucketTable[minId];
    if (bucket->getCount() > 0 || bucket->local_depth <= 4) return;
    uint64_t boundary = (1 << (bucket->local_depth - 1));
    uint64_t lid;
    if (minId < boundary) {
        lid = minId + boundary;
    }
    else {
        lid = minId - boundary;
    }
    pm_bucket* new_bucket = bucketTable[lid];

    if (bucket->local_depth != new_bucket->local_depth) return;
    free_list.push(bucket);

    pm_address address = vAddr2pmAddr[bucket];
    uint32_t offset = (address.offset - DATA_PAGE_SLOT_NUM) / sizeof(pm_bucket);
    page_pointers[address.fileId]->used[offset] = false;

    bucketTable[minId] = new_bucket;
    //uint64_t cur_id = minId;

    uint64_t add = boundary;
    uint64_t cur_id = minId;
    //bucketTable[cur_id] = larger_bucket;
    SetCatalogToSameBucket(cur_id, add, bucket, new_bucket);
//
    depth_count[new_bucket->local_depth] -= 2;
    depth_count[new_bucket->local_depth - 1] += 1;
    new_bucket->local_depth -= 1;


    if (depth_count[metadata->global_depth] <= 0) {
        //cout << "目录减半\n";
        metadata->catalog_size /= 2;
        metadata->global_depth--;
        persist(metadata, sizeof(ehash_metadata));
    }

//如果内存中某个页为空，就把它释放
    bool flag = true;
    uint32_t page_id = vAddr2pmAddr[bucket].fileId;
    for (int i = 0; i < DATA_PAGE_SLOT_NUM; ++i) {
        if (page_pointers[page_id]->used[i] == true) {
            flag = false;
            break;
        }
    }
    if (flag == true)
        deletePage(page_id);

//由于merge过程的特殊性，（深度不同的桶也暂时不合并，那么就会出现需要连续合并的情况
    mergeBucket(minId);


}


void PmEHash::extendCatalog() {
    if (metadata->catalog_size == MAX_CATALOG_SIZE) {
        printf("目录大小已达到最大值，无法再增大！\n");
        return;
    }

    size_t mapped_len;
    int is_pmem;
    pmem_unmap(catalog.buckets_pm_address, sizeof(pm_address) * metadata->catalog_size);

    catalog.buckets_pm_address = (pm_address*)pmem_map_file(CATALOG_NAME,
    sizeof(pm_address) * metadata->catalog_size * 2,
    PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem);
    if (catalog.buckets_pm_address == NULL) {
        perror("pmem_map_file");
        exit(1);
    }

    uint64_t cat = metadata->catalog_size;
    for (uint64_t i = 0; i < cat; ++i) {
        bucketTable[i + cat] = bucketTable[i];
    }

    metadata->catalog_size *= 2;
    metadata->global_depth++;
    persist(metadata, sizeof(ehash_metadata));


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


void PmEHash::persist(void* virtual_address, size_t mapped_len) {
    pmem_persist(virtual_address, mapped_len);
}

inline pm_bucket* PmEHash::key_to_bucket(uint64_t key) {
    uint64_t hash_key = hashFunc(key);
    uint64_t mask = (1 << metadata->global_depth) - 1;
    uint64_t catalogID = hash_key & mask;
    pm_bucket* bucket = bucketTable[catalogID];
    return bucket;
}





void PmEHash::print() {
//    cout << "free_list:\n";
//    while(!free_list.empty()) {
//        pm_bucket* bucket = free_list.front();
//        free_list.pop();
//        if (page_in_memory[vAddr2pmAddr[bucket].fileId] == false) continue;
//        //cout << bucket << endl;
//        cout << vAddr2pmAddr[bucket].fileId << ' ' << vAddr2pmAddr[bucket].offset << endl;
//
//    }
    cout << "global_depth: " << metadata->global_depth << endl;
    cout << "depth_count: \n";
    for (uint64_t i = 1; i <= metadata->global_depth; ++i) {
        cout << i << ' ' << depth_count[i] << endl;
    }
    cout << "catalog:\n";
    cout << metadata->catalog_size << endl;
    for (uint64_t k = 0; k < metadata->catalog_size; ++k) {
        cout << k << " : ";
        pm_bucket* bucket = bucketTable[k];
        cout << vAddr2pmAddr[bucket].fileId << ' ' << vAddr2pmAddr[bucket].offset << ' ' << bucket->local_depth << '\t';
        for (int i = 0; i < BUCKET_SLOT_NUM / 8; ++i) {
            for (int j = 0; j < 8;++j) {
                if ((bucket->bitmap[i] >> (7- j)) & 1) {
                    cout << bucket->slot[i*8+j].key << ' ' << bucket->slot[i*8+j].value << '\t';
                }
            }

        }
        cout << endl;
    }

}

void PmEHash::selfDestroy() {
    for (uint64_t i = 1; i < metadata->max_file_id; ++i) {
        char filename[30];
        sprintf(filename, "%s%d", PM_EHASH_DIRECTORY, i);
        if (page_pointers[i]->in_memory == true) {
            pmem_unmap(page_pointers[i], sizeof(data_page));
        }
        std::remove(filename);

    }

    pmem_unmap(catalog.buckets_pm_address, sizeof(pm_address) * metadata->catalog_size);
    std::remove(CATALOG_NAME);
    pmem_unmap(metadata, sizeof(metadata));
    std::remove(META_NAME);
    destroyed = true;
}

void PmEHash::SetCatalogToSameBucket(uint64_t cur_id, uint64_t add, pm_bucket* bucket, pm_bucket* new_bucket) {
    vector<uint64_t> vec{cur_id};
    while(true) {
        int n = vec.size();
        bool flag = true;
        for (int i = 0; i < n; ++i) {
            uint64_t top = vec[i];
            cur_id = top + add * 2;
            if (cur_id >= metadata->catalog_size) {
                flag = false;
                break;
            }
            if (bucketTable[cur_id] == bucket) {
                vec.push_back(cur_id);
                bucketTable[cur_id] = new_bucket;
            }

        }
        if (flag == false) break;

        add *= 2;

    }
}
