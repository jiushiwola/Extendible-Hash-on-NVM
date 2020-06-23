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
#include <libpmem.h>
/**
 * @description: construct a new instance of PmEHash in a default directory
 * @param NULL
 * @return: new instance of PmEHash
 */
PmEHash::PmEHash() {
    recover();

}
/**
 * @description: persist and munmap all data in NVM
 * @param NULL
 * @return: NULL
 */
PmEHash::~PmEHash() {
    writeBack();
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
    uint64_t temp;
    if (search(key, temp) == -1) return -1;
    //if (key == 989)
    //cout << "remove: " << key << endl;
    uint64_t hash_key = hashFunc(key);
    uint64_t mask = (1 << metadata->global_depth) - 1;
    uint64_t catalogID = hash_key & mask;
    //cout << "catalogID: " << catalogID << endl;
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

}
/**
 * @description: 更新现存的键值对的值
 * @param kv: 更新的键值对，有原键和新值
 * @return: 0 = update successfully, -1 = fail to update(target data doesn't exist)
 */
int PmEHash::update(kv kv_pair) {
    uint64_t temp;
    if (search(kv_pair.key, temp) == -1) return -1;
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
}
/**
 * @description:
 * @param uint64_t: 查询的目标键
 * @param uint64_t&: 查询成功后返回的目标值
 * @return: 0 = search successfully, -1 = fail to search(target data doesn't exist)
 */
int PmEHash::search(uint64_t key, uint64_t& return_val) {
    //pm_bucket* bucket = key_to_bucket(key);
    uint64_t hash_key = hashFunc(key);
    uint64_t mask = (1 << metadata->global_depth) - 1;
    uint64_t catalogID = hash_key & mask;
    pm_bucket* bucket = bucketTable[catalogID];
    for (int i = 0; i < BUCKET_SLOT_NUM / 8; ++i) {
        //cout << "i: " << i << endl;
        uint8_t bitmap = bucket->bitmap[i];
        //cout << "bitmap: " << (int)bucket->bitmap[i] << endl;
        for (int j = 0; j < 8; ++j) {
            if (((bitmap >> (7 - j)) & 1) == 1 ) {//找到桶中的一个kv对且key相匹配
                //cout << "hahha " << bucket->slot[i*8+j].key << ' ' << key << endl;
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
    //cout << "insertToIndex: " << catalogID << endl;
    pm_bucket* bucket = bucketTable[catalogID];
    for (int i = 0; i < BUCKET_SLOT_NUM / 8; ++i) {
        uint8_t bitmap = bucket->bitmap[i];
        for (int j = 0; j < 8; ++j) {
            if (((bitmap >> (7 - j)) & 1) == 0) {//找到桶中的第一个空位
                //cout << bucket << ' ' << i << ' ' << j << endl;
                return bucket;
            }
        }
    }

    splitBucket(catalogID);
    //重新调用自己
    return getFreeBucket(key);
    //return NULL;

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
}

/**
 * @description: 桶满后进行分裂操作，可能触发目录的倍增
 * @param uint64_t: 目标桶在目录中的序号
 * @return: NULL
 */
void PmEHash::splitBucket(uint64_t bucket_id) {
    bucket_id = minTrueBucket(bucket_id);
    //cout << "split: " << bucket_id << endl;
    pm_bucket* bucket = bucketTable[bucket_id];
    bucket->local_depth += 1;
    persist(&bucket->local_depth, sizeof(bucket->local_depth));
//分裂桶和合并桶的时候某个深度的变化一次有两个
    depth_count[bucket->local_depth] += 2;
    depth_count[bucket->local_depth - 1] -= 1;

    pm_bucket* new_bucket = NULL;
    while(!free_list.empty()) {
        new_bucket = free_list.front();
        free_list.pop();
        //cout << new_bucket << endl;
        pm_address address = vAddr2pmAddr[new_bucket];
        if (page_in_memory[address.fileId] == true) break; //必须保证拿到的桶在内存中
    }
    if (new_bucket == NULL) {
        allocNewPage();
        if (free_list.empty()) {
            cout << "page allocate wrong\n";
        }
        new_bucket = free_list.front();
        free_list.pop();
    }

    //页表中对应的位置要置为true
    pm_address address = vAddr2pmAddr[new_bucket];
    int bucket_index = (address.offset - DATA_PAGE_SLOT_NUM) / sizeof(pm_bucket);
    page_pointers[address.fileId]->used[bucket_index] = true;
    //cout << address.fileId << ' ' << bucket_index << endl;

//判断是否需要倍增目录
    if (bucket->local_depth > metadata->global_depth) {
//        cout << "目录翻倍" << endl;
//        cout << "bucket_id: " << bucket_id << " bucket->local_depth: " << bucket->local_depth << endl;
        extendCatalog();
    }

    new_bucket->local_depth = bucket->local_depth;
//    cout << endl << bucket->local_depth << endl;
//    cout << bucket << ' ' << new_bucket << endl;
//    cout << bucket_id  << ' ' << (1 << (bucket->local_depth - 1)) << ' ' << new_bucket << endl;
    int add = (1 << (bucket->local_depth - 1));
    int cur_id = bucket_id + add;
    bucketTable[cur_id] = new_bucket;
//这一步很重要，它能实时更新原来与cur_id指向相同桶的目录id指向的桶，（如果不及时更新信息就过时了）

    while(true) {
        cur_id = cur_id + add * 2;
        if (cur_id >= metadata->catalog_size) break;
        if (bucketTable[cur_id] == bucket)
            bucketTable[cur_id] = new_bucket;
        add *= 2;

    }


//    cout << "split!sssssssssssssssssssssssssssssssssssssssssssssssss\n" << bucket_id << ' ' <<
//        (1 << (bucket->local_depth - 1)) << ' ' << bucket->local_depth << endl;


    //遍历bucket，判断某一个键值对该留在bucket还是转移到new_bucket
    int k = 0; //new_bucket的slot下标
    for (int i = 0; i < BUCKET_SLOT_NUM; ++i) {
        uint64_t key = bucket->slot[i].key;
        key = hashFunc(key);
        if ((key >> (bucket->local_depth - 1)) & 1) { //最高位是1，在bucket里删除并插到new_bucket里
            new_bucket->slot[k] = bucket->slot[i];
            new_bucket->bitmap[k/8] |= (1 << (7 - (k % 8)));
            bucket->bitmap[i/8] &= ~(1 << (7 - (i % 8)));

            k++;
        }

    }

    persist(bucket, sizeof(pm_bucket));
    persist(new_bucket, sizeof(pm_bucket));
    persist(&page_pointers[address.fileId]->used, sizeof(page_pointers[address.fileId]->used));

}

uint64_t PmEHash::minTrueBucket(uint64_t bucket_id) {
    for (int i = 4; i <= bucketTable[bucket_id]->local_depth; ++i) {

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
//定义何情况下需要merge：
//1.当一个桶为空，且另一个桶个数小于容量的一半时，这适合桶空间足够的情况，可以减少重新分裂桶带来的消耗
//2.当一个桶为空时，立刻合并桶(
//暂时采用第二种方式

    //cout << "bucket_id: " << bucket_id << endl;
    uint64_t minId = minTrueBucket(bucket_id); //获得指向相同桶的最小id

    pm_bucket* bucket = bucketTable[minId];
    if (bucket->getCount() > 0 || bucket->local_depth <= 4) return;

    //cout << "minId: " << minId << "bucket_id: " << bucket_id << endl;
    uint64_t boundary = (1 << (bucket->local_depth - 1));
    //分两种情况，minId是两个要合并的目录中较大的目录还是较小的目录
    //第一种情况，minId是较小的目录，向下合并尤其需要注意的是，当较大目录指向的桶改变时，较小目录也要随之改变
    if (minId < boundary) {
        //分两种情况，一种是minId与minId + boundary指向的桶相同，那么只需改变深度，不用改其它
        //另一种是指向的桶不同，那么合并两个不同的桶，改变深度
        //第一种情况
        //if (minId == 2)cout << "1.";
        if (bucketTable[minId] == bucketTable[minId + boundary]) {
            //if (minId == 2)cout << "1.";
            depth_count[bucket->local_depth] -= 2; //虽然有两个目录，实际上只有一个桶，也即一个深度
            depth_count[bucket->local_depth - 1] += 1;
            bucket->local_depth -= 1;
            if (bucket->local_depth < 4) bucket->local_depth = 4;

        }
        //第二种情况
        else {
            //if (minId == 2)cout << "2.";
            pm_bucket* larger_bucket = bucketTable[minId + boundary];
            free_list.push(bucket);
            pm_address address = vAddr2pmAddr[bucket];
            int bucket_index = (address.offset - DATA_PAGE_SLOT_NUM) / sizeof(pm_bucket);
            page_pointers[address.fileId]->used[bucket_index] = false;
            //因为已经push到free_list里了，也即可能会被其他需要的目录申请到，所以必须保证目录中不会再出现指向bucket的项
            bucketTable[minId] = larger_bucket;
            int cur_id = minId, temp = DEFAULT_CATALOG_SIZE;
            while(true) {
                cur_id = cur_id - temp;
                //cout << "cur_id: " << cur_id << endl;
                if (cur_id < 0 ) break;
                if (bucketTable[cur_id] == bucket) bucketTable[cur_id] = larger_bucket;


            }
            cur_id = minId, temp = 16;
            while(true) {
                cur_id = cur_id + temp;
                if (cur_id >= metadata->catalog_size) break;
                if (bucketTable[cur_id] == bucket) bucketTable[cur_id] = larger_bucket;

            }

            //改变larger_bucket的深度，分两种情况，一是larger_bucket的深度与bucket深度不同（即比它大），二是相同
            //第一种，深度不同，那么上面已经做完将bucket指向larger_bucket的工作，不用再做其他事情了
            if (larger_bucket->local_depth > bucket->local_depth) {
                //if (minId == 2)cout << "1.";
            }
            //第二种，深度相同，需要改变larger_bucket的深度
            else {
                //if (minId == 2)cout << "2.";
                depth_count[larger_bucket->local_depth] -= 2;
                depth_count[larger_bucket->local_depth - 1] += 1;
                larger_bucket->local_depth -= 1;
                if (larger_bucket->local_depth < 4) larger_bucket->local_depth = 4;
            }

        }


    }
    //第二种情况，minId是较大的目录，总体与第一种情况处理方法相似
    else {
        //分两种情况，一种是minId与minId - boundary指向的桶相同，那么只需改变深度，不用改其它
        //另一种是指向的桶不同，那么合并两个不同的桶，改变深度
        //第一种情况
        //if (minId == 2)cout << "2.";
        if (bucketTable[minId] == bucketTable[minId - boundary]) {
            //if (minId == 2)cout << "1.";
            depth_count[bucket->local_depth] -= 2; //会出现这种情况的原因是之前合并时出现过两个桶深度不同的情况，那个时候没有改变深度，所以都在这里改
            depth_count[bucket->local_depth - 1] += 1;
            bucket->local_depth -= 1;
            if (bucket->local_depth < 4) bucket->local_depth = 4;

        }
        //第二种情况
        else {
            //if (minId == 2)cout << "2.";
            pm_bucket* smaller_bucket = bucketTable[minId - boundary];
            free_list.push(bucket);
            pm_address address = vAddr2pmAddr[bucket];
            int bucket_index = (address.offset - DATA_PAGE_SLOT_NUM) / sizeof(pm_bucket);
            page_pointers[address.fileId]->used[bucket_index] = false;
            //因为已经push到free_list里了，也即可能会被其他需要的目录申请到，所以必须保证目录中不会再出现指向bucket的项
            bucketTable[minId] = smaller_bucket;

            int cur_id = minId, temp = DEFAULT_CATALOG_SIZE;
            while(true) {
                cur_id = cur_id - temp;
                if (cur_id < 0) break;
                if (bucketTable[cur_id] == bucket) bucketTable[cur_id] = smaller_bucket;
                //temp <<= 1;
            }
            cur_id = minId;
            while(true) {
                cur_id = cur_id + temp;
                if (cur_id >= metadata->catalog_size) break;
                if (bucketTable[cur_id] == bucket) bucketTable[cur_id] = smaller_bucket;
                //temp <<= 1;
            }

            //改变smaller_bucket的深度，分两种情况，一是smaller_bucket的深度与bucket深度不同（），二是相同
            //第一种，深度不同，那么上面已经做完将bucket指向smaller_bucket的工作，不用再做其他事情了
            if (smaller_bucket->local_depth > bucket->local_depth) {
                //if (minId == 2)cout << "1.";
            }
            //第二种，深度相同，需要改变smaller_bucket的深度
            else if (smaller_bucket->local_depth == bucket->local_depth){
                //if (minId == 2)cout << "2.";
                depth_count[smaller_bucket->local_depth] -= 2;
                depth_count[smaller_bucket->local_depth - 1] += 1;
                smaller_bucket->local_depth -= 1;
                if (smaller_bucket->local_depth < 4) {
                    //if (minId == 2)cout << "深度相同，需要改变smaller_bucket的深度 " << minId << ' ' << minId - boundary << endl;
                }
            }

        }


    }




//合并目录,举个例子，如果有32个目录，即全局深度为5，但是局部深度大于等于5的个数减为0，那么就缩小目录
//这种实现需要维护各个深度对应的个数，但是就比暴力遍历计数节省了时间
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

//由于merge过程的特殊性，（深度不同的桶也可以合并，那么就会出现需要连续合并的情况
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
    for (int i = 0; i < cat; ++i) {
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
    //cout << metadata->max_file_id << endl;
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

        for(int i = 0; i < metadata->catalog_size; ++i) {
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

//  写回目录
    for (int i = 0; i < metadata->catalog_size; ++i) {
        catalog.buckets_pm_address[i] = vAddr2pmAddr[bucketTable[i]];

    }
    pmem_persist(catalog.buckets_pm_address, sizeof(pm_address*) * metadata->catalog_size);
    pmem_unmap(catalog.buckets_pm_address, sizeof(pm_address*) * metadata->catalog_size);
//  写回页表
    size_t mapped_len = sizeof(data_page);
    for (int i = 1; i < metadata->max_file_id; ++i) {
        if (page_in_memory[i] == false) continue;
//persist的作用是从cache到内存
        if (Is_pmem[i]) {
            printf("Writing to pm\n");
            pmem_persist(page_pointers[i], mapped_len);
        }
        else {
            printf("Not Writing to pm\n");
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
void PmEHash::mapAllPage() {
//将页面作内存映射，映射到pmem0挂载的地方;
//初始化free_list
    for (int i = 1; i < metadata->max_file_id; ++i) {
        char filename[30];
        sprintf(filename, "%s%d", PM_EHASH_DIRECTORY, i);
        int PMEM_LEN = sizeof(data_page);
        size_t mapped_len;
        if ((page_pointers[i] = (data_page*)pmem_map_file(filename, PMEM_LEN, PMEM_FILE_CREATE,
			0666, &mapped_len, &Is_pmem[i])) == NULL) {
            perror("pmem_map_file");
            exit(1);
        }

    }
    if (at_begin == false) {
        for (int i = 1; i < metadata->max_file_id; ++i) {
            if (page_pointers[i]->in_memory == false) {
                pmem_unmap(page_pointers[i], sizeof(data_page));
                page_in_memory[i] = false;
                continue;
            }

            page_in_memory[i] = true;

            for (int j = 0; j < DATA_PAGE_SLOT_NUM; ++j) {
                if (page_pointers[i]->used[j] == false) {
                    pm_bucket* new_bucket = &page_pointers[i]->buckets[j];
                    //cout << endl << i << endl;
                    pm_address address(i, DATA_PAGE_SLOT_NUM + j * sizeof(pm_bucket));
                    vAddr2pmAddr[new_bucket] = address;
                    free_list.push(new_bucket);
                    //cout << address.fileId << ' ' << address.offset << endl;
                }
            }
        }


    }

    else {
        for (int j = 0; j < DATA_PAGE_SLOT_NUM; ++j) {
            pm_bucket* new_bucket = &page_pointers[2]->buckets[j];
            //pm_bucket* new_bucket = new pm_bucket();
            pm_address address(2, DATA_PAGE_SLOT_NUM + j * sizeof(pm_bucket));
            vAddr2pmAddr[new_bucket] = address;
            new_bucket->local_depth = log(DEFAULT_CATALOG_SIZE) / log(2);
            persist(&new_bucket->local_depth, sizeof(new_bucket->local_depth));
            free_list.push(new_bucket);
            page_pointers[2]->used[j] = false;
        }
        persist(&page_pointers[2]->used, sizeof(page_pointers[2]->used));
        for (int j = 0; j < DATA_PAGE_SLOT_NUM; ++j) {
            page_pointers[1]->used[j] = true;
        }
        persist(&page_pointers[1]->used, sizeof(page_pointers[1]->used));
        page_pointers[1]->in_memory = page_in_memory[1] = true;
        page_pointers[2]->in_memory = page_in_memory[2] = true;
        persist(&page_pointers[1]->in_memory, sizeof(page_pointers[1]->in_memory));
        persist(&page_pointers[2]->in_memory, sizeof(page_pointers[2]->in_memory));

    }
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


//这个函数暂时不知道什么意思
void* PmEHash::getNewSlot(pm_address& new_address) {

}

void PmEHash::allocNewPage() {
    int id = 0;
    for (int i = 1; i < metadata->max_file_id; ++i) {
        if (page_in_memory[i] == false) {
            id = i;
            break;
        }
    }
    if (id == 0 && metadata->max_file_id == MAX_PAGE_ID) {
        printf("failed to allocate new page.\n");
        return;
    }
    if (id == 0) id = metadata->max_file_id++;
    char filename[30];
    sprintf(filename, "%s%d", PM_EHASH_DIRECTORY, id);
    int PMEM_LEN = (sizeof(pm_bucket) + 1) * DATA_PAGE_SLOT_NUM;
    size_t mapped_len;
    if ((page_pointers[id] = (data_page*)pmem_map_file(filename, PMEM_LEN, PMEM_FILE_CREATE,
        0666, &mapped_len, &Is_pmem[id])) == NULL) {
        perror("pmem_map_file");
        exit(1);
    }
    page_pointers[id]->in_memory = page_in_memory[id] = true;

    for (int i = 0; i < DATA_PAGE_SLOT_NUM; ++i) {
        pm_address address(id, DATA_PAGE_SLOT_NUM + i * sizeof(pm_bucket));
        pm_bucket* bucket = &page_pointers[id]->buckets[i];
        bucket->local_depth = 4;
        page_pointers[id]->used[i] = false;
        vAddr2pmAddr[bucket] = address;
        free_list.push(bucket);
    }
    persist(metadata, sizeof(ehash_metadata));
    persist(page_pointers[id], sizeof(data_page));
}

void PmEHash::deletePage(uint32_t page_id) {
    cout << "Delete page " << page_id << endl;
    page_pointers[page_id]->in_memory = page_in_memory[page_id] = false;
    persist(&page_pointers[page_id]->in_memory, sizeof(page_pointers[page_id]->in_memory));
    pmem_unmap(page_pointers[page_id], sizeof(data_page));
//更新free_slot
    int cnt = free_list.size();
    for (int i = 0; i < cnt; ++i) {
        pm_bucket* bucket = free_list.front();
        free_list.pop();
        if (page_in_memory[vAddr2pmAddr[bucket].fileId] == true) free_list.push(bucket);
    }
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
    cout << "depth_count: \n";
    for (int i = 1; i <= metadata->global_depth; ++i) {
        cout << i << ' ' << depth_count[i] << endl;
    }
    cout << "catalog:\n";
    cout << metadata->catalog_size << endl;
    for (int k = 0; k < metadata->catalog_size; ++k) {
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


//void PmEHash::print(int key) {
//    uint64_t hash_key = hashFunc(key);
//    uint64_t mask = (1 << metadata->global_depth) - 1;
//    uint64_t catalogID = hash_key & mask;
//    pm_bucket* bucket = bucketTable[catalogID];
//    cout << catalogID << ' ';
//    cout << vAddr2pmAddr[bucket].fileId << ' ' << vAddr2pmAddr[bucket].offset << endl;
//
//    for (int i = 0; i < BUCKET_SLOT_NUM / 8; ++i) {
//        uint8_t bitmap = bucket->bitmap[i];
//        for (int j = 0; j < 8; ++j) {
//
//                cout << ((bitmap >> (7-j)) & 1) ;
//
//        }
//        cout << '\t';
//    }
//
//}
int main() {
    PmEHash t;
//286
//
    for (int i = 0; i < 6000; ++i) {
        t.insert(kv(i,i));
    }

    for (int i = 0; i < 6000; ++i) {
        t.remove(i);
    }

    t.print();

}
