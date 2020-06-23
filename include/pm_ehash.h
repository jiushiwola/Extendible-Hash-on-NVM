#ifndef __PM_EHASH_H__
#define __PM_EHASH_H__
#include<cstdint>
#include<queue>
#include<map>
#include<math.h>
#include <libpmem.h>
#define BUCKET_SLOT_NUM 16
#define DEFAULT_CATALOG_SIZE 16
#define MAX_PAGE_ID 20000  //最多不能超过100页
#define DATA_PAGE_SLOT_NUM 16
#define MAX_GLOBAL_DEPTH 20
#define MAX_CATALOG_SIZE (1 << MAX_GLOBAL_DEPTH)   //开太大会导致段错误

using namespace std;




/*
---the physical address of data in NVM---
fileId: 1-N, the data page name
offset: data offset in the file
*/
typedef struct pm_address
{
    uint32_t fileId;
    uint32_t offset;
    pm_address(uint32_t id, uint32_t offst) {
        fileId = id;
        offset = offst;
    }
    pm_address() {
    }
} pm_address;

/*
the data entry stored by the hash
*/
typedef struct kv
{
    uint64_t key;
    uint64_t value;
    kv(uint64_t key = 0, uint64_t value = 0) {
        this->key = key;
        this->value = value;
    }
} kv;

typedef struct pm_bucket  //pm_bucket的大小是256个字节，为什么？
{
    uint64_t local_depth ;
    uint8_t  bitmap[BUCKET_SLOT_NUM / 8];      // one bit for each slot
    kv       slot[BUCKET_SLOT_NUM];                                // one slot for one kv-pair
//    pm_bucket(uint64_t depth = log(DEFAULT_CATALOG_SIZE) / log(2)) {
//        local_depth = depth;
//        for (int i = 0; i < BUCKET_SLOT_NUM / 8 + 1; ++i) bitmap[i] = 0;
//    }
    int getCount() {
        int count = 0;
        for (int i = 0; i < BUCKET_SLOT_NUM / 8 ; ++i) {
            for (int j = 0; j < 8;++j) {
                if ((bitmap[i] >> (7- j)) & 1) {
                    ++count;
                }
            }

        }
        return count;
    }

} pm_bucket;

// use pm_address to locate the data in the page

// uncompressed page format design to store the buckets of PmEHash
// one slot stores one bucket of PmEHash
// 一个页有16个槽，每个槽可以存放一个可扩展哈西中的bucket
// 一个页的结构存储在一个文件中
typedef struct data_page {
    // fixed-size record design
    // uncompressed page format
    bool used[DATA_PAGE_SLOT_NUM];
    pm_bucket buckets[DATA_PAGE_SLOT_NUM];
    bool in_memory;

} data_page;


typedef struct ehash_catalog
{
    pm_address* buckets_pm_address;         // pm address array of buckets
    pm_bucket*  buckets_virtual_address;    // virtual address array mapped by pmem_map
} ehash_catalog;

typedef struct ehash_metadata
{
    uint64_t max_file_id;   // next file id that can be allocated
    uint64_t catalog_size;  // the catalog size of catalog file(amount of data entry)
    uint64_t global_depth;  // global depth of PmEHash
    int depth_count[MAX_GLOBAL_DEPTH + 1];
    ehash_metadata(uint64_t c_size = DEFAULT_CATALOG_SIZE, uint64_t depth = 4, uint64_t id = 10) {
        max_file_id = id;
        catalog_size = c_size;
        global_depth = depth;
    }
} ehash_metadata;

class PmEHash
{
private:


    ehash_metadata* metadata;       // virtual address of metadata, mapping the metadata file
    ehash_catalog   catalog;        // the catalog of hash

    queue<pm_bucket*> free_list;        //all free slots in data pages to store buckets
    map<pm_bucket*, pm_address> vAddr2pmAddr;       // virtual address map to pm_address, used to find specific pm_address

    //new add
    //unordered_map<uint64_t, pm_bucket*> bucketTable;
    //pm_bucket* bucketTable[MAX_CATALOG_SIZE + 1];
    pm_bucket** bucketTable;
    data_page* page_pointers[MAX_PAGE_ID + 1];
    int Is_pmem[MAX_PAGE_ID + 1];
    bool at_begin; //是否处于哈希表的初始状态，即未有任何数据被插入
    bool page_in_memory[MAX_PAGE_ID + 1];
    int depth_count[MAX_GLOBAL_DEPTH + 1];
    bool destroyed;

    uint64_t hashFunc(uint64_t key);

    pm_bucket* getFreeBucket(uint64_t key);
    pm_bucket* getNewBucket();
    void freeEmptyBucket(pm_bucket* bucket);
    kv* getFreeKvSlot(pm_bucket* bucket);

    void splitBucket(uint64_t bucket_id);
    void mergeBucket(uint64_t bucket_id);

    void extendCatalog();
    //void* getNewSlot(pm_address& new_address);
    void allocNewPage();

    void recover();
    void mapAllPage();

    //new add functions
    void persist(void* virtual_address, size_t map_len);
    inline pm_bucket* key_to_bucket(uint64_t key);
    void writeBack();
    void deletePage(uint32_t page_id);
    void SetCatalogToSameBucket(uint64_t cur_id, uint64_t add, pm_bucket* bucket, pm_bucket* new_bucket);

public:
    PmEHash();
    ~PmEHash();

    int insert(kv new_kv_pair);
    int remove(uint64_t key);
    int update(kv kv_pair);
    int search(uint64_t key, uint64_t& return_val);
    void selfDestroy();

    uint64_t minTrueBucket(uint64_t bucket_id);
//仅作为测试的打印函数
    void print() ;
    void print(int key);

};



#endif
