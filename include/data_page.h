#ifndef DATA_PAGE
#define DATA_PAGE
//#include "pm_ehash.h"
//#define DATA_PAGE_SLOT_NUM 16
// use pm_address to locate the data in the page

// uncompressed page format design to store the buckets of PmEHash
// one slot stores one bucket of PmEHash
// 一个页有16个槽，每个槽可以存放一个可扩展哈西中的bucket
// 一个页的结构存储在一个文件中
//typedef struct data_page {
//    // fixed-size record design
//    // uncompressed page format
//    pm_bucket buckets[DATA_PAGE_SLOT_NUM];
//} data_page;

#endif
