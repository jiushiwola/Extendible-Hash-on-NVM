#include "../include/pm_ehash.h"
// 数据页表的相关操作实现都放在这个源文件下，如PmEHash申请新的数据页和删除数据页的底层实现
#include <iostream>

extern const char* PM_EHASH_DIRECTORY;        // add your own directory path to store the pm_ehash
void PmEHash::deletePage(uint32_t page_id) {
    cout << "Delete page " << page_id << endl;
    page_pointers[page_id]->in_memory = page_in_memory[page_id] = false;
    persist(&page_pointers[page_id]->in_memory, sizeof(page_pointers[page_id]->in_memory));
    pmem_unmap(page_pointers[page_id], sizeof(data_page));
//更新free_list
    int cnt = free_list.size();
    for (int i = 0; i < cnt; ++i) {
        pm_bucket* bucket = free_list.front();
        free_list.pop();
        if (page_in_memory[vAddr2pmAddr[bucket].fileId] == true) free_list.push(bucket);
    }
}

void PmEHash::mapAllPage() {
//将页面作内存映射，映射到pmem0挂载的地方;
//初始化free_list
    for (uint64_t i = 1; i < metadata->max_file_id; ++i) {
        char filename[30];
        sprintf(filename, "%s%ld", PM_EHASH_DIRECTORY, i);
        int PMEM_LEN = sizeof(data_page);
        size_t mapped_len;
        if ((page_pointers[i] = (data_page*)pmem_map_file(filename, PMEM_LEN, PMEM_FILE_CREATE,
			0666, &mapped_len, &Is_pmem[i])) == NULL) {
            perror("pmem_map_file");
            exit(1);
        }

    }
    if (at_begin == false) {
        for (uint64_t i = 1; i < metadata->max_file_id; ++i) {
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

void PmEHash::allocNewPage() {
    int id = 0;
    for (uint64_t i = 1; i < metadata->max_file_id; ++i) {
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


