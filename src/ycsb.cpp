#include"../include/pm_ehash.h"
#include <fstream>
#include <string>
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
const char* loads[7]{"../workloads/1w-rw-50-50-load.txt", "../workloads/10w-rw-0-100-load.txt", "../workloads/10w-rw-25-75-load.txt", "../workloads/10w-rw-50-50-load.txt"
                , "../workloads/10w-rw-75-25-load.txt", "../workloads/10w-rw-100-0-load.txt", "../workloads/220w-rw-50-50-load.txt"};
const char* runs[7]{"../workloads/1w-rw-50-50-run.txt", "../workloads/10w-rw-0-100-run.txt", "../workloads/10w-rw-25-75-run.txt", "../workloads/10w-rw-50-50-run.txt"
                , "../workloads/10w-rw-75-25-run.txt", "../workloads/10w-rw-100-0-run.txt", "../workloads/220w-rw-50-50-run.txt"};

string op;
uint64_t num;
stringstream ss;
string key;


kv convert(uint64_t num) {
    kv key_and_val;
    key_and_val.value = num;
    ss.clear();
    ss << num;
    ss >> key;
    key = key.substr(0, 8);
    ss.clear();
    ss << key;
    ss >> key_and_val.key;
    return key_and_val;
}
void test(const char* load, const char* run) {
    PmEHash t;
    ifstream in(load, ios::in);
    while(in >> op) {

        in >> num;
        kv key_and_val = convert(num);

        t.insert(key_and_val);

    }
    in.close();
    clock_t start, finish;
    double  duration;
    /* 测量一个事件持续的时间*/

    start = clock();

    in.open(run);
    uint64_t temp;
    while(in >> op) {
        in >> num;
        if (op == "UPDATE") {
            kv key_and_val = convert(num);
            t.update(key_and_val);
        }
        else if (op == "READ") {
            kv key_and_val = convert(num);
            t.search(key_and_val.key, temp);
        }
        else if (op == "INSERT"){
            kv key_and_val = convert(num);
            t.insert(key_and_val);
        }
        else {
            cout << op << endl;
        }
    }
    in.close();

    finish = clock();
    duration = (double)(finish - start) / CLOCKS_PER_SEC;
    printf( "run time of %s : %f seconds\n", run, duration );
    t.selfDestroy();
}
int main() {
    for (int i = 0; i < 7; ++i) {
        test(loads[i], runs[i]);
    }
//    PmEHash t;
//    for (int i = 0; i < 10000; ++i) {
//        t.insert(kv(i,i));
//    }
//    for (int i = 10000; i >= 10; --i) {
//        t.remove(i);
//    }
//    t.print();
    return 0;

}
