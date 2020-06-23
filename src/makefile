target : pm_ehash.o ycsb.o data_page.o
	g++ -o target pm_ehash.o ycsb.o data_page.o -l pmem

pm_ehash.o : pm_ehash.cpp ../include/pm_ehash.h
	g++ -c pm_ehash.cpp -o pm_ehash.o
ycsb.o : ycsb.cpp ../include/pm_ehash.h
	g++ -c ycsb.cpp -o ycsb.o
data_page.o : data_page.cpp ../include/pm_ehash.h
	g++ -c data_page.cpp -o data_page.o
clean : target pm_ehash.o ycsb.o data_page.o
