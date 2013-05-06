#ifndef _pf_h_
#define _pf_h_
#include <fstream>
#include <map>
#include <set>
#include <vector>
#include <iostream>
using namespace std;

struct CacheBlock {
	string fname;
	int pg_num;
	void* data;
	bool d_bit;
};

typedef int RC;
typedef unsigned PageNum;

#define PF_PAGE_SIZE 4096

class PF_FileHandle;
class Cache_Policy;

class PF_Manager {
public:
	static Cache_Policy *_fifo_cache;
	static PF_Manager* Instance(int cacheNumPages); // Access to the _pf_manager instance
	//static CacheRepPolicy *_lru_cache;

	RC CreateFile(const char *fileName); // Create a new file
	RC DestroyFile(const char *fileName); // Destroy a file
	RC OpenFile(const char *fileName, PF_FileHandle &fileHandle); // Open a file
	RC CloseFile(PF_FileHandle &fileHandle); // Close a file

protected:
	PF_Manager(int cacheNumPages); // Constructor
	~PF_Manager(); // Destructor

private:
	static PF_Manager *_pf_manager;
};

class PF_FileHandle {
public:
	fstream file; // Added File for Filehandler
	string filename;

	PF_FileHandle(); // Default constructor
	~PF_FileHandle(); // Destructor

	PF_FileHandle& operator=(const PF_FileHandle &pf) {
		(this->file).copyfmt(pf.file);
		(this->file).clear(pf.file.rdstate());
		(this->file).basic_ios<char>::rdbuf(pf.file.rdbuf());

		return (*this);
	}

	RC ReadPage(PageNum pageNum, void *data); // Get a specific page
	RC WritePage(PageNum pageNum, const void *data); // Write a specific page
	RC AppendPage(const void *data); // Append a specific page
	unsigned GetNumberOfPages(); // Get the number of pages in the file
};

class Cache_Policy {
public:
	int cache_counter;
	int num_blocks;
	vector<CacheBlock*> _fifo_cache_list;

	Cache_Policy(int numpages);
	~Cache_Policy();
};

#endif
