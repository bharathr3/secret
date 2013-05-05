#ifndef _pf_h_
#define _pf_h_
#include <fstream>
#include <map>
#include <set>
#include <vector>
#include <iostream>
using namespace std;

typedef int RC;
typedef unsigned PageNum;

#define PF_PAGE_SIZE 4096

class PF_FileHandle;
//class CacheRepPolicy;

class PF_Manager {
public:
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
	//char *filename;

	PF_FileHandle(); // Default constructor
	~PF_FileHandle(); // Destructor

	PF_FileHandle& operator=(const PF_FileHandle &pf)
	       {
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

/*struct block_info {
	char* fname;
	int pg_num;
};

struct CacheBlock {
	block_info block;
	void* data;
	bool d_bit;
	CacheBlock* prev;
	CacheBlock* next;
};

namespace std {
template<> struct less<block_info> {
	bool operator()(const block_info& lhs, const block_info& rhs) {
		return lhs.pg_num < rhs.pg_num;
	}
};
}

class CacheRepPolicy {
private:
	CacheBlock *begin;
	CacheBlock *end;
	CacheBlock *list;
public:
	map<block_info, CacheBlock*> h_map;
	multimap<char*, int> f_pages;
	vector<CacheBlock*> empty_blocks;
	CacheRepPolicy(size_t size);
	~CacheRepPolicy();

	void set(block_info key, void* data, int rw);
	int get(block_info key,CacheBlock& ret);

private:
	void remove_end(CacheBlock* block);
	void to_front(CacheBlock* block);
};*/

#endif
