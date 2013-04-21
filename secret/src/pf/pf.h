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
class CacheRepPolicy;

class PF_Manager
{
public:
	static PF_Manager* Instance(int cacheNumPages);                                      // Access to the _pf_manager instance
	static CacheRepPolicy *_lru_cache;

	RC CreateFile    (const char *fileName);                            // Create a new file
	RC DestroyFile   (const char *fileName);                            // Destroy a file
	RC OpenFile      (const char *fileName, PF_FileHandle &fileHandle); // Open a file
	RC CloseFile     (PF_FileHandle &fileHandle);                       // Close a file

protected:
	PF_Manager(int cacheNumPages);                                                       // Constructor
	~PF_Manager();                                                   // Destructor

private:
	static PF_Manager *_pf_manager;
};

class PF_FileHandle
{
public:
	fstream file;														// Added File for Filehandler
	char *filename;
	set<int> f_pages;

	PF_FileHandle();                                                    // Default constructor
	~PF_FileHandle();                                                   // Destructor

	RC ReadPage(PageNum pageNum, void *data);                           // Get a specific page
	RC WritePage(PageNum pageNum, const void *data);                    // Write a specific page
	RC AppendPage(const void *data);                                    // Append a specific page
	unsigned GetNumberOfPages();                                        // Get the number of pages in the file
};

struct CacheBlock
{
	string key;
	void* data;
	bool d_bit;
	CacheBlock* prev;
	CacheBlock* next;
};

class CacheRepPolicy
{
private:
	map<string,CacheBlock*> h_map;
	vector<CacheBlock*> empty_blocks;
	CacheBlock *begin;
	CacheBlock *end;
	CacheBlock *list;
public:
	CacheRepPolicy(size_t size);
	~CacheRepPolicy();

	void set(string key,void* data,int rw);
	void* get(string key);

private:
	void remove_end(CacheBlock* block);
	void to_front(CacheBlock* block);
};

#endif
