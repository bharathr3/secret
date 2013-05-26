#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <fstream>
#include <cstring>
#include <iostream>
#include "../pf/pf.h"
#include "../rm/rm.h"

# define IX_EOF (-1)  // end of the index scan
/*const int twice_order = 340;
 const int order = 170;*/

const int twice_order = 312;
const int order = 156;

using namespace std;

class IX_IndexHandle;

class IX_Manager {

public:
	static IX_Manager* Instance();

	RC CreateIndex(const string tableName, // create new index
			const string attributeName);
	RC DestroyIndex(const string tableName, // destroy an index
			const string attributeName);
	RC OpenIndex(const string tableName, // open an index
			const string attributeName, IX_IndexHandle &indexHandle);
	RC CloseIndex(IX_IndexHandle &indexHandle); // close index

protected:
	IX_Manager(); // Constructor
	~IX_Manager(); // Destructor

private:
	static IX_Manager *_ix_manager;
};

template<class T>
struct node {
	int n;
	T keys[twice_order];
	node() {
		for (int i = 0; i < twice_order; i++) {
			keys[i] = -1;
		}
		n = 0;
	}
};

template<class T>
struct internal_node: node<T> {
	int ptrs[twice_order + 1];
	internal_node() {
		for (int i = 0; i < twice_order + 1; i++) {
			ptrs[i] = -1;
		}
	}
};

template<class T>
struct leaf_node: node<T> {
	RID rids[twice_order];
	bool dup[twice_order];
	int front_ptr;
	int back_ptr;
	leaf_node() {
		for (int i = 0; i < twice_order; i++) {
			rids[i].pageNum = 4096;
			rids[i].slotNum = 4096;
			dup[i] = false;
		}
		front_ptr = -1;
		back_ptr = -1;
	}
};

class IX_IndexHandle {
public:
	PF_FileHandle fileHandle;
	IX_IndexHandle& operator=(const IX_IndexHandle &x) {
		this->fileHandle = x.fileHandle;
		return (*this);
	}
	IX_IndexHandle(); // Constructor
	~IX_IndexHandle(); // Destructor

	// The following two functions are using the following format for the passed key value.
	//  1) data is a concatenation of values of the attributes
	//  2) For int and real: use 4 bytes to store the value;
	//     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
	RC InsertEntry(void *key, const RID &rid); // Insert new index entry
	RC DeleteEntry(void *key, const RID &rid); // Delete index entry

	template<class T> RC insert_recursive(int nodepointer, T k, const RID &rid,
			T &newchildentry, int &page, int front_ptr, int back_ptr);
	void update_index_catalog(int root);
	template<class T> RC delete_recursive(int nodepointer, T k, RID rid,
			int &flag);
};

class IX_IndexScan {
	vector<RID> rids;
	unsigned Iterator;
public:
	IX_IndexScan() {
		Iterator = 0;
	}
	; // Constructor
	~IX_IndexScan(); // Destructor

	// for the format of "value", please see IX_IndexHandle::InsertEntry()
	// Opens a scan on the index in the range (lowKey, highKey)
	//
	// If lowKeyInclusive (or highKeyInclusive) is true, then lowKey (or highKey)
	// should be included in the scan
	//
	// If lowKey is null, then the range is -infinity to highKey
	// If highKey is null, then the range is lowKey to +infinity
	RC OpenScan(const IX_IndexHandle &indexHandle, void *lowKey, void *highKey,
			bool lowKeyInclusive, bool highKeyInclusive);

	template<class T> RC scan_int(PF_FileHandle &fileHandle, CompOp compOp,
			T val, int root);
	template<class T> RC scan_real(PF_FileHandle &fileHandle, CompOp compOp1,
			CompOp compOp2, T val, T val2, int root);

	RC GetNextEntry(RID &rid); // Get next matching entry
	RC CloseScan(); // Terminate index scan
};

// print out the error message for a given return code
void IX_PrintError(RC rc);
#endif
