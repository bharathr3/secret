#include "ix.h"
#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <string.h>

using namespace std;

PF_Manager *pf = PF_Manager::Instance(10);

IX_Manager* IX_Manager::_ix_manager = 0;

IX_Manager* IX_Manager::Instance() {
	if (!_ix_manager)
		_ix_manager = new IX_Manager();
	return _ix_manager;
}

IX_Manager::IX_Manager() {
}

IX_Manager::~IX_Manager() {
}

RC IX_Manager::CreateIndex(const string tableName, const string attributeName) {
	RM *rm = RM::Instance();
	PF_FileHandle fileHandle;
	string fileName = tableName + "." + attributeName;
	RC rc = pf->CreateFile(fileName.c_str());
	if (rc == -1)
		return rc;
	rc = pf->OpenFile(fileName.c_str(), fileHandle);
	if (rc == -1)
		return rc;
	void *data = malloc(PF_PAGE_SIZE);
	int root = 0;
	int m = twice_order;
	memcpy((char*) data, &root, sizeof(int));
	vector<Attribute> attrs;
	rc = rm->getAttributes(tableName, attrs);
	if (rc == -1)
		return rc;
	AttrType type;
	for (unsigned i = 0; i < attrs.size(); i++) {
		if (attrs[i].name == attributeName)
			{
			type = attrs[i].type;
			m = attrs[i].length;
			}
	}
	m=8160/(2*m+16);
	memcpy((char*) data + 4, &type, sizeof(type));
	memcpy((char*) data + 8, &m, sizeof(int));
	rc = fileHandle.AppendPage(data);
	rc = pf->CloseFile(fileHandle);
	free(data);
	return rc;
}

RC IX_Manager::DestroyIndex(const string tableName,
		const string attributeName) {
	string fileName = tableName + "." + attributeName;
	return pf->DestroyFile(fileName.c_str());
}

RC IX_Manager::OpenIndex(const string tableName, const string attributeName,
		IX_IndexHandle &indexHandle) {
	string fileName = tableName + "." + attributeName;
	if (!indexHandle.fileHandle.file.is_open()) {
		indexHandle.fileHandle.file.open(fileName.c_str(),
				ios::in | ios::out | ios::binary);
		if (indexHandle.fileHandle.file.is_open())
			return 0;
		return -1;
	} else
		return -1;
}

RC IX_Manager::CloseIndex(IX_IndexHandle &indexHandle) {
	if (indexHandle.fileHandle.file.is_open()) {
		indexHandle.fileHandle.file.close();
		return 0;
	}
	return 0;
}

IX_IndexHandle::IX_IndexHandle() {
}

IX_IndexHandle::~IX_IndexHandle() {
	pf->CloseFile(fileHandle);
}

/*RC IX_IndexHandle::SetHandle(const char *fileName) {
 fileHandle.file.open(fileName, ios::in | ios::out | ios::binary);
 if (fileHandle.file.is_open())
 return 0;
 return -1;
 //return fileHandle.SetHandle(fileName);
 }
 RC IX_IndexHandle::DropHandle() {
 if (fileHandle.file.is_open()) {
 fileHandle.file.close();
 return 0;
 }
 return -1;
 //return fileHandle.DropHandle();
 }
 RC IX_IndexHandle::IsFree() {
 if (fileHandle.file.is_open())
 return -1;
 else
 return 0;
 //return fileHandle.IsFree();
 }*/

RC IX_IndexHandle::InsertEntry(void *key, const RID &rid) {
	if (!fileHandle.file.is_open())
		return 1;
	int nodepointer, flag = 0;
	void *data = malloc(PF_PAGE_SIZE);
	fileHandle.ReadPage(0, data);
	memcpy(&nodepointer, (int*) data, sizeof(int));
	int page = -1;
	int fp = -1, bp = -1;
	AttrType type;
	memcpy(&type, (char*) data + 4, sizeof(type));
	int kInt, newchildentryInt = -1;
	float kFloat, newchildentryFloat = -1;
	if (type == TypeInt) {
		memcpy(&kInt, (char*) key, sizeof(kInt));
		flag = insert(nodepointer, kInt, rid, newchildentryInt, page, fp, bp);
	} else if (type == TypeReal) {
		memcpy(&kFloat, (char*) key, sizeof(kFloat));
		flag = insert(nodepointer, kFloat, rid, newchildentryFloat, page, fp,
				bp);
	}
	free(data);
	if (flag == 1)
		return 1;
	return 0;
}

void IX_IndexHandle::update_book(int root) {
	void *data = malloc(PF_PAGE_SIZE);
	fileHandle.ReadPage(0, data);
	memcpy((char*) data, &root, sizeof(root));
	fileHandle.WritePage(0, data);
	free(data);
}

template<class T>
RC IX_IndexHandle::insert(int nodepointer, T k, const RID &rid,
		T &newchildentry, int &page, int fp, int bp) {
	int flag;
	int exists = 0;
	if (nodepointer < -1) {
		leaf_node<T> L;
		L.keys[0] = k;
		L.rids[0] = rid;
		L.n++;
		L.fp = fp;
		L.bp = bp;
		void *data = malloc(PF_PAGE_SIZE);
		int x = 1;
		memcpy((char*) data, &x, sizeof(x));
		memcpy((char*) data + 4, &L, sizeof(L));
		nodepointer = -(nodepointer);
		x = fileHandle.WritePage(nodepointer, data);
		page = nodepointer;
		if (fp != -1) {
			x = fileHandle.ReadPage(fp, data);
			memcpy(&L, (char*) data + 4, sizeof(L));
			L.bp = nodepointer;
			memcpy((char*) data + 4, &L, sizeof(L));
			x = fileHandle.WritePage(fp, data);
		}
		if (bp != -1) {
			x = fileHandle.ReadPage(bp, data);
			memcpy(&L, (char*) data + 4, sizeof(L));
			L.fp = nodepointer;
			memcpy((char*) data + 4, &L, sizeof(L));
			x = fileHandle.WritePage(bp, data);
		}
		free(data);
		return exists;
	}
	if (nodepointer == -1) {
		leaf_node<T> L;
		L.keys[0] = k;
		L.rids[0].pageNum = rid.pageNum;
		L.rids[0].slotNum = rid.slotNum;
		L.n++;
		L.fp = fp;
		L.bp = bp;
		void *data = malloc(PF_PAGE_SIZE);
		flag = 1;
		memcpy((char*) data, &flag, sizeof(int));
		memcpy((char*) data + 4, &L, sizeof(L));
		int nop = fileHandle.GetNumberOfPages();
		fileHandle.AppendPage(data);
		page = nop;
		if (fp != -1) {
			fileHandle.ReadPage(fp, data);
			memcpy(&L, (char*) data + 4, sizeof(L));
			L.bp = page;
			memcpy((char*) data + 4, &L, sizeof(L));
			fileHandle.WritePage(fp, data);
		}
		if (bp != -1) {
			fileHandle.ReadPage(bp, data);
			memcpy(&L, (char*) data + 4, sizeof(L));
			L.fp = page;
			memcpy((char*) data + 4, &L, sizeof(L));
			fileHandle.WritePage(bp, data);
		}
		free(data);
		return exists;
	}
	if (nodepointer == 0) {
		internal_node<T> N;
		N.keys[N.n] = k;
		N.n++;
		N.ptrs[1] = 2;
		leaf_node<T> L;
		L.keys[L.n] = k;
		L.rids[L.n] = rid;
		L.n++;
		void *data = malloc(PF_PAGE_SIZE);
		int flag = 0;
		memcpy((int*) data, &flag, sizeof(flag));
		memcpy((char*) data + 4, &N, sizeof(N));
		fileHandle.AppendPage(data);
		flag = 1;
		memcpy((int*) data, &flag, sizeof(int));
		memcpy((char*) data + 4, &L, sizeof(L));
		fileHandle.AppendPage(data);
		nodepointer = 1;
		update_book(nodepointer);
		free(data);
		return exists;
	}
	void *data = malloc(PF_PAGE_SIZE);
	int x = fileHandle.ReadPage(nodepointer, data);
	memcpy(&x, (int*) data, sizeof(int));
	int j;
	if (x == 0) {
		internal_node<T> N;
		memcpy(&N, (char*) data + 4, sizeof(N));
		int i;
		for (i = 0; i < N.n && N.keys[i] <= k; i++)
			;
		int oldnodepointer = nodepointer;
		nodepointer = N.ptrs[i];
		if (i < twice_order) {
			if (N.ptrs[i + 1] != -1) {
				fp = N.ptrs[i + 1];
			} else if (fp != -1) {
				void *data1 = malloc(PF_PAGE_SIZE);
				x = fileHandle.ReadPage(fp, data1);
				internal_node<T> tmp;
				memcpy(&tmp, (char*) data1 + 4, sizeof(tmp));
				fp = tmp.ptrs[0];
				free(data1);
			}
		}
		if (i > 0) {
			if (N.ptrs[i - 1] != -1) {
				bp = N.ptrs[i - 1];
			} else if (bp != -1) {
				void *data1 = malloc(PF_PAGE_SIZE);
				x = fileHandle.ReadPage(bp, data1);
				internal_node<T> tmp;
				memcpy(&tmp, (char*) data1 + 4, sizeof(tmp));
				bp = tmp.ptrs[tmp.n];
				free(data1);
			}
		}
		exists = insert(nodepointer, k, rid, newchildentry, page, fp, bp);
		if (exists == 1) {
			free(data);
			return exists;
		}
		nodepointer = oldnodepointer;
		if (newchildentry == -1) {
			if (page != -1) {
				N.ptrs[i] = page;
				memcpy((char*) data + 4, &N, sizeof(N));
				x = fileHandle.WritePage(nodepointer, data);
				page = -1;
			}
			free(data);
			return 0;
		}
		if (N.n < twice_order) {
			for (j = N.n; j > i; j--) {
				N.keys[j] = N.keys[j - 1];
				N.ptrs[j + 1] = N.ptrs[j];
			}
			N.ptrs[j + 1] = page;
			page = -1;
			N.keys[j] = newchildentry;
			newchildentry = -1;
			N.n++;
			memcpy((char*) data + 4, &N, sizeof(N));
			x = fileHandle.WritePage(nodepointer, data);
			free(data);
			return 0;
		}
		internal_node<T> N2;
		T *keys;
		int *ptrs;
		keys = (T *) malloc((twice_order + 1) * sizeof(T));
		ptrs = (int *) malloc((twice_order + 2) * sizeof(int));
		for (int i = 0; i < twice_order + 1; i++) {
			ptrs[i] = -1;
			keys[i] = -1;
		}
		ptrs[i] = -1;
		for (int i = 0; i < twice_order && N.keys[i] <= k; i++) {
			ptrs[i] = N.ptrs[i];
			keys[i] = N.keys[i];
		}
		ptrs[i] = N.ptrs[i];
		keys[i] = newchildentry;
		i++;
		ptrs[i] = page;
		for (; i < twice_order + 1; i++) {
			keys[i] = N.keys[i - 1];
			ptrs[i + 1] = N.ptrs[i];
		}
		internal_node<T> N1;
		for (i = 0; i < order; i++) {
			N1.ptrs[i] = ptrs[i];
			N1.keys[i] = keys[i];
			N1.n++;
		}
		N1.ptrs[i] = ptrs[i];
		i++;
		for (i = order + 1, j = 0; i < twice_order + 1; i++, j++) {
			N2.ptrs[j] = ptrs[i];
			N2.keys[j] = keys[i];
			N2.n++;
		}
		N2.ptrs[j] = ptrs[i];
		memcpy((char*) data + 4, &N1, sizeof(N1));
		x = fileHandle.WritePage(nodepointer, data);
		memcpy((char*) data + 4, &N2, sizeof(N2));
		int nop = fileHandle.GetNumberOfPages();
		x = fileHandle.AppendPage(data);
		page = nop;
		newchildentry = keys[order];
		x = fileHandle.ReadPage(0, data);
		int root;
		memcpy(&root, (int*) data, sizeof(int));
		if (root == nodepointer) {
			internal_node<T> N3;
			N3.keys[0] = keys[order];
			N3.ptrs[0] = nodepointer;
			N3.ptrs[1] = page;
			N3.n++;
			nop = fileHandle.GetNumberOfPages();
			flag = 0;
			memcpy((int*) data, &flag, sizeof(int));
			memcpy((char*) data + 4, &N3, sizeof(N3));
			x = fileHandle.AppendPage(data);
			page = nop;
			update_book(page);
		}
		free(data);
		free(keys);
		free(ptrs);
		return 0;
	} else {
		leaf_node<T> L;
		int i;
		memcpy(&L, (char*) data + 4, sizeof(L));
		for (i = 0; i < L.n; i++)
			if (L.keys[i] == k && L.rids[i].pageNum == rid.pageNum
					&& L.rids[i].slotNum == rid.slotNum) {
				free(data);
				return 1;
			}
		if (L.n < twice_order) {
			for (i = 0; i < L.n && L.keys[i] <= k; i++)
				;
			if (i == L.n) {
				L.keys[L.n] = k;
				L.rids[L.n] = rid;
			} else {
				for (j = L.n; j > i; j--) {
					L.keys[j] = L.keys[j - 1];
					L.rids[j] = L.rids[j - 1];
				}
				L.keys[i] = k;
				L.rids[i] = rid;
			}
			L.n++;
			memcpy((char*) data + 4, &L, sizeof(L));
			x = fileHandle.WritePage(nodepointer, data);
			newchildentry = -1;
			free(data);
			return 0;
		} else {
			leaf_node<T> L2;
			T *keys = (T *) malloc((twice_order + 1) * sizeof(T));
			RID *rids = (RID *) malloc((twice_order + 1) * sizeof(RID));
			for (i = 0; L.keys[i] <= k && i < L.n; i++) {
				keys[i] = L.keys[i];
				rids[i] = L.rids[i];
			}
			keys[i] = k;
			rids[i] = rid;
			i++;
			for (; i < twice_order + 1; i++) {
				keys[i] = L.keys[i - 1];
				rids[i] = L.rids[i - 1];
			}
			leaf_node<T> L1;
			L1.fp = L.fp;
			L1.bp = L.bp;
			for (i = 0; i < order; i++) {
				L1.keys[i] = keys[i];
				L1.rids[i] = rids[i];
				L1.n++;
			}
			for (j = 0; i < twice_order + 1; i++, j++) {
				L2.keys[j] = keys[i];
				L2.rids[j] = rids[i];
				L2.n++;
			}
			int nop = fileHandle.GetNumberOfPages();
			L2.bp = nodepointer;
			L2.fp = L1.fp;
			L1.fp = nop;
			int y = 1;
			memcpy((char*) data, &y, sizeof(int));
			memcpy((char*) data + 4, &L1, sizeof(L1));
			x = fileHandle.WritePage(nodepointer, data);
			memcpy((char*) data, &y, sizeof(int));
			memcpy((char*) data + 4, &L2, sizeof(L2));
			x = fileHandle.AppendPage(data);
			page = nop;
			newchildentry = L2.keys[0];
			leaf_node<T> L3;
			x = fileHandle.ReadPage(L2.fp, data);
			memcpy(&L3, (char*) data + 4, sizeof(L3));
			L3.bp = L1.fp;
			memcpy((char*) data + 4, &L3, sizeof(L3));
			x = fileHandle.WritePage(L2.fp, data);
			free(keys);
			free(rids);
			free(data);
			return 0;
		}
	}
}

RC IX_IndexScan::OpenScan(const IX_IndexHandle &indexHandle, void *lowKey,
		void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
	PF_FileHandle &fileHandle =
			const_cast<PF_FileHandle &>(indexHandle.fileHandle);
	if (!fileHandle.file.is_open())
		return 1;
	void *data = malloc(PF_PAGE_SIZE);
	fileHandle.ReadPage(0, data);
	rids.clear();
	Iterator = 0;
	state = 1;
	AttrType type;
	CompOp compop;
	int root;
	memcpy(&root, (char*) data, sizeof(int));
	memcpy(&type, (char*) data + 4, sizeof(type));
	free(data);
	int vIntLow = -1, vIntHigh = -1;
	float vFloatLow = -1, vFloatHigh = -1;
	//cout<<"type:"<<type<<endl;
	if (type == TypeInt) {
		//cout<<"TypeInt!"<<endl;
		if (lowKey != NULL)
			memcpy(&vIntLow, (char*) lowKey, sizeof(vIntLow));
		if (highKey != NULL)
			memcpy(&vIntHigh, (char*) highKey, sizeof(vIntHigh));

		if (lowKey == NULL && highKey == NULL) {
			compop = NO_OP;
			return openscan(fileHandle, compop, vIntLow, root);
		} else if (lowKey == NULL) {
			if (highKeyInclusive == true) {
				compop = LE_OP;
				return openscan(fileHandle, compop, vIntHigh, root);
			} else if (highKeyInclusive == false) {
				compop = LT_OP;
				return openscan(fileHandle, compop, vIntHigh, root);
			}
		} else if (highKey == NULL) {
			if (lowKeyInclusive == true) {
				compop = GE_OP;
				return openscan(fileHandle, compop, vIntLow, root);
			} else if (lowKeyInclusive == false) {
				compop = GT_OP;
				return openscan(fileHandle, compop, vIntLow, root);
			}
		} else {
			compop = EQ_OP;
			if (lowKeyInclusive == true && highKeyInclusive == true) {
				for (int i = vIntLow; i <= vIntHigh; i++)
					openscan(fileHandle, compop, i, root);
				return 0;
			} else if (lowKeyInclusive == false && highKeyInclusive == false) {
				for (int i = vIntLow + 1; i < vIntHigh; i++)
					openscan(fileHandle, compop, i, root);
				return 0;
			} else if (lowKeyInclusive == false && highKeyInclusive == true) {
				for (int i = vIntLow + 1; i <= vIntHigh; i++)
					openscan(fileHandle, compop, i, root);
				return 0;
			} else if (lowKeyInclusive == true && highKeyInclusive == false) {
				for (int i = vIntLow; i < vIntHigh; i++)
					openscan(fileHandle, compop, i, root);
				return 0;
			}
		}
	} else if (type == TypeReal) {
		//cout<<"TypeReal!"<<endl;
		if (lowKey != NULL)
			memcpy(&vFloatLow, (char*) lowKey, sizeof(vFloatLow));
		if (highKey != NULL)
			memcpy(&vFloatHigh, (char*) highKey, sizeof(vFloatHigh));
		if (lowKey == NULL && highKey == NULL) {
			compop = NO_OP;
			return openscan(fileHandle, compop, vIntLow, root);
		} else if (lowKey == NULL) {
			if (highKeyInclusive == true) {
				compop = LE_OP;
				return openscan(fileHandle, compop, vFloatHigh, root);
			} else if (highKeyInclusive == false) {
				compop = LT_OP;
				return openscan(fileHandle, compop, vFloatHigh, root);
			}
		} else if (highKey == NULL) {
			if (lowKeyInclusive == true) {
				compop = GE_OP;
				return openscan(fileHandle, compop, vFloatLow, root);
			} else if (lowKeyInclusive == false) {
				compop = GT_OP;
				return openscan(fileHandle, compop, vFloatLow, root);
			}
		} else {
			if (lowKeyInclusive == true && highKeyInclusive == true) {
				openscan2(fileHandle, LE_OP, GE_OP, vFloatLow, vFloatHigh,
						root);
				return 0;
			} else if (lowKeyInclusive == false && highKeyInclusive == false) {
				openscan2(fileHandle, LT_OP, GT_OP, vFloatLow, vFloatHigh,
						root);
				return 0;
			} else if (lowKeyInclusive == false && highKeyInclusive == true) {
				openscan2(fileHandle, LT_OP, GE_OP, vFloatLow, vFloatHigh,
						root);
				return 0;
			} else if (lowKeyInclusive == true && highKeyInclusive == false) {
				openscan2(fileHandle, LE_OP, GT_OP, vFloatLow, vFloatHigh,
						root);
				return 0;
			}
		}
	}
	return 1;
}

template<class T>
RC IX_IndexScan::openscan2(PF_FileHandle &fileHandle, CompOp compOp1,
		CompOp compOp2, T val1, T val2, int root) {
	void *data = malloc(PF_PAGE_SIZE);
	int flag = 1;
	int i;
	fileHandle.ReadPage(root, data);
	//cout<<"HI!"<<endl;
	memcpy(&flag, (int*) data, sizeof(int));
	while (flag == 0) {
		internal_node<T> N;
		memcpy(&N, (char*) data + 4, sizeof(N));
		for (i = 0; i < N.n + 1 && N.ptrs[i] < 0; i++)
			;
		if (i == N.n + 1)
			break;
		fileHandle.ReadPage(N.ptrs[i], data);
		memcpy(&flag, (int*) data, sizeof(int));
		//cout<<"HELLO"<<endl;
	}
	leaf_node<T> L;
	memcpy(&L, (char*) data + 4, sizeof(L));
	int fp = 0;
	if (compOp1 == LT_OP && compOp2 == GT_OP) {
		while (fp != -1) {
			for (i = 0; i < L.n; i++) {
				if (L.keys[i] < val2 && L.keys[i] > val1) {
					//intScan.push_back(L.keys[i]);
					rids.push_back(L.rids[i]);
				}
			}
			fp = L.fp;
			if (fp != -1) {
				fileHandle.ReadPage(fp, data);
				memcpy(&L, (char*) data + 4, sizeof(L));
			}
		}
	} else if (compOp1 == LE_OP && compOp2 == GT_OP) {
		while (fp != -1) {
			for (i = 0; i < L.n; i++) {
				if (L.keys[i] < val2 && L.keys[i] >= val1) {
					//intScan.push_back(L.keys[i]);
					rids.push_back(L.rids[i]);
				}
			}
			fp = L.fp;
			if (fp != -1) {
				fileHandle.ReadPage(fp, data);
				memcpy(&L, (char*) data + 4, sizeof(L));
			}
		}
	} else if (compOp1 == LT_OP && compOp2 == GE_OP) {
		while (fp != -1) {
			for (i = 0; i < L.n; i++) {
				if (L.keys[i] <= val2 && L.keys[i] > val1) {
					//intScan.push_back(L.keys[i]);
					rids.push_back(L.rids[i]);
				}
			}
			fp = L.fp;
			if (fp != -1) {
				fileHandle.ReadPage(fp, data);
				memcpy(&L, (char*) data + 4, sizeof(L));
			}
		}

	} else if (compOp1 == LE_OP && compOp2 == GE_OP) {
		while (fp != -1) {
			for (i = 0; i < L.n; i++) {
				if (L.keys[i] <= val2 && L.keys[i] >= val1) {
					//intScan.push_back(L.keys[i]);
					rids.push_back(L.rids[i]);
				}
			}
			fp = L.fp;
			if (fp != -1) {
				fileHandle.ReadPage(fp, data);
				memcpy(&L, (char*) data + 4, sizeof(L));
			}
		}
	}
	free(data);
	return 0;
}

template<class T>
RC IX_IndexScan::openscan(PF_FileHandle &fileHandle, CompOp compOp, T val,
		int root) {
	void *data = malloc(PF_PAGE_SIZE);
	int flag = 1;
	int i;
	fileHandle.ReadPage(root, data);
	switch (compOp) {
	case NE_OP: {
		//cout<<"HI!"<<endl;
		memcpy(&flag, (int*) data, sizeof(int));
		while (flag == 0) {
			internal_node<T> N;
			memcpy(&N, (char*) data + 4, sizeof(N));
			for (i = 0; i < N.n + 1 && N.ptrs[i] < 0; i++)
				;
			if (i == N.n + 1)
				break;
			fileHandle.ReadPage(N.ptrs[i], data);
			memcpy(&flag, (int*) data, sizeof(int));
		}
		leaf_node<T> L;
		memcpy(&L, (char*) data + 4, sizeof(L));
		int fp = 0;
		while (fp != -1) {
			for (i = 0; i < L.n; i++) {
				//intScan.push_back(L.keys[i]);
				if (val != L.keys[i])
					rids.push_back(L.rids[i]);
			}
			fp = L.fp;
			if (fp != -1) {
				fileHandle.ReadPage(fp, data);
				memcpy(&L, (char*) data + 4, sizeof(L));
			}
		}
	}
		break;
	case EQ_OP: {
		memcpy(&flag, (int*) data, sizeof(int));
		while (flag == 0) {
			internal_node<T> N;
			memcpy(&N, (char*) data + 4, sizeof(N));
			for (i = 0; i < N.n && N.keys[i] <= val; i++)
				;
			if (N.ptrs[i] < 0)
				break;
			fileHandle.ReadPage(N.ptrs[i], data);
			memcpy(&flag, (int*) data, sizeof(int));
		}
		leaf_node<T> L;
		memcpy(&L, (char*) data + 4, sizeof(L));
		for (i = 0; i < L.n && val != L.keys[i]; i++)
			;
		if (val == L.keys[i]) {
			//intScan.push_back(val);
			rids.push_back(L.rids[i]);
		}
	}
		break;
	case LT_OP: {
		//cout<<"HI!"<<endl;
		memcpy(&flag, (int*) data, sizeof(int));
		while (flag == 0) {
			internal_node<T> N;
			memcpy(&N, (char*) data + 4, sizeof(N));
			for (i = 0; i < N.n + 1 && N.ptrs[i] < 0; i++)
				;
			if (i == N.n + 1)
				break;
			fileHandle.ReadPage(N.ptrs[i], data);
			memcpy(&flag, (int*) data, sizeof(int));
			//cout<<"HELLO"<<endl;
		}
		leaf_node<T> L;
		memcpy(&L, (char*) data + 4, sizeof(L));
		int fp = 0;
		while (fp != -1) {
			for (i = 0; i < L.n; i++) {
				if (L.keys[i] < val) {
					//intScan.push_back(L.keys[i]);
					rids.push_back(L.rids[i]);
				}
			}
			fp = L.fp;
			if (fp != -1) {
				fileHandle.ReadPage(fp, data);
				memcpy(&L, (char*) data + 4, sizeof(L));
			}
		}
	}
		break;
	case GT_OP: {
		//cout<<"HI!"<<endl;
		memcpy(&flag, (int*) data, sizeof(int));
		while (flag == 0) {
			internal_node<T> N;
			memcpy(&N, (char*) data + 4, sizeof(N));
			/*
			 cout<<"Processing following node :"<<endl;
			 display_node(N);
			 cout<<"------------------------------"<<endl;
			 */
			for (i = 0; i < N.n + 1 && N.ptrs[i] < 0; i++)
				;
			if (i == N.n + 1)
				break;
			fileHandle.ReadPage(N.ptrs[i], data);
			memcpy(&flag, (int*) data, sizeof(int));
		}
		leaf_node<T> L;
		memcpy(&L, (char*) data + 4, sizeof(L));
		/*
		 cout<<"Entered the leaves at the following leaf:"<<endl;
		 display_leaf(L);
		 cout<<"-----------------------------------------"<<endl;
		 */
		int fp = 0;
		while (fp != -1) {
			for (i = 0; i < L.n; i++) {
				if (L.keys[i] > val) {
					//intScan.push_back(L.keys[i]);
					rids.push_back(L.rids[i]);
				}
			}
			fp = L.fp;
			//display_leaf(L);
			//cout<<fp<<endl;
			if (fp != -1) {
				fileHandle.ReadPage(fp, data);
				memcpy(&L, (char*) data + 4, sizeof(L));
				//display_leaf(L);
			}
		}
	}
		break;
	case LE_OP: {
		//cout<<"HI!"<<endl;
		memcpy(&flag, (int*) data, sizeof(int));
		internal_node<T> N;
		while (flag == 0) {
			memcpy(&N, (char*) data + 4, sizeof(N));
			for (i = 0; i < N.n + 1 && N.ptrs[i] < 0; i++)
				;
			if (i == N.n + 1)
				break;
			fileHandle.ReadPage(N.ptrs[i], data);
			memcpy(&flag, (int*) data, sizeof(int));
		}
		if (i == N.n + 1)
			break;
		leaf_node<T> L;
		memcpy(&L, (char*) data + 4, sizeof(L));
		int fp = 0;
		while (fp != -1) {
			for (i = 0; i < L.n; i++) {
				if (L.keys[i] <= val) {
					//intScan.push_back(L.keys[i]);
					rids.push_back(L.rids[i]);
				}
			}
			fp = L.fp;
			if (fp != -1) {
				fileHandle.ReadPage(fp, data);
				memcpy(&L, (char*) data + 4, sizeof(L));
			}
		}
	}
		break;
	case GE_OP: {
		//cout<<"HI!"<<endl;
		memcpy(&flag, (int*) data, sizeof(int));
		while (flag == 0) {
			internal_node<T> N;
			memcpy(&N, (char*) data + 4, sizeof(N));
			for (i = 0; i < N.n + 1 && N.ptrs[i] < 0; i++)
				;
			if (i == N.n + 1)
				break;
			fileHandle.ReadPage(N.ptrs[i], data);
			memcpy(&flag, (int*) data, sizeof(int));
		}
		leaf_node<T> L;
		memcpy(&L, (char*) data + 4, sizeof(L));
		int fp = 0;
		while (fp != -1) {
			for (i = 0; i < L.n; i++) {
				if (L.keys[i] >= val) {
					//intScan.push_back(L.keys[i]);
					rids.push_back(L.rids[i]);
				}
			}
			fp = L.fp;
			if (fp != -1) {
				fileHandle.ReadPage(fp, data);
				memcpy(&L, (char*) data + 4, sizeof(L));
			}
		}
	}
		break;
	case NO_OP: {
		//cout<<"HI!"<<endl;
		memcpy(&flag, (int*) data, sizeof(int));
		while (flag == 0) {
			internal_node<T> N;
			memcpy(&N, (char*) data + 4, sizeof(N));
			for (i = 0; i < N.n + 1 && N.ptrs[i] < 0; i++)
				;
			if (i == N.n + 1)
				break;
			fileHandle.ReadPage(N.ptrs[i], data);
			memcpy(&flag, (int*) data, sizeof(int));
		}
		leaf_node<T> L;
		memcpy(&L, (char*) data + 4, sizeof(L));
		int fp = 0;
		while (fp != -1) {
			for (i = 0; i < L.n; i++) {
				//intScan.push_back(L.keys[i]);
				rids.push_back(L.rids[i]);
			}
			fp = L.fp;
			if (fp != -1) {
				fileHandle.ReadPage(fp, data);
				memcpy(&L, (char*) data + 4, sizeof(L));
			}
		}
	}
		break;
	}
	free(data);
	return 0;
}

RC IX_IndexScan::GetNextEntry(RID &rid) {
	if (Iterator == rids.size())
		return -1;
	rid = rids[Iterator];
	Iterator++;
	return 0;
}

RC IX_IndexScan::CloseScan() {
	if (state == 1) {
		state = 0;
		Iterator = 0;
		return 0;
	}
	return 1;
}

RC IX_IndexHandle::DeleteEntry(void *key, const RID &rid) {
	void *data = malloc(PF_PAGE_SIZE);
	fileHandle.ReadPage(0, data);
	int root;
	memcpy(&root, (char*) data, sizeof(int));
	int nodepointer = root;
	AttrType type;
	memcpy(&type, (char*) data + 4, sizeof(type));
	int kInt;
	float kFloat;
	int flag = 0;
	if (type == TypeInt) {
		memcpy(&kInt, (char*) key, sizeof(kInt));
		free(data);
		return delete1(nodepointer, kInt, rid, flag);
	} else if (type == TypeReal) {
		memcpy(&kFloat, (char*) key, sizeof(kFloat));
		free(data);
		return delete1(nodepointer, kFloat, rid, flag);
	}
	free(data);
	return 1;
}

template<class T>
RC IX_IndexHandle::delete1(int nodepointer, T k, RID rid, int &flag) {
	if (!fileHandle.file.is_open())
		return 1;
	if (nodepointer == -1)
		return 1;
	int x;
	int old;
	int returnval;
	void *data = malloc(PF_PAGE_SIZE);
	x = fileHandle.ReadPage(nodepointer, data);
	memcpy(&x, (char*) data, sizeof(int));
	if (x == 0) {
		internal_node<T> N;
		memcpy(&N, (char*) data + 4, sizeof(N));
		int i;
		for (i = 0; i < N.n && N.keys[i] <= k; i++)
			;
		old = nodepointer;
		nodepointer = N.ptrs[i];
		returnval = delete1(nodepointer, k, rid, flag);
		if (returnval == 1) {
			free(data);
			return 1;
		}
		/*if (flag == 1) {
		 cout<<"FLAG ONE HERE"<<endl;
		 N.ptrs[i] = -1;
		 memcpy((char*) data + 4, &N, sizeof(N));
		 x = fileHandle.WritePage(old, data);
		 flag = 0;
		 }*/
	} else {
		leaf_node<T> L;
		memcpy(&L, (char*) data + 4, sizeof(L));
		int i;
		for (i = 0; i < L.n && L.keys[i] != k; i++)
			;
		if (i == L.n) {
			free(data);
			return 1;
		}
		for (; i < L.n - 1; i++) {
			L.keys[i] = L.keys[i + 1];
			L.rids[i] = L.rids[i + 1];
		}
		L.keys[L.n - 1] = -1;
		L.rids[L.n - 1].pageNum = 4096;
		L.rids[L.n - 1].slotNum = 4096;
		L.n--;
		memcpy((char*) data + 4, &L, sizeof(L));
		x = fileHandle.WritePage(nodepointer, data);
		if (L.n == 0)
			flag = -(nodepointer);
		//flag=1;
	}
	free(data);
	return 0;
}

IX_IndexScan::~IX_IndexScan() {
}
