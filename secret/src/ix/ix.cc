#include "ix.h"
#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <string.h>
#include <math.h>

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
		if (attrs[i].name == attributeName) {
			type = attrs[i].type;
		}
	}
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

RC IX_IndexHandle::InsertEntry(void *key, const RID &rid) {
	if (!fileHandle.file.is_open())
		return 1;
	int nodepointer, flag = 0;
	void *data = malloc(PF_PAGE_SIZE);
	fileHandle.ReadPage(0, data);
	memcpy(&nodepointer, (int*) data, sizeof(int));
	int page = -1, front_ptr = -1, back_ptr = -1;
	AttrType type;
	memcpy(&type, (char*) data + 4, sizeof(type));
	int keyInt = 0, newchildInt = -1;
	float keyFloat = 0, newchildFloat = -1;
	if (type == TypeInt) {
		memcpy(&keyInt, (char*) key, sizeof(keyInt));
		flag = insert_recursive(nodepointer, keyInt, rid, newchildInt, page,
				front_ptr, back_ptr);
	} else if (type == TypeReal) {
		memcpy(&keyFloat, (char*) key, sizeof(keyFloat));
		flag = insert_recursive(nodepointer, keyFloat, rid, newchildFloat, page,
				front_ptr, back_ptr);
	} else if (type == TypeVarChar) {
		int tmp, tmp1;
		int cnt;
		cnt = *((char *) key);
		if (cnt > 3)
			cnt = 3;
		for (int qq = 0; qq < cnt; qq++) {
			tmp = *((char *) key + 4 + qq);
			tmp1 = tmp;
			int lencnt = 0;
			for (; tmp != 0; tmp /= 10, lencnt++)
				;
			keyInt = keyInt * pow(10.0, lencnt) + tmp1;
		}
		flag = insert_recursive(nodepointer, keyInt, rid, newchildInt, page,
				front_ptr, back_ptr);
	}
	free(data);
	if (flag == 1)
		return 1;
	return 0;
}

void IX_IndexHandle::update_index_catalog(int root) {
	void *data = malloc(PF_PAGE_SIZE);
	fileHandle.ReadPage(0, data);
	memcpy((char*) data, &root, sizeof(root));
	fileHandle.WritePage(0, data);
	free(data);
}

template<class T>
RC IX_IndexHandle::insert_recursive(int nodepointer, T k, const RID &rid,
		T &newchild, int &page, int front_ptr, int back_ptr) {
	int flag;
	int exists = 0;
	if (nodepointer < -1) {
		leaf_node<T> L;
		L.keys[0] = k;
		L.rids[0] = rid;
		L.n++;
		L.front_ptr = front_ptr;
		L.back_ptr = back_ptr;
		void *data = malloc(PF_PAGE_SIZE);
		int x = 1;
		memcpy((char*) data, &x, sizeof(x));
		memcpy((char*) data + 4, &L, sizeof(L));
		nodepointer = -(nodepointer);
		fileHandle.WritePage(nodepointer, data);
		page = nodepointer;
		if (front_ptr != -1) {
			fileHandle.ReadPage(front_ptr, data);
			memcpy(&L, (char*) data + 4, sizeof(L));
			L.back_ptr = nodepointer;
			memcpy((char*) data + 4, &L, sizeof(L));
			fileHandle.WritePage(front_ptr, data);
		}
		if (back_ptr != -1) {
			fileHandle.ReadPage(back_ptr, data);
			memcpy(&L, (char*) data + 4, sizeof(L));
			L.front_ptr = nodepointer;
			memcpy((char*) data + 4, &L, sizeof(L));
			fileHandle.WritePage(back_ptr, data);
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
		L.front_ptr = front_ptr;
		L.back_ptr = back_ptr;
		void *data = malloc(PF_PAGE_SIZE);
		flag = 1;
		memcpy((char*) data, &flag, sizeof(int));
		memcpy((char*) data + 4, &L, sizeof(L));
		int no_page = fileHandle.GetNumberOfPages();
		fileHandle.AppendPage(data);
		page = no_page;
		if (front_ptr != -1) {
			fileHandle.ReadPage(front_ptr, data);
			memcpy(&L, (char*) data + 4, sizeof(L));
			L.back_ptr = page;
			memcpy((char*) data + 4, &L, sizeof(L));
			fileHandle.WritePage(front_ptr, data);
		}
		if (back_ptr != -1) {
			fileHandle.ReadPage(back_ptr, data);
			memcpy(&L, (char*) data + 4, sizeof(L));
			L.front_ptr = page;
			memcpy((char*) data + 4, &L, sizeof(L));
			fileHandle.WritePage(back_ptr, data);
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
		update_index_catalog(nodepointer);
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
				front_ptr = N.ptrs[i + 1];
			} else if (front_ptr != -1) {
				void *data1 = malloc(PF_PAGE_SIZE);
				x = fileHandle.ReadPage(front_ptr, data1);
				internal_node<T> tmp;
				memcpy(&tmp, (char*) data1 + 4, sizeof(tmp));
				front_ptr = tmp.ptrs[0];
				free(data1);
			}
		}
		if (i > 0) {
			if (N.ptrs[i - 1] != -1) {
				back_ptr = N.ptrs[i - 1];
			} else if (back_ptr != -1) {
				void *data1 = malloc(PF_PAGE_SIZE);
				x = fileHandle.ReadPage(back_ptr, data1);
				internal_node<T> tmp;
				memcpy(&tmp, (char*) data1 + 4, sizeof(tmp));
				back_ptr = tmp.ptrs[tmp.n];
				free(data1);
			}
		}
		exists = insert_recursive(nodepointer, k, rid, newchild, page,
				front_ptr, back_ptr);
		if (exists == 1) {
			free(data);
			return exists;
		}
		nodepointer = oldnodepointer;
		if (newchild == -1) {
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
			N.keys[j] = newchild;
			newchild = -1;
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
		keys[i] = newchild;
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
		int no_page = fileHandle.GetNumberOfPages();
		x = fileHandle.AppendPage(data);
		page = no_page;
		newchild = keys[order];
		x = fileHandle.ReadPage(0, data);
		int root;
		memcpy(&root, (int*) data, sizeof(int));
		if (root == nodepointer) {
			internal_node<T> N3;
			N3.keys[0] = keys[order];
			N3.ptrs[0] = nodepointer;
			N3.ptrs[1] = page;
			N3.n++;
			no_page = fileHandle.GetNumberOfPages();
			flag = 0;
			memcpy((int*) data, &flag, sizeof(int));
			memcpy((char*) data + 4, &N3, sizeof(N3));
			x = fileHandle.AppendPage(data);
			page = no_page;
			update_index_catalog(page);
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
			if (L.keys[i] == k) {
				if (L.dup[i] == false) {
					RID tmp;
					tmp.pageNum = L.rids[i].pageNum;
					tmp.slotNum = L.rids[i].slotNum;
					leaf_node<T> L_dup;
					L_dup.keys[L_dup.n] = k;
					L_dup.rids[L_dup.n].pageNum = tmp.pageNum;
					L_dup.rids[L_dup.n].slotNum = tmp.slotNum;
					L_dup.n++;
					L_dup.keys[L_dup.n] = k;
					L_dup.rids[L_dup.n].pageNum = rid.pageNum;
					L_dup.rids[L_dup.n].slotNum = rid.slotNum;
					L_dup.n++;
					void *datar = malloc(PF_PAGE_SIZE);
					int flag = 1;
					memcpy((int*) datar, &flag, sizeof(int));
					memcpy((char*) datar + 4, &L_dup, sizeof(L_dup));
					fileHandle.AppendPage(datar);
					int pg_cnt = fileHandle.GetNumberOfPages();
					L.rids[i].pageNum = pg_cnt - 1;
					L.rids[i].slotNum = -1;
					L.dup[i] = true;
					memcpy((char*) data + 4, &L, sizeof(L));
					x = fileHandle.WritePage(nodepointer, data);
					free(data);
					free(datar);
					return 0;
				} else {
					void *data11 = malloc(PF_PAGE_SIZE);
					fileHandle.ReadPage(L.rids[i].pageNum, data11);
					leaf_node<T> L_tmp;
					memcpy(&L_tmp, (char*) data11 + 4, sizeof(L_tmp));
					if (L_tmp.n < twice_order) {
						L_tmp.keys[L_tmp.n] = k;
						L_tmp.rids[L_tmp.n].pageNum = rid.pageNum;
						L_tmp.rids[L_tmp.n].slotNum = rid.slotNum;
						L_tmp.n++;
						memcpy((char*) data11 + 4, &L_tmp, sizeof(L_tmp));
						x = fileHandle.WritePage(L.rids[i].pageNum, data11);
						free(data);
						free(data11);
						return 0;
					} else {
						int prev_page = L.rids[i].pageNum;
						while (L_tmp.front_ptr != -1) {
							prev_page = L_tmp.front_ptr;
							fileHandle.ReadPage(L_tmp.front_ptr, data11);
							memcpy(&L_tmp, (char*) data11 + 4, sizeof(L_tmp));
						}
						if (L_tmp.n < twice_order) {
							L_tmp.keys[L_tmp.n] = k;
							L_tmp.rids[L_tmp.n].pageNum = rid.pageNum;
							L_tmp.rids[L_tmp.n].slotNum = rid.slotNum;
							L_tmp.n++;
							memcpy((char*) data11 + 4, &L_tmp, sizeof(L_tmp));
							x = fileHandle.WritePage(prev_page, data11);
							free(data);
							free(data11);
							return 0;
						} else {
							leaf_node<T> L_dup;
							L_dup.keys[L_dup.n] = k;
							L_dup.rids[L_dup.n].pageNum = rid.pageNum;
							L_dup.rids[L_dup.n].slotNum = rid.slotNum;
							L_dup.n++;
							void *datar = malloc(PF_PAGE_SIZE);
							int flag = 1;
							memcpy((int*) datar, &flag, sizeof(int));
							memcpy((char*) datar + 4, &L_dup, sizeof(L_dup));
							fileHandle.AppendPage(datar);
							int pg_cnt = fileHandle.GetNumberOfPages() - 1;
							fileHandle.ReadPage(prev_page, data11);
							leaf_node<T> L_tmp;
							memcpy(&L_tmp, (char*) data11 + 4, sizeof(L_tmp));
							L_tmp.front_ptr = pg_cnt;
							memcpy((char*) data11 + 4, &L_tmp, sizeof(L_tmp));
							x = fileHandle.WritePage(prev_page, data11);
							free(data);
							free(data11);
							free(datar);
							return 0;
						}
					}
				}
			}
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
			newchild = -1;
			free(data);
			return 0;
		} else {
			leaf_node<T> L2;
			T *keys = (T *) malloc((twice_order + 1) * sizeof(T));
			RID *rids = (RID *) malloc((twice_order + 1) * sizeof(RID));
			bool *dups = (bool *) malloc((twice_order + 1) * sizeof(bool));
			for (i = 0; L.keys[i] <= k && i < L.n; i++) {
				keys[i] = L.keys[i];
				rids[i] = L.rids[i];
				dups[i] = L.dup[i];
			}
			keys[i] = k;
			rids[i] = rid;
			dups[i] = false;
			i++;
			for (; i < twice_order + 1; i++) {
				keys[i] = L.keys[i - 1];
				rids[i] = L.rids[i - 1];
				dups[i] = L.dup[i - 1];
			}
			leaf_node<T> L1;
			L1.front_ptr = L.front_ptr;
			L1.back_ptr = L.back_ptr;
			for (i = 0; i < order; i++) {
				L1.keys[i] = keys[i];
				L1.rids[i] = rids[i];
				L1.dup[i] = dups[i];
				L1.n++;
			}
			for (j = 0; i < twice_order + 1; i++, j++) {
				L2.keys[j] = keys[i];
				L2.rids[j] = rids[i];
				L2.dup[j] = dups[i];
				L2.n++;
			}
			int no_page = fileHandle.GetNumberOfPages();
			L2.back_ptr = nodepointer;
			L2.front_ptr = L1.front_ptr;
			L1.front_ptr = no_page;
			int y = 1;
			memcpy((char*) data, &y, sizeof(int));
			memcpy((char*) data + 4, &L1, sizeof(L1));
			x = fileHandle.WritePage(nodepointer, data);
			memcpy((char*) data, &y, sizeof(int));
			memcpy((char*) data + 4, &L2, sizeof(L2));
			x = fileHandle.AppendPage(data);
			page = no_page;
			newchild = L2.keys[0];
			leaf_node<T> L3;
			x = fileHandle.ReadPage(L2.front_ptr, data);
			memcpy(&L3, (char*) data + 4, sizeof(L3));
			L3.back_ptr = L1.front_ptr;
			memcpy((char*) data + 4, &L3, sizeof(L3));
			x = fileHandle.WritePage(L2.front_ptr, data);
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
	AttrType type;
	CompOp compop;
	int root;
	memcpy(&root, (char*) data, sizeof(int));
	memcpy(&type, (char*) data + 4, sizeof(type));
	free(data);
	int vIntLow = -1, vIntHigh = -1;
	float vFloatLow = -1, vFloatHigh = -1;
	if (type == TypeInt) {
		if (lowKey != NULL)
			memcpy(&vIntLow, (char*) lowKey, sizeof(vIntLow));
		if (highKey != NULL)
			memcpy(&vIntHigh, (char*) highKey, sizeof(vIntHigh));
		if (lowKey == NULL && highKey == NULL) {
			compop = NO_OP;
			return scan_int(fileHandle, compop, vIntLow, root);
		} else if (lowKey == NULL) {
			if (highKeyInclusive == true) {
				compop = LE_OP;
				return scan_int(fileHandle, compop, vIntHigh, root);
			} else if (highKeyInclusive == false) {
				compop = LT_OP;
				return scan_int(fileHandle, compop, vIntHigh, root);
			}
		} else if (highKey == NULL) {
			if (lowKeyInclusive == true) {
				compop = GE_OP;
				return scan_int(fileHandle, compop, vIntLow, root);
			} else if (lowKeyInclusive == false) {
				compop = GT_OP;
				return scan_int(fileHandle, compop, vIntLow, root);
			}
		} else {
			compop = EQ_OP;
			if (lowKeyInclusive == true && highKeyInclusive == true) {
				for (int i = vIntLow; i <= vIntHigh; i++)
					scan_int(fileHandle, compop, i, root);
				return 0;
			} else if (lowKeyInclusive == false && highKeyInclusive == false) {
				for (int i = vIntLow + 1; i < vIntHigh; i++)
					scan_int(fileHandle, compop, i, root);
				return 0;
			} else if (lowKeyInclusive == false && highKeyInclusive == true) {
				for (int i = vIntLow + 1; i <= vIntHigh; i++)
					scan_int(fileHandle, compop, i, root);
				return 0;
			} else if (lowKeyInclusive == true && highKeyInclusive == false) {
				for (int i = vIntLow; i < vIntHigh; i++)
					scan_int(fileHandle, compop, i, root);
				return 0;
			}
		}
	} else if (type == TypeReal) {
		if (lowKey != NULL)
			memcpy(&vFloatLow, (char*) lowKey, sizeof(vFloatLow));
		if (highKey != NULL)
			memcpy(&vFloatHigh, (char*) highKey, sizeof(vFloatHigh));
		if (lowKey == NULL && highKey == NULL) {
			compop = NO_OP;
			return scan_int(fileHandle, compop, vIntLow, root);
		} else if (lowKey == NULL) {
			if (highKeyInclusive == true) {
				compop = LE_OP;
				return scan_int(fileHandle, compop, vFloatHigh, root);
			} else if (highKeyInclusive == false) {
				compop = LT_OP;
				return scan_int(fileHandle, compop, vFloatHigh, root);
			}
		} else if (highKey == NULL) {
			if (lowKeyInclusive == true) {
				compop = GE_OP;
				return scan_int(fileHandle, compop, vFloatLow, root);
			} else if (lowKeyInclusive == false) {
				compop = GT_OP;
				return scan_int(fileHandle, compop, vFloatLow, root);
			}
		} else {
			if (lowKeyInclusive == true && highKeyInclusive == true) {
				scan_real(fileHandle, LE_OP, GE_OP, vFloatLow, vFloatHigh,
						root);
				return 0;
			} else if (lowKeyInclusive == false && highKeyInclusive == false) {
				scan_real(fileHandle, LT_OP, GT_OP, vFloatLow, vFloatHigh,
						root);
				return 0;
			} else if (lowKeyInclusive == false && highKeyInclusive == true) {
				scan_real(fileHandle, LT_OP, GE_OP, vFloatLow, vFloatHigh,
						root);
				return 0;
			} else if (lowKeyInclusive == true && highKeyInclusive == false) {
				scan_real(fileHandle, LE_OP, GT_OP, vFloatLow, vFloatHigh,
						root);
				return 0;
			}
		}
	} else if (type == TypeVarChar) {
		if (lowKey != NULL) {
			vIntLow = 0;
			int tmp, tmp1;
			int cnt;
			cnt = *((char *) lowKey);
			if (cnt > 3)
				cnt = 3;
			for (int qq = 0; qq < cnt; qq++) {
				tmp = *((char *) lowKey + 4 + qq);
				tmp1 = tmp;
				int lencnt = 0;
				for (; tmp != 0; tmp /= 10, lencnt++)
					;
				vIntLow = vIntLow * pow(10.0, lencnt) + tmp1;
			}
		}
		if (highKey != NULL) {
			vIntHigh = 0;
			int tmp, tmp1;
			int cnt;
			cnt = *((char *) highKey);
			if (cnt > 3)
				cnt = 3;
			for (int qq = 0; qq < cnt; qq++) {
				tmp = *((char *) highKey + 4 + qq);
				tmp1 = tmp;
				int lencnt = 0;
				for (; tmp != 0; tmp /= 10, lencnt++)
					;
				vIntHigh = vIntHigh * pow(10.0, lencnt) + tmp1;
			}
		}
		if (lowKey == NULL && highKey == NULL) {
			compop = NO_OP;
			return scan_int(fileHandle, compop, vIntLow, root);
		} else if (lowKey == NULL) {
			if (highKeyInclusive == true) {
				compop = LE_OP;
				return scan_int(fileHandle, compop, vIntHigh, root);
			} else if (highKeyInclusive == false) {
				compop = LT_OP;
				return scan_int(fileHandle, compop, vIntHigh, root);
			}
		} else if (highKey == NULL) {
			if (lowKeyInclusive == true) {
				compop = GE_OP;
				return scan_int(fileHandle, compop, vIntLow, root);
			} else if (lowKeyInclusive == false) {
				compop = GT_OP;
				return scan_int(fileHandle, compop, vIntLow, root);
			}
		} else {
			compop = EQ_OP;
			if (vIntLow == vIntHigh) {
				scan_int(fileHandle, compop, vIntLow, root);
				return 0;
			} else if (lowKeyInclusive == true && highKeyInclusive == true) {
				for (int i = vIntLow; i <= vIntHigh; i++)
					scan_int(fileHandle, compop, i, root);
				return 0;
			} else if (lowKeyInclusive == false && highKeyInclusive == false) {
				for (int i = vIntLow + 1; i < vIntHigh; i++)
					scan_int(fileHandle, compop, i, root);
				return 0;
			} else if (lowKeyInclusive == false && highKeyInclusive == true) {
				for (int i = vIntLow + 1; i <= vIntHigh; i++)
					scan_int(fileHandle, compop, i, root);
				return 0;
			} else if (lowKeyInclusive == true && highKeyInclusive == false) {
				for (int i = vIntLow; i < vIntHigh; i++)
					scan_int(fileHandle, compop, i, root);
				return 0;
			}
		}
	}
	return 1;
}

template<class T>
RC IX_IndexScan::scan_real(PF_FileHandle &fileHandle, CompOp compOp1,
		CompOp compOp2, T val1, T val2, int root) {
	void *data = malloc(PF_PAGE_SIZE);
	int flag = 1;
	int i;
	fileHandle.ReadPage(root, data);
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
	if (compOp1 == LT_OP && compOp2 == GT_OP) {
		while (fp != -1) {
			for (i = 0; i < L.n; i++) {
				if (L.keys[i] < val2 && L.keys[i] > val1) {
					if (L.dup[i] == true) {
						void *dataa = malloc(PF_PAGE_SIZE);
						fileHandle.ReadPage(L.rids[i].pageNum, dataa);
						leaf_node<T> L_1;
						memcpy(&L_1, (char*) dataa + 4, sizeof(L_1));
						for (int qi = 0; qi < L_1.n; qi++)
							rids.push_back(L_1.rids[qi]);
						while (L_1.front_ptr != -1) {
							fileHandle.ReadPage(L_1.front_ptr, dataa);
							memcpy(&L_1, (char*) dataa + 4, sizeof(L_1));
							for (int qi = 0; qi < L_1.n; qi++)
								rids.push_back(L_1.rids[qi]);
						}
						free(dataa);
					} else
						rids.push_back(L.rids[i]);
				}
			}
			fp = L.front_ptr;
			if (fp != -1) {
				fileHandle.ReadPage(fp, data);
				memcpy(&L, (char*) data + 4, sizeof(L));
			}
		}
	} else if (compOp1 == LE_OP && compOp2 == GT_OP) {
		while (fp != -1) {
			for (i = 0; i < L.n; i++) {
				if (L.keys[i] < val2 && L.keys[i] >= val1) {
					if (L.dup[i] == true) {
						void *dataa = malloc(PF_PAGE_SIZE);
						fileHandle.ReadPage(L.rids[i].pageNum, dataa);
						leaf_node<T> L_1;
						memcpy(&L_1, (char*) dataa + 4, sizeof(L_1));
						for (int qi = 0; qi < L_1.n; qi++)
							rids.push_back(L_1.rids[qi]);
						while (L_1.front_ptr != -1) {
							fileHandle.ReadPage(L_1.front_ptr, dataa);
							memcpy(&L_1, (char*) dataa + 4, sizeof(L_1));
							for (int qi = 0; qi < L_1.n; qi++)
								rids.push_back(L_1.rids[qi]);
						}
						free(dataa);
					} else
						rids.push_back(L.rids[i]);
				}
			}
			fp = L.front_ptr;
			if (fp != -1) {
				fileHandle.ReadPage(fp, data);
				memcpy(&L, (char*) data + 4, sizeof(L));
			}
		}
	} else if (compOp1 == LT_OP && compOp2 == GE_OP) {
		while (fp != -1) {
			for (i = 0; i < L.n; i++) {
				if (L.keys[i] <= val2 && L.keys[i] > val1) {
					if (L.dup[i] == true) {
						void *dataa = malloc(PF_PAGE_SIZE);
						fileHandle.ReadPage(L.rids[i].pageNum, dataa);
						leaf_node<T> L_1;
						memcpy(&L_1, (char*) dataa + 4, sizeof(L_1));
						for (int qi = 0; qi < L_1.n; qi++)
							rids.push_back(L_1.rids[qi]);
						while (L_1.front_ptr != -1) {
							fileHandle.ReadPage(L_1.front_ptr, dataa);
							memcpy(&L_1, (char*) dataa + 4, sizeof(L_1));
							for (int qi = 0; qi < L_1.n; qi++)
								rids.push_back(L_1.rids[qi]);
						}
						free(dataa);
					} else
						rids.push_back(L.rids[i]);
				}
			}
			fp = L.front_ptr;
			if (fp != -1) {
				fileHandle.ReadPage(fp, data);
				memcpy(&L, (char*) data + 4, sizeof(L));
			}
		}
	} else if (compOp1 == LE_OP && compOp2 == GE_OP) {
		while (fp != -1) {
			for (i = 0; i < L.n; i++) {
				if (L.keys[i] <= val2 && L.keys[i] >= val1) {
					if (L.dup[i] == true) {
						void *dataa = malloc(PF_PAGE_SIZE);
						fileHandle.ReadPage(L.rids[i].pageNum, dataa);
						leaf_node<T> L_1;
						memcpy(&L_1, (char*) dataa + 4, sizeof(L_1));
						for (int qi = 0; qi < L_1.n; qi++)
							rids.push_back(L_1.rids[qi]);
						while (L_1.front_ptr != -1) {
							fileHandle.ReadPage(L_1.front_ptr, dataa);
							memcpy(&L_1, (char*) dataa + 4, sizeof(L_1));
							for (int qi = 0; qi < L_1.n; qi++)
								rids.push_back(L_1.rids[qi]);
						}
						free(dataa);
					} else
						rids.push_back(L.rids[i]);
				}
			}
			fp = L.front_ptr;
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
RC IX_IndexScan::scan_int(PF_FileHandle &fileHandle, CompOp compOp, T val,
		int root) {
	void *data = malloc(PF_PAGE_SIZE);
	int flag = 1;
	int i;
	fileHandle.ReadPage(root, data);
	switch (compOp) {
	case NE_OP: {
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
				if (val != L.keys[i]) {
					if (L.dup[i] == true) {
						void *dataa = malloc(PF_PAGE_SIZE);
						fileHandle.ReadPage(L.rids[i].pageNum, dataa);
						leaf_node<T> L_1;
						memcpy(&L_1, (char*) dataa + 4, sizeof(L_1));
						for (int qi = 0; qi < L_1.n; qi++)
							rids.push_back(L_1.rids[qi]);
						while (L_1.front_ptr != -1) {
							fileHandle.ReadPage(L_1.front_ptr, dataa);
							memcpy(&L_1, (char*) dataa + 4, sizeof(L_1));
							for (int qi = 0; qi < L_1.n; qi++)
								rids.push_back(L_1.rids[qi]);
						}
						free(dataa);
					} else
						rids.push_back(L.rids[i]);
				}
			}
			fp = L.front_ptr;
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
			if (L.dup[i] == true) {
				void *dataa = malloc(PF_PAGE_SIZE);
				fileHandle.ReadPage(L.rids[i].pageNum, dataa);
				leaf_node<T> L_1;
				memcpy(&L_1, (char*) dataa + 4, sizeof(L_1));
				for (int qi = 0; qi < L_1.n; qi++)
					rids.push_back(L_1.rids[qi]);
				while (L_1.front_ptr != -1) {
					fileHandle.ReadPage(L_1.front_ptr, dataa);
					memcpy(&L_1, (char*) dataa + 4, sizeof(L_1));
					for (int qi = 0; qi < L_1.n; qi++)
						rids.push_back(L_1.rids[qi]);
				}
				free(dataa);
			} else
				rids.push_back(L.rids[i]);
		}
	}
		break;
	case LT_OP: {
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
				if (L.keys[i] < val) {
					if (L.dup[i] == true) {
						void *dataa = malloc(PF_PAGE_SIZE);
						fileHandle.ReadPage(L.rids[i].pageNum, dataa);
						leaf_node<T> L_1;
						memcpy(&L_1, (char*) dataa + 4, sizeof(L_1));
						for (int qi = 0; qi < L_1.n; qi++)
							rids.push_back(L_1.rids[qi]);
						while (L_1.front_ptr != -1) {
							fileHandle.ReadPage(L_1.front_ptr, dataa);
							memcpy(&L_1, (char*) dataa + 4, sizeof(L_1));
							for (int qi = 0; qi < L_1.n; qi++)
								rids.push_back(L_1.rids[qi]);
						}
						free(dataa);
					} else
						rids.push_back(L.rids[i]);
				}
			}
			fp = L.front_ptr;
			if (fp != -1) {
				fileHandle.ReadPage(fp, data);
				memcpy(&L, (char*) data + 4, sizeof(L));
			}
		}
	}
		break;
	case GT_OP: {
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
				if (L.keys[i] > val) {
					if (L.dup[i] == true) {
						void *dataa = malloc(PF_PAGE_SIZE);
						fileHandle.ReadPage(L.rids[i].pageNum, dataa);
						leaf_node<T> L_1;
						memcpy(&L_1, (char*) dataa + 4, sizeof(L_1));
						for (int qi = 0; qi < L_1.n; qi++)
							rids.push_back(L_1.rids[qi]);
						while (L_1.front_ptr != -1) {
							fileHandle.ReadPage(L_1.front_ptr, dataa);
							memcpy(&L_1, (char*) dataa + 4, sizeof(L_1));
							for (int qi = 0; qi < L_1.n; qi++)
								rids.push_back(L_1.rids[qi]);
						}
						free(dataa);
					} else
						rids.push_back(L.rids[i]);
				}
			}
			fp = L.front_ptr;
			if (fp != -1) {
				fileHandle.ReadPage(fp, data);
				memcpy(&L, (char*) data + 4, sizeof(L));
			}
		}
	}
		break;
	case LE_OP: {
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
					if (L.dup[i] == true) {
						void *dataa = malloc(PF_PAGE_SIZE);
						fileHandle.ReadPage(L.rids[i].pageNum, dataa);
						leaf_node<T> L_1;
						memcpy(&L_1, (char*) dataa + 4, sizeof(L_1));
						for (int qi = 0; qi < L_1.n; qi++)
							rids.push_back(L_1.rids[qi]);
						while (L_1.front_ptr != -1) {
							fileHandle.ReadPage(L_1.front_ptr, dataa);
							memcpy(&L_1, (char*) dataa + 4, sizeof(L_1));
							for (int qi = 0; qi < L_1.n; qi++)
								rids.push_back(L_1.rids[qi]);
						}
						free(dataa);
					} else
						rids.push_back(L.rids[i]);
				}
			}
			fp = L.front_ptr;
			if (fp != -1) {
				fileHandle.ReadPage(fp, data);
				memcpy(&L, (char*) data + 4, sizeof(L));
			}
		}
	}
		break;
	case GE_OP: {
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
					if (L.dup[i] == true) {
						void *dataa = malloc(PF_PAGE_SIZE);
						fileHandle.ReadPage(L.rids[i].pageNum, dataa);
						leaf_node<T> L_1;
						memcpy(&L_1, (char*) dataa + 4, sizeof(L_1));
						for (int qi = 0; qi < L_1.n; qi++)
							rids.push_back(L_1.rids[qi]);
						while (L_1.front_ptr != -1) {
							fileHandle.ReadPage(L_1.front_ptr, dataa);
							memcpy(&L_1, (char*) dataa + 4, sizeof(L_1));
							for (int qi = 0; qi < L_1.n; qi++)
								rids.push_back(L_1.rids[qi]);
						}
						free(dataa);
					} else
						rids.push_back(L.rids[i]);
				}
			}
			fp = L.front_ptr;
			if (fp != -1) {
				fileHandle.ReadPage(fp, data);
				memcpy(&L, (char*) data + 4, sizeof(L));
			}
		}
	}
		break;
	case NO_OP: {
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
				if (L.dup[i] == true) {
					void *dataa = malloc(PF_PAGE_SIZE);
					fileHandle.ReadPage(L.rids[i].pageNum, dataa);
					leaf_node<T> L_1;
					memcpy(&L_1, (char*) dataa + 4, sizeof(L_1));
					for (int qi = 0; qi < L_1.n; qi++)
						rids.push_back(L_1.rids[qi]);
					while (L_1.front_ptr != -1) {
						fileHandle.ReadPage(L_1.front_ptr, dataa);
						memcpy(&L_1, (char*) dataa + 4, sizeof(L_1));
						for (int qi = 0; qi < L_1.n; qi++)
							rids.push_back(L_1.rids[qi]);
					}
					free(dataa);
				} else
					rids.push_back(L.rids[i]);
			}
			fp = L.front_ptr;
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
	Iterator = 0;
	return 0;
}

RC IX_IndexHandle::DeleteEntry(void *key, const RID &rid) {
	void *data = malloc(PF_PAGE_SIZE);
	fileHandle.ReadPage(0, data);
	int root;
	memcpy(&root, (char*) data, sizeof(int));
	int nodepointer = root;
	AttrType type;
	memcpy(&type, (char*) data + 4, sizeof(type));
	int keyInt = 0;
	float keyFloat = 0;
	int flag = 0;
	if (type == TypeInt) {
		memcpy(&keyInt, (char*) key, sizeof(keyInt));
		free(data);
		return delete_recursive(nodepointer, keyInt, rid, flag);
	} else if (type == TypeReal) {
		memcpy(&keyFloat, (char*) key, sizeof(keyFloat));
		free(data);
		return delete_recursive(nodepointer, keyFloat, rid, flag);
	} else if (type == TypeVarChar) {
		int tmp, tmp1;
		int cnt;
		cnt = *((char *) key);
		if (cnt > 3)
			cnt = 3;
		for (int qq = 0; qq < cnt; qq++) {
			tmp = *((char *) key + 4 + qq);
			tmp1 = tmp;
			int lencnt = 0;
			for (; tmp != 0; tmp /= 10, lencnt++)
				;
			keyInt = keyInt * pow(10.0, lencnt) + tmp1;
		}
		memcpy(&keyInt, (char*) key, sizeof(keyInt));
		free(data);
		return delete_recursive(nodepointer, keyInt, rid, flag);
	}
	free(data);
	return 1;
}

template<class T>
RC IX_IndexHandle::delete_recursive(int nodepointer, T key, RID rid,
		int &flag) {
	if (!fileHandle.file.is_open())
		return 1;
	if (nodepointer == -1)
		return 1;
	int x, rt;
	void *data = malloc(PF_PAGE_SIZE);
	x = fileHandle.ReadPage(nodepointer, data);
	memcpy(&x, (char*) data, sizeof(int));
	if (x == 0) {
		internal_node<T> N;
		memcpy(&N, (char*) data + 4, sizeof(N));
		int i;
		for (i = 0; i < N.n && N.keys[i] <= key; i++)
			;
		nodepointer = N.ptrs[i];
		rt = delete_recursive(nodepointer, key, rid, flag);
		if (rt == 1) {
			free(data);
			return 1;
		}
	} else {
		leaf_node<T> L;
		memcpy(&L, (char*) data + 4, sizeof(L));
		int i;
		for (i = 0; i < L.n && L.keys[i] != key; i++)
			;
		if (i == L.n) {
			free(data);
			return 1;
		}
		for (; i < L.n - 1; i++) {
			L.keys[i] = L.keys[i + 1];
			L.rids[i] = L.rids[i + 1];
			L.dup[i] = L.dup[i + 1];
		}
		L.keys[L.n - 1] = -1;
		L.rids[L.n - 1].pageNum = 4096;
		L.rids[L.n - 1].slotNum = 4096;
		L.dup[L.n - 1] = false;
		L.n--;
		memcpy((char*) data + 4, &L, sizeof(L));
		x = fileHandle.WritePage(nodepointer, data);
		if (L.n == 0)
			flag = -(nodepointer);
	}
	free(data);
	return 0;
}

IX_IndexScan::~IX_IndexScan() {
}
