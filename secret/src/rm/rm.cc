#include "rm.h"
#include <stdlib.h>
#include <stdio.h>
#include <map>
#include <string.h>
#include <iostream>
#include "../pf/pf.h"

using namespace std;

RM* RM::_rm = 0;

RM* RM::Instance() {
	if (!_rm)
		_rm = new RM();
	return _rm;
}

RM::RM() {
	pf = PF_Manager::Instance(10);
	RC rc1 = pf->CreateFile("TableInfo");
	RC rc2 = pf->CreateFile("ColumnInfo");
	num_tables = 2;
	if (rc1 == -1 && rc2 == -1)
		load_catalog();
	else
		updatecatalog();
}

RM::~RM() {
	delete _rm;
}

void RM::load_catalog() {
	table_cache_item item;
	map<int, int> table_count;
	RC rc;
	PF_FileHandle fileHandle;
	void *buffer = malloc(4096);
	pf->OpenFile("ColumnInfo", fileHandle);
	int num_page = fileHandle.GetNumberOfPages();
	for (int pageNum = 0; pageNum < num_page; pageNum++) {
		rc = fileHandle.ReadPage(pageNum, buffer);
		if (rc == -1)
			break;
		int num_rec = *((unsigned int*) ((char*) buffer + 4096 - 8));
		for (int i = 1; i <= num_rec; i++) {
			RID rid;
			rid.pageNum = pageNum;
			rid.slotNum = i;
			void *data = malloc(4096);
			rc = readTuple("ColumnInfo", rid, data);
			if (rc != -1) {
				int tableid = (*(unsigned int*) ((char*) data)); //first 4 bytes table-id

				table_count.insert(std::pair<int, int>(tableid, 1));
				int colname_length = (*(unsigned int*) ((char*) data + 4)); //next 4 bytes length of column-name
				char colname[4096];
				memcpy(colname, (char*) data + 8, colname_length); //next colname.length bytes for column-name
				colname[colname_length] = '\0'; //String end
				AttrType coltype = (*(AttrType*) ((char*) data + 9
						+ colname_length));
				int collength = (*(unsigned int*) ((char*) data + 13
						+ colname_length));

				item.tablename = gettableName(tableid);
				item.colname = colname;
				item.coltype = coltype;
				item.collength = collength;
				table_cache.push_back(item);
			}
			free(data);
		}
	}
	free(buffer);
	num_tables = table_count.size();
}

RC RM::updatecatalog() {
	RC rc;
	RID rid;
	string tablename = "TableInfo";
	void *buffer = malloc(4096);
	int tableid = 1;
	memcpy(buffer, &tableid, 4);
	int tablename_length = tablename.length();
	memcpy((char *) buffer + 4, &tablename_length, 4);
	memcpy((char *) buffer + 8, tablename.c_str(), tablename.length() + 1);
	memcpy((char *) buffer + 9 + tablename.length(), &tablename_length, 4);
	memcpy((char *) buffer + 13 + tablename.length(), tablename.c_str(),
			tablename.length() + 1);
	rc = insertTuple("TableInfo", buffer, rid);
	free(buffer);
	buffer = malloc(4096);
	tablename = "ColumnInfo";
	tableid = 2;
	memcpy(buffer, &tableid, 4);
	tablename_length = tablename.length();
	memcpy((char *) buffer + 4, &tablename_length, 4);
	memcpy((char *) buffer + 8, tablename.c_str(), tablename.length() + 1);
	memcpy((char *) buffer + 9 + tablename.length(), &tablename_length, 4);
	memcpy((char *) buffer + 13 + tablename.length(), tablename.c_str(),
			tablename.length() + 1);
	rc = insertTuple("TableInfo", buffer, rid);
	free(buffer);
	tablename = "ColumnInfo";
	tableid = 1;
	buffer = malloc(4096);
	vector<Attribute> attrlist;
	Attribute attribute;
	attribute.name = "Tableid";
	attribute.type = TypeInt;
	attribute.length = (AttrLength) 4;
	attrlist.push_back(attribute);
	attribute.name = "TableName";
	attribute.type = TypeVarChar;
	attribute.length = (AttrLength) 30;
	attrlist.push_back(attribute);
	attribute.name = "FileName";
	attribute.type = TypeVarChar;
	attribute.length = (AttrLength) 30;
	attrlist.push_back(attribute);
	int offset;
	for (int i = 0; i < (int) attrlist.size(); i++) {
		table_cache_item item;
		offset = 0;
		memcpy((char *) buffer + offset, &tableid, sizeof(int));
		offset = offset + sizeof(int);
		int a = (attrlist.at(i).name).length();
		memcpy((char *) buffer + offset, &a, sizeof(int));
		offset = offset + sizeof(int);
		item.colname = attrlist.at(i).name;
		memcpy((char *) buffer + offset, attrlist.at(i).name.c_str(), a);
		offset = offset + a;
		int y = attrlist.at(i).type;
		item.coltype = attrlist.at(i).type;
		memcpy((char *) buffer + offset, &y, sizeof(int));
		offset = offset + sizeof(int);
		int l = attrlist.at(i).length;
		item.collength = l;
		item.tablename = "TableInfo";
		memcpy((char *) buffer + offset, &l, sizeof(int));
		offset = offset + sizeof(int);
		rc = insertTuple("ColumnInfo", buffer, rid);
		table_cache.push_back(item);
	}
	attrlist.clear();
	attribute.name = "Tableid";
	attribute.type = TypeInt;
	attribute.length = (AttrLength) 4;
	attrlist.push_back(attribute);
	attribute.name = "ColumnName";
	attribute.type = TypeVarChar;
	attribute.length = (AttrLength) 30;
	attrlist.push_back(attribute);
	attribute.name = "ColumnType";
	attribute.type = TypeVarChar;
	attribute.length = (AttrLength) 4;
	attrlist.push_back(attribute);
	attribute.name = "ColumnLength";
	attribute.type = TypeInt;
	attribute.length = (AttrLength) 4;
	attrlist.push_back(attribute);
	free(buffer);
	buffer = malloc(4096);
	int tableid2 = 2;
	for (int j = 0; j < (int) attrlist.size(); j++) {
		table_cache_item item;
		offset = 0;
		memcpy((char *) buffer + offset, &tableid2, sizeof(int));
		offset = offset + sizeof(int);
		int a = (attrlist.at(j).name).length();
		memcpy((char *) buffer + offset, &a, sizeof(int));
		offset = offset + sizeof(int);
		item.colname = attrlist.at(j).name;
		memcpy((char *) buffer + offset, attrlist.at(j).name.c_str(), a);
		offset = offset + a;
		int y = attrlist.at(j).type;
		item.coltype = attrlist.at(j).type;
		memcpy((char *) buffer + offset, &y, sizeof(int));
		offset = offset + sizeof(int);
		int l = attrlist.at(j).length;
		item.collength = attrlist.at(j).length;
		item.tablename = "ColumnInfo";
		memcpy((char *) buffer + offset, &l, sizeof(int));
		offset = offset + sizeof(int);
		rc = insertTuple("ColumnInfo", buffer, rid);
		table_cache.push_back(item);
	}
	free(buffer);
	return rc;
}

string RM::gettableName(int tableid) {
	PF_FileHandle fileHandle;
	void *buffer = malloc(4096);
	pf->OpenFile("TableInfo", fileHandle);
	int num_page = fileHandle.GetNumberOfPages();
	for (int pageNum = 0; pageNum < num_page; pageNum++) {
		RC rc = fileHandle.ReadPage(pageNum, buffer);
		if (rc == -1)
			break;
		int num_rec = *((unsigned int *) ((char *) buffer + 4096 - 8));
		for (int i = 1; i <= num_rec; i++) {
			RID rid;
			rid.pageNum = pageNum;
			rid.slotNum = i;
			void *data = malloc(4096);
			rc = readTuple("TableInfo", rid, data);
			if (rc != -1) {
				int tid = (*(unsigned int*) ((char*) data)); //first 4 bytes table-id
				int tablelength = (*(unsigned int*) ((char*) data + 4)); //next 4 bytes length of tablename
				char tablename[4096];
				memcpy(tablename, (char *) data + 8, tablelength); //next tablename.length bytes for tablename
				if (tableid == tid)
					return tablename;
			}
			free(data);
		}
	}
	free(buffer);
	return 0;
}

RC RM::readTuple(const string tableName, const RID &rid, void *data) {
	RC rc = 1;
	unsigned int page_id, slot_id;
	RID tempRID;
	PF_FileHandle filehandle;
	void *buffer = malloc(4096);
	unsigned int recordOffset, recordLen;
	rc = pf->OpenFile(tableName.c_str(), filehandle);
	if (rc == -1)
		return -1;
	page_id = rid.pageNum;
	slot_id = rid.slotNum;

	if (page_id >= filehandle.GetNumberOfPages() || slot_id < 1)
		return -1;
	rc = filehandle.ReadPage(page_id, buffer);
	if (rc == -1) {
		pf->CloseFile(filehandle);
		return -1;
	}
	recordOffset = *(unsigned int *) ((char *) buffer + 4096 - 8 - 8 * slot_id);
	recordLen = *(unsigned int *) ((char *) buffer + 4096 - 4 - 8 * slot_id);
	if (recordLen == 5000) {
		pf->CloseFile(filehandle);
		return -1;
	}
	recordLen--;
	if (*(char *) ((char *) buffer + recordOffset) == 1) {
		recordOffset++;
		page_id = *(unsigned int *) ((char *) buffer + recordOffset); //first 4bytes for page_id
		recordOffset += 4;
		slot_id = *(unsigned int *) ((char *) buffer + recordOffset); //next 4bytes for slot_id
		tempRID.pageNum = page_id;
		tempRID.slotNum = slot_id;
		readTuple(tableName, tempRID, data);
	} else {
		recordOffset++;
		memcpy(data, (char*) buffer + recordOffset, recordLen);
	}
	rc = pf->CloseFile(filehandle);
	free(buffer);
	return rc;
}

RC RM::createTable(const string tableName, const vector<Attribute> &attrs) {
	//PF_FileHandle fileHandle;
	cout << endl << tableName.c_str();
	RC rc = pf->CreateFile(tableName.c_str());
	if (rc == -1)
		return -1;
	else {
		void *data = malloc(4096);
		num_tables = num_tables + 1;
		int tableid = num_tables;
		memcpy(data, &tableid, 4);
		int tablename_length = tableName.length();
		memcpy((char *) data + 4, &tablename_length, 4);
		memcpy((char *) data + 8, tableName.c_str(), tableName.length() + 1);
		memcpy((char *) data + 9 + tableName.length(), &tablename_length, 4);
		memcpy((char *) data + 13 + tableName.length(), tableName.c_str(),
				tableName.length() + 1);
		RID rid;
		rc = insertTuple("TableInfo", data, rid);
		free(data);

		for (vector<Attribute>::const_iterator attr = attrs.begin();
				attr != attrs.end(); ++attr) {
			table_cache_item item;
			item.tablename = tableName;
			void *data = malloc(4096);
			memcpy(data, &tableid, 4);
			int colname_length = (attr->name).length();
			memcpy((char *) data + 4, &colname_length, 4);
			string colname = attr->name;
			item.colname = attr->name;
			memcpy((char *) data + 8, colname.c_str(), colname.length() + 1);
			int coltype = attr->type;
			item.coltype = attr->type;
			memcpy((char *) data + 9 + colname.length(), &coltype, 4);
			int collength = attr->length;
			item.collength = attr->length;
			memcpy((char *) data + 13 + colname.length(), &collength, 4);
			rc = insertTuple("ColumnInfo", data, rid);
			table_cache.push_back(item);
			free(data);
		}
	}
	return rc;
}

RC RM::insertTuple(const string tableName, const void *data, RID &rid) {
	RC rc;
	unsigned int lpage_id;
	PF_FileHandle filehandle;
	void *buffer = malloc(4096);
	unsigned int recordLen;
	rc = getRecordLength(tableName, data, recordLen);
	unsigned int num_slots, freespaceoffset;
	rc = pf->OpenFile(tableName.c_str(), filehandle);
	if (rc == -1)
		return -1;
	lpage_id = filehandle.GetNumberOfPages();
	if (lpage_id == 0) {
		num_slots = 0;
		freespaceoffset = 0;
		*(char *) buffer = 0;
		memcpy((char *) buffer + 1, data, recordLen);
		*(unsigned int *) ((char *) buffer + 4096 - 4) = recordLen + 1;
		*(unsigned int *) ((char *) buffer + 4096 - 8) = num_slots + 1;
		*(unsigned int *) ((char *) buffer + 4096 - (4 + 8 * (num_slots + 1))) =
				recordLen + 1;
		*(unsigned int *) ((char *) buffer + 4096 - (8 + 8 * (num_slots + 1))) =
				freespaceoffset;
		rc = filehandle.AppendPage(buffer);
		if (rc == -1) {
			pf->CloseFile(filehandle);
			return -1;
		}
	} else {
		lpage_id--;
		rc = filehandle.ReadPage(lpage_id, buffer);
		if (rc == -1) {
			pf->CloseFile(filehandle);
			return -1;
		}
		freespaceoffset = *((unsigned int *) ((char *) buffer + 4096 - 4));
		num_slots = *((unsigned int *) ((char *) buffer + 4096 - 8));
		int hole_size;
		hole_size = 4096 - 8 - (num_slots * 8) - freespaceoffset - 8;
		if ((int) recordLen < hole_size) {
			*((char *) buffer + freespaceoffset) = 0;
			memcpy((char *) buffer + freespaceoffset + 1, data, recordLen);
			*(unsigned int *) ((char *) buffer + 4096 - 4) = recordLen + 1
					+ freespaceoffset;
			*(unsigned int *) ((char *) buffer + 4096 - 8) = num_slots + 1;
			*(unsigned int *) ((char *) buffer + 4096
					- (4 + 8 * (num_slots + 1))) = recordLen + 1;
			*(unsigned int *) ((char *) buffer + 4096
					- (8 + 8 * (num_slots + 1))) = freespaceoffset;
			rc = filehandle.WritePage(lpage_id, buffer);
			if (rc == -1) {
				pf->CloseFile(filehandle);
				return -1;
			}
		} else {
			memset((char *) buffer, 0, 4096);
			num_slots = 0;
			freespaceoffset = 0;
			*(char *) buffer = 0;
			memcpy((char *) buffer + 1, data, recordLen);
			*(unsigned int *) ((char *) buffer + 4096 - 4) = recordLen + 1;
			*(unsigned int *) ((char *) buffer + 4096 - 8) = num_slots + 1;
			*(unsigned int *) ((char *) buffer + 4096
					- (4 + 8 * (num_slots + 1))) = recordLen + 1;
			*(unsigned int *) ((char *) buffer + 4096
					- (8 + 8 * (num_slots + 1))) = freespaceoffset;
			rc = filehandle.AppendPage(buffer);
			if (rc == -1) {
				pf->CloseFile(filehandle);
				return -1;
			}
		}

	}
	cout << "Freespace offset: "
			<< *((unsigned int *) ((char *) buffer + 4096 - 4)) << endl;
	cout << "Num Slots: " << *((unsigned int *) ((char *) buffer + 4096 - 8))
			<< endl;
	if (filehandle.GetNumberOfPages() == 0)
		rid.pageNum = 0;
	else if (filehandle.GetNumberOfPages() > 0)
		rid.pageNum = filehandle.GetNumberOfPages() - 1;
	int temp = rid.pageNum;
	cout << endl << temp << endl;
	rid.slotNum = num_slots + 1;
	rc = pf->CloseFile(filehandle);
	free(buffer);
	return rc;
}

RC RM::getRecordLength(const string tableName, const void *data,
		unsigned int &length) {
	RC rc = 0;
	length = 0;
	if (tableName == "ColumnInfo" || tableName == "TableInfo") {
		void *buffer;
		buffer = malloc(4096);
		memcpy(buffer, (char *) data + 4, 4);
		unsigned int name_len = *(unsigned int*) buffer;
		if (tableName == "TableInfo")
			length = name_len * 2 + 12;
		if (tableName == "ColumnInfo")
			length = name_len + 16;
		free(buffer);
	} else {
		for (vector<table_cache_item>::const_iterator item =
				table_cache.begin(); item != table_cache.end(); ++item) {
			vector<Attribute> attr;
			if (tableName == item->tablename) {
				if (item->coltype != 2) {
					length += item->collength;
				} else {
					void *buffer;
					buffer = malloc(4);
					memcpy(buffer, (char *) data + length, 4);
					unsigned int name_len = *(unsigned int*) buffer;
					length += name_len + 4;
					free(buffer);
				}
			}
		}
	}
	return rc;
}

RC RM::deleteTable(const string tableName) {
	RM_ScanIterator iter, iter1;
	RC rc;
	RID rid;
	void *data = malloc(4096);
	int table_id = gettableID(tableName);
	string attr = "Tableid";
	vector<string> attributes;
	attributes.push_back(attr);
	rc = scan("ColumnInfo", "", NO_OP, NULL, attributes, iter);
	if (rc == -1)
		return -1;
	while (iter.getNextTuple(rid, data) != RM_EOF) {
		cout << *(int *) data << endl;
		if (table_id == *(int *) data)
			deleteTuple("ColumnInfo", rid);
	}
	free(data);
	iter.close();
	data = malloc(4096);
	rc = scan("TableInfo", "", NO_OP, NULL, attributes, iter1);
	if (rc == -1)
		return -1;
	while (iter1.getNextTuple(rid, data) != RM_EOF) {
		if (table_id == *(int *) data)
			deleteTuple("TableInfo", rid);
	}
	iter1.close();
	free(data);
	vector<table_cache_item>::size_type i = 0;
	while (i < table_cache.size()) {
		if (strcmp(tableName.c_str(), table_cache[i].tablename.c_str()) == 0) {
			table_cache.erase(table_cache.begin() + i);
		} else {
			++i;
		}
	}
	rc = pf->DestroyFile(tableName.c_str());
	return rc;
}

RC RM::deleteTuples(const string tableName) {
	RC rc;
	rc = pf->DestroyFile(tableName.c_str());
	if (rc == -1)
		return rc;
	rc = pf->CreateFile(tableName.c_str());
	return rc;
}

RC RM::deleteTuple(const string tableName, const RID &rid) {
	RC rc = 1;
	unsigned int page_id, slot_id;
	RID tempRID;
	PF_FileHandle filehandle;
	void *buffer = malloc(4096);
	unsigned int recordOffset, recordLen;
	rc = pf->OpenFile(tableName.c_str(), filehandle);
	if (rc == -1)
		return -1;
	page_id = rid.pageNum;
	slot_id = rid.slotNum;
	if (page_id >= filehandle.GetNumberOfPages() || slot_id < 1)
		return -1;
	rc = filehandle.ReadPage(page_id, buffer);
	if (rc == -1) {
		pf->CloseFile(filehandle);
		return -1;
	}

	recordOffset = *(unsigned int *) ((char *) buffer + 4096 - 8 - 8 * slot_id);
	recordLen = *(unsigned int *) ((char *) buffer + 4096 - 4 - 8 * slot_id);
	recordLen--;
	if (*(char *) ((char *) buffer + recordOffset) == 1) {
		unsigned int tslot_id, tpage_id;
		*(char *) ((char *) buffer + recordOffset) = 0;
		recordOffset++;
		tpage_id = *(unsigned int *) ((char *) buffer + recordOffset);
		*(unsigned int *) ((char *) buffer + recordOffset) = 0;
		recordOffset += 4;
		tslot_id = *(unsigned int *) ((char *) buffer + recordOffset);
		*(unsigned int *) ((char *) buffer + recordOffset) = 0;
		tempRID.pageNum = tpage_id;
		tempRID.slotNum = tslot_id;
		deleteTuple(tableName, tempRID);
	}

	*(unsigned int *) ((char *) buffer + 4096 - 4 - 8 * slot_id) = 5000;
	rc = filehandle.WritePage(rid.pageNum, buffer);
	rc = pf->CloseFile(filehandle);
	free(buffer);
	return rc;
}

RC RM::getAttributes(const string tableName, vector<Attribute> &attrs) {
	Attribute attr;
	for (vector<table_cache_item>::const_iterator table_info =
			table_cache.begin(); table_info != table_cache.end();
			++table_info) {
		cout << tableName << endl << table_info->tablename << endl;
		if (tableName == table_info->tablename) {
			attr.name = table_info->colname;
			attr.type = table_info->coltype;
			attr.length = table_info->collength;
			attrs.push_back(attr);
		}
	}
	return 0;
}

RC RM::readAttribute(const string tableName, const RID &rid,
		const string attributeName, void *data) {
	void *buffer;
	buffer = malloc(4096);
	RC rc = 0;
	rc = readTuple(tableName, rid, buffer);
	unsigned int reclen = 0;
	getRecordLength(tableName, buffer, reclen);
	if (rc == -1)
		return -1;
	else {
		vector<Attribute> attributes;
		getAttributes(tableName, attributes);
		int offset = 0;
		for (vector<Attribute>::const_iterator attr = attributes.begin();
				attr != attributes.end(); ++attr) {
			if (attr->name != attributeName) {
				if (attr->type != 2)
					offset += 4;
				else
					offset += (*(unsigned int*) ((char*) buffer + offset)) + 4;
			} else {
				if (offset < (int) reclen) {
					if (attr->type != 2)
						memcpy((char *) data, (char *) buffer + offset, 4);
					else {
						int data_length = (*(unsigned int*) ((char*) buffer
								+ offset));
						memcpy((char *) data, (char *) buffer + offset,
								data_length + 4);
					}
				} else {
					memcpy((char *) data, (char *) "", 1);
				}
			}
		}
	}
	free(buffer);
	return rc;
}

RC RM::updateTuple(const string tableName, const void *data, const RID &rid) {
	RC rc;
	unsigned int page_id, slot_id, new_page_id, new_slot_id;
	RID new_RID;
	PF_FileHandle filehandle;
	void *buffer = malloc(4096);
	unsigned int recordlength = 0, oldrecordlength, oldrecordOffset; //have to find using system catalog
	unsigned int num_slots, freespaceoffset;
	int holelength;

	rc = getRecordLength(tableName, data, recordlength);

	rc = pf->OpenFile(tableName.c_str(), filehandle);
	if (rc == -1)
		return -1;

	page_id = rid.pageNum;
	slot_id = rid.slotNum;

	if (page_id >= filehandle.GetNumberOfPages())
		return -1;
	rc = filehandle.ReadPage(page_id, buffer);
	if (rc == -1) {
		pf->CloseFile(filehandle);
		return -1;
	}

	oldrecordlength = *(unsigned int *) ((char *) buffer + 4096
			- (4 + 8 * slot_id));
	if (oldrecordlength == 5000)
		return -1;
	oldrecordlength--;
	oldrecordOffset = *(unsigned int *) ((char *) buffer + 4096
			- (8 + 8 * slot_id));
	if (*(char *) ((char *) buffer + oldrecordOffset) == 1) {
		oldrecordOffset++;
		page_id = *(unsigned int *) ((char *) buffer + oldrecordOffset);
		oldrecordOffset += 4;
		slot_id = *(unsigned int *) ((char *) buffer + oldrecordOffset);
		new_RID.pageNum = page_id;
		new_RID.slotNum = slot_id;
		updateTuple(tableName, data, new_RID);
	} else {
		if (recordlength == oldrecordlength) {
			//oldrecordOffset = *(unsigned int *) ((char *) buffer + 4096	- (8 + 8 * slot_id));
			memcpy((char *) buffer + oldrecordOffset + 1, data, recordlength);
			rc = filehandle.WritePage(page_id, buffer);
		} else {
			freespaceoffset = *((unsigned int *) ((char *) buffer + 4096 - 4));
			num_slots = *((unsigned int *) ((char *) buffer + 4096 - 8));
			holelength = 4096 - 8 - (num_slots * 8) - freespaceoffset;
			if ((int) recordlength < holelength) {
				*((char *) buffer + freespaceoffset) = 0;
				memcpy((char *) buffer + freespaceoffset + 1, data,
						recordlength);
				*(unsigned int *) ((char *) buffer + 4096 - 4) = recordlength
						+ 1 + freespaceoffset;
				*(unsigned int *) ((char *) buffer + 4096 - (4 + 8 * slot_id)) =
						recordlength + 1;
				*(unsigned int *) ((char *) buffer + 4096 - (8 + 8 * slot_id)) =
						freespaceoffset;
				rc = filehandle.WritePage(page_id, buffer);
				if (rc == -1) {
					pf->CloseFile(filehandle);
					return -1;
				}
			} else {
				insertTuple(tableName, data, new_RID);
				new_page_id = new_RID.pageNum;
				new_slot_id = new_RID.slotNum;
				oldrecordOffset = *(unsigned int *) ((char *) buffer + 4096
						- (8 + 8 * slot_id));
				*(char *) ((char *) buffer + oldrecordOffset) = 1;
				oldrecordOffset++;
				*(unsigned int *) ((char *) buffer + oldrecordOffset) =
						new_page_id;
				oldrecordOffset += 4;
				*(unsigned int *) ((char *) buffer + oldrecordOffset) =
						new_slot_id;
				rc = filehandle.WritePage(page_id, buffer);
				if (rc == -1) {
					pf->CloseFile(filehandle);
					return -1;
				}
			}
		}
	}
	rc = pf->CloseFile(filehandle);
	free(buffer);
	return rc;
}

int RM::gettableID(string tableName) {
	PF_FileHandle filehandle;
	RC rc;
	RID rid;
	char tablename[4096];
	int tableid, tablelength;
	void *buffer = malloc(4096);
	pf->OpenFile("TableInfo", filehandle);
	int num_page = filehandle.GetNumberOfPages();
	for (int i = 0; i < num_page; i++) {
		rc = filehandle.ReadPage(i, buffer);
		if (rc == -1)
			break;
		int num_slots = *((unsigned int *) ((char *) buffer + 4096 - 8));
		for (int j = 1; j <= num_slots; j++) {
			rid.pageNum = i;
			rid.slotNum = j;
			void *data = malloc(4096);
			rc = readTuple("TableInfo", rid, data);
			if (rc != -1) {
				tableid = (*(unsigned int*) ((char*) data));

				tablelength = (*(unsigned int*) ((char*) data + 4));
				memcpy(tablename, (char *) data + 8, tablelength);
				free(data);
				if (strcmp(tablename, tableName.c_str()) == 0)
					return tableid;
			}
		}
	}
	free(buffer);
	return 0;
}

RC RM::addAttribute(const string tableName, const Attribute attr) {
	int table_id = gettableID(tableName);
	table_cache_item table_info;
	table_info.tablename = tableName;

	void *buffer = malloc(4096);
	memcpy(buffer, &table_id, 4);

	int name_length = (attr.name).length();
	memcpy((char *) buffer + 4, &name_length, 4);

	string column_name = attr.name;
	table_info.colname = attr.name;
	memcpy((char *) buffer + 8, column_name.c_str(), column_name.length() + 1);

	int column_type = attr.type;
	table_info.coltype = attr.type;
	memcpy((char *) buffer + 9 + column_name.length(), &column_type, 4);

	int column_length = attr.length;
	table_info.collength = attr.length;
	memcpy((char *) buffer + 13 + column_name.length(), &column_length, 4);
	cout << table_id << "--" << name_length << "--" << column_name << "--"
			<< column_type << "--" << column_length;
	RID rid_c;
	RC rc = insertTuple("ColumnInfo", buffer, rid_c);
	table_cache.push_back(table_info);
	free(buffer);
	return rc;
}

RC RM::scan(const string tableName, const string conditionAttribute,
		const CompOp compOp, const void *value,
		const vector<string> &attributeNames,
		RM_ScanIterator &rm_ScanIterator) {
	RC rc;
	//PF_FileHandle filehandle;
	rc = pf->OpenFile(tableName.c_str(), rm_ScanIterator.filehandle);
	if (rc == -1)
		return -1;
	rm_ScanIterator.tablename = tableName;
	cout << rm_ScanIterator.tablename << endl;
	rm_ScanIterator.attributenames = attributeNames;
	for (unsigned i = 0; i < rm_ScanIterator.attributenames.size(); i++)
		cout << rm_ScanIterator.attributenames[i] << endl;
	rm_ScanIterator.value = (void*) value;
	rm_ScanIterator.compop = compOp;
	cout << rm_ScanIterator.compop << endl;
	rm_ScanIterator.conditionattribute = conditionAttribute;
	cout << rm_ScanIterator.conditionattribute << endl;
	//rm_ScanIterator.filehandle = filehandle;
	rm_ScanIterator.currentrid.pageNum = 0;
	rm_ScanIterator.currentrid.slotNum = 1;
	vector<Attribute> attributes;
	rc = getAttributes(tableName, attributes);

	unsigned count = 0;
	for (vector<Attribute>::const_iterator attr = attributes.begin();
			attr != attributes.end(); ++attr) {
		cout << attr->name << endl;
		cout << conditionAttribute << endl;
		if (strcmp(attr->name.c_str(), conditionAttribute.c_str()) == 0) {
			rm_ScanIterator.attributetype = attr->type;
			cout << rm_ScanIterator.attributetype << endl;
		} else
			count++;
	}
	if (count == attributes.size()) {
		rm_ScanIterator.attributetype = (AttrType) 5;
		cout << rm_ScanIterator.attributetype << endl;
	}
	return rc;
}

RC RM::dropAttribute(const string tableName, const string attributeName) {
	RM_ScanIterator iter;
	RC rc;
	RID rid;
	void *data = malloc(4096);
	int table_id = gettableID(tableName);
	string attr = "Tableid";
	vector<string> attributes;
	attributes.push_back("Tableid");
	attributes.push_back(attr);
	rc = scan("ColumnInfo", "Columnname", EQ_OP, attributeName.c_str(),
			attributes, iter);

	if (rc == -1)
		return -1;
	while (iter.getNextTuple(rid, data) != RM_EOF) {
		if (table_id == *(int *) data)
			deleteTuple("ColumnInfo", rid);
	}
	iter.close();
	free(data);
	vector<table_cache_item>::size_type i = 0;
	while (i < table_cache.size()) {
		if (strcmp(tableName.c_str(), table_cache[i].tablename.c_str()) == 0) {
			table_cache.erase(table_cache.begin() + i);
		} else {
			++i;
		}
	}
	return 0;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
	RM *rm = RM::Instance();
	RC rc;
	void *page_buffer = malloc(4096);
	void *rec_attr_val = malloc(4096);
	int num_page = filehandle.GetNumberOfPages();
	float diff = 0;
	int found = 0;
	if (currentrid.pageNum != 5000 && currentrid.slotNum != 5000) {
		for (int i = currentrid.pageNum; i < num_page; i++) {
			rc = filehandle.ReadPage(i, page_buffer);
			if (rc == -1)
				return -1;
			int num_slots =
					*((unsigned int *) ((char *) page_buffer + 4096 - 8));
			int tslotnum = 1;
			if ((unsigned) i == currentrid.pageNum)
				tslotnum = currentrid.slotNum;
			else
				tslotnum = 1;
			for (int j = tslotnum; j <= num_slots; j++) {
				if (found == 1) {
					currentrid.pageNum = i;
					currentrid.slotNum = j;
					return 0;
				}
				if (found == 0) {
					rid.pageNum = i;
					rid.slotNum = j;
					rc = rm->readAttribute(tablename, rid, conditionattribute,
							rec_attr_val);
					if (attributetype == 2) {
						void *strbuffer = malloc(4096);
						memcpy(strbuffer, rec_attr_val, 4);
						unsigned int strlength = *(unsigned int*) strbuffer;
						memcpy((char *) strbuffer, (char *) rec_attr_val + 4,
								strlength);
						memcpy((char *) strbuffer + strlength, "\0", 1);
						void *strinput = malloc(4096);
						memcpy(strinput, value, 4);
						unsigned int input_length = *(unsigned int*) strinput;
						memcpy((char *) strinput, (char *) value + 4,
								input_length);
						memcpy((char *) strinput + input_length, "\0", 1);
						diff = strcmp((char*) strinput, (char*) strbuffer);
						free(strbuffer);
						free(strinput);
					}
					if (attributetype == 1) {
						void *realbuffer = malloc(4096);
						memcpy(realbuffer, rec_attr_val, 4);
						if (*(float *) realbuffer == *(float *) value)
							diff = 0;
						else
							diff = *(float *) realbuffer - *(float *) value;
						free(realbuffer);
					}
					if (attributetype == 0) {
						void *intbuffer = malloc(4096);
						memcpy(intbuffer, rec_attr_val, 4);
						diff = *(int *) intbuffer - *(int *) value;
						free(intbuffer);
					}
					switch (compop) {
					case EQ_OP:
						if (diff == 0)
							found = 1;
						break;
					case LT_OP:
						if (diff < 0)
							found = 1;
						break;
					case GT_OP:
						if (diff > 0)
							found = 1;
						break;
					case LE_OP:
						if (diff <= 0)
							found = 1;
						break;
					case GE_OP:
						if (diff >= 0)
							found = 1;
						break;
					case NE_OP:
						if (diff != 0)
							found = 1;
						break;
					case NO_OP:
						found = 1;
						break;

					}
					if (found == 1) {
						void *record = malloc(4096);
						rc = rm->readTuple(tablename, rid, record);
						if (rc == -1) {
							found = 0;
							continue;
						}
						vector<Attribute> table_attr;
						rc = rm->getAttributes(tablename, table_attr);
						int record_offset = 0;
						int projection_offset = 0;
						for (vector<string>::const_iterator projection_attr =
								attributenames.begin();
								projection_attr != attributenames.end();
								++projection_attr) {
							record_offset = 0;
							for (vector<Attribute>::const_iterator table_attr_iter =
									table_attr.begin();
									table_attr_iter != table_attr.end();
									++table_attr_iter) {
								if (table_attr_iter->type != 2) {
									if (table_attr_iter->name
											== *projection_attr) {
										memcpy(
												(char *) data
														+ projection_offset,
												(char *) record + record_offset,
												4);
										projection_offset += 4;
										record_offset += 4;
									} else
										record_offset += 4;
								} else {
									int data_length =
											(*(unsigned int*) ((char*) record
													+ record_offset));
									if (table_attr_iter->name
											== *projection_attr) {
										memcpy(
												(char *) data
														+ projection_offset,
												(char *) record + record_offset,
												data_length + 4);
										projection_offset += data_length + 4;
										record_offset += data_length + 4;
									} else
										record_offset += data_length + 4;
								}
							}
						}
						free(record);
					}
				}
				if (i == num_page - 1 and j == num_slots) {
					currentrid.pageNum = 5000;
					currentrid.slotNum = 5000;
				}
			}
		}
	}
	if (found == 0)
		rc = RM_EOF;
	free(page_buffer);
	free(rec_attr_val);
	return rc;
}

//TODO reorganizepage
/*RC RM::reorganizePage(const string tableName, const unsigned pageNumber) {
 PF_FileHandle fHandle;
 unsigned int freespaceoffset, num_slots;

 unsigned int recordlenTo, recordoffsetTo, recordlenFrom, recordoffsetFrom;
 unsigned int lastmoved = 1;
 unsigned int lastmoved_offset = 0;

 void * buffer = malloc(4096);
 RC rc;

 rc = pf->OpenFile(tableName.c_str(), fHandle);
 if (rc == -1)
 return -1;

 rc = fHandle.ReadPage(pageNumber, buffer);
 if (rc == -1) {
 pf->CloseFile(fHandle);
 return -1;
 }
 freespaceoffset = *((unsigned int *) ((char *) buffer + 4096 - 4));
 num_slots = *((unsigned int *) ((char *) buffer + 4096 - 8));

 for (unsigned int i = 1; i <= num_slots; i++) {
 recordlenTo = *(unsigned int *) ((char *) buffer + 4096 - (4 + 8 * i));
 recordoffsetTo =
 *(unsigned int *) ((char *) buffer + 4096 - (8 + 8 * i));
 if ((recordlenTo == 5000) && (recordoffsetTo != 4097))
 {
 if (lastmoved_offset > recordoffsetTo) {
 if (lastmoved_offset > recordoffsetTo + recordlenTo)
 *(unsigned int *) ((char *) buffer + 4096 - (8 + 8 * i)) =
 4097;
 else
 recordoffsetTo = lastmoved_offset;
 } else {
 //find next valid slot
 unsigned int start = lastmoved > (i + 1) ? lastmoved : i;
 start++; //start searching from next slot
 for (unsigned int j = start; j <= nSlots; j++) {
 recordoffsetFrom = *(unsigned int *) ((char *) buffer + 4096
 - (8 + 8 * j));
 recordlenFrom = *(unsigned int *) ((char *) buffer + 4096
 - (4 + 8 * j));
 if (recordlenFrom != 0) {
 memcpy((char *) buffer + recordoffsetTo,
 (char *) buffer + recordoffsetFrom,
 recordlenFrom);
 *(unsigned int *) ((char *) buffer + 4096 - (8 + 8 * j)) =
 recordoffsetTo; //update new offset

 *(unsigned int *) ((char *) buffer + 4096 - (8 + 8 * i)) =
 4097; //mark as collected

 lastmoved = j;
 lastmoved_offset = recordoffsetTo + recordlenFrom + 1; //points to next free byte/slot
 break; //moved 1 block exit innerloop
 }
 }
 }
 } else if ((*(unsigned int *) ((char *) buffer + recordoffsetTo) == 1)
 && (recordlenTo != 9)) //this is a non collected tombstone.
 {
 recordoffsetTo += 9; //tombstone needs only 9 bytes
 //find next valid slot
 unsigned int start = lastmoved > (i + 1) ? lastmoved : i;
 start++; //start searching from next slot
 for (unsigned int j = start; j <= nSlots; j++) {
 recordoffsetFrom = *(unsigned int *) ((char *) buffer + 4096
 - (8 + 8 * j));
 recordlenFrom = *(unsigned int *) ((char *) buffer + 4096
 - (4 + 8 * j));
 if (recordlenFrom != 0) {
 memcpy((char *) buffer + recordoffsetTo,
 (char *) buffer + recordoffsetFrom, recordlenFrom);
 *(unsigned int *) ((char *) buffer + 4096 - (8 + 8 * j)) =
 recordoffsetTo; //update new offset

 *(unsigned int *) ((char *) buffer + 4096 - (4 + 8 * i)) =
 9; //mark tombstone as collected

 lastmoved = j;
 break; //moved 1 block exit innerloop
 }
 }

 }
 }
 //this implementation lets holes in the slots of footer of page.
 retCode = pf->CloseFile(fHandle);
 free(buffer);
 return retCode;
 }*/

RC RM::reorganizeTable(const string tableName) {
	RC rc;
	void *record = malloc(4096);
	RM_ScanIterator iter;
	RID rid;
	vector<Attribute> attributes;
	vector<string> attr_name;
	rc = getAttributes(tableName, attributes);
	if (rc == -1)
		return -1;
	for (vector<Attribute>::iterator attr_iter = attributes.begin();
			attr_iter != attributes.end(); ++attr_iter) {
		attr_name.push_back(attr_iter->name);
	}
	rc = scan(tableName, "", NO_OP, NULL, attr_name, iter);
	if (rc == -1)
		return -1;
	createTable(tableName + "_temp", attributes);
	while (iter.getNextTuple(rid, record) != RM_EOF)
		insertTuple(tableName + "_temp", record, rid);
	iter.close();
	deleteTuples(tableName);
	rc = scan(tableName + "_temp", "", NO_OP, NULL, attr_name, iter);
	if (rc == -1)
		return -1;
	while (iter.getNextTuple(rid, record) != RM_EOF)
		insertTuple(tableName, record, rid);
	iter.close();
	deleteTable(tableName + "_temp");
	free(record);
	return rc;
}
