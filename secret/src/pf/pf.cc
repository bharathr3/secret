#include "pf.h"
#include <fstream>
#include <iostream>
#include <stdlib.h>
#include <string.h>
using namespace std;

PF_Manager* PF_Manager::_pf_manager = 0;
Cache_Policy* PF_Manager::_fifo_cache = 0;

Cache_Policy::Cache_Policy(int num_pages) {
	num_blocks = num_pages;
	CacheBlock *item = new CacheBlock[num_pages];
	for (int i = 0; i < num_pages; i++) {
		item[i].d_bit = false;
		item[i].fname = "";
		item[i].pg_num = -1;
		item[i].data = NULL;
		_fifo_cache_list.push_back(item + i);
	}
}

PF_Manager* PF_Manager::Instance(int cacheNumPages) {
	if (!_pf_manager)
		_pf_manager = new PF_Manager(cacheNumPages);
	return _pf_manager;
}

PF_Manager::PF_Manager(int cacheNumPages) {
	if (!_fifo_cache)
		_fifo_cache = new Cache_Policy(cacheNumPages);
}

PF_Manager::~PF_Manager() {
	delete _pf_manager;
}

RC PF_Manager::CreateFile(const char *fileName) {
	fstream file_str;
	file_str.open(fileName, ios::in | ios::binary);
	if (file_str.is_open()) {
		file_str.close();
		return -1;
	}
	file_str.open(fileName, ios::out | ios::binary);
	file_str.close();
	return 0;
}

RC PF_Manager::DestroyFile(const char *fileName) {
	if (remove(fileName) != 0)
		return -1;
	for (int i = 0; i < _fifo_cache->num_blocks; i++) {
		if (strcmp(_fifo_cache->_fifo_cache_list.at(i)->fname.c_str(), fileName)
				== 0) {
			_fifo_cache->_fifo_cache_list.at(i)->d_bit = false;
			_fifo_cache->_fifo_cache_list.at(i)->fname = "";
			_fifo_cache->_fifo_cache_list.at(i)->pg_num = -1;
			_fifo_cache->_fifo_cache_list.at(i)->data = NULL;
		}
	}
	return 0;
}

RC PF_Manager::OpenFile(const char *fileName, PF_FileHandle &fileHandle) {
	if (!fileHandle.file.is_open()) {
		fileHandle.file.open(fileName, ios::in | ios::out | ios::binary);
		if (fileHandle.file.is_open()) {
			fileHandle.filename = fileName;
			return 0;
		}
	}
	return -1;
}

RC PF_Manager::CloseFile(PF_FileHandle &fileHandle) {
	if (fileHandle.file.is_open()) {
		for (int i = 0; i < _fifo_cache->num_blocks; i++) {
			if ((strcmp(_fifo_cache->_fifo_cache_list.at(i)->fname.c_str(),
					fileHandle.filename.c_str()) == 0)
					&& (_fifo_cache->_fifo_cache_list.at(i)->d_bit == true)) {
				_fifo_cache->_fifo_cache_list.at(i)->d_bit = false;
				_fifo_cache->_fifo_cache_list.at(i)->fname = "";
				_fifo_cache->_fifo_cache_list.at(i)->pg_num = -1;
				_fifo_cache->_fifo_cache_list.at(i)->data = NULL;
			}
		}
		fileHandle.filename = "";
		fileHandle.file.close();
		if (fileHandle.file.is_open())
			return -1;
		return 0;
	}
	return -1;
}

PF_FileHandle::PF_FileHandle() {
}

PF_FileHandle::~PF_FileHandle() {
	if (file.is_open())
		file.close();
}

RC PF_FileHandle::ReadPage(PageNum pageNum, void *data) {
	int i = 0;
	for (i = 0; i < PF_Manager::_fifo_cache->num_blocks; i++) {
		if ((strcmp(
				PF_Manager::_fifo_cache->_fifo_cache_list.at(i)->fname.c_str(),
				filename.c_str()) == 0)
				&& (PF_Manager::_fifo_cache->_fifo_cache_list.at(i)->pg_num
						== (int) pageNum)) {
			PF_Manager::_fifo_cache->_fifo_cache_list.at(i)->d_bit = false;
			PF_Manager::_fifo_cache->_fifo_cache_list.at(i)->fname = "";
			PF_Manager::_fifo_cache->_fifo_cache_list.at(i)->pg_num = -1;
			PF_Manager::_fifo_cache->_fifo_cache_list.at(i)->data = NULL;
		}
	}
	PF_Manager::_fifo_cache->_fifo_cache_list.at(
			PF_Manager::_fifo_cache->cache_counter)->fname = filename;
	PF_Manager::_fifo_cache->_fifo_cache_list.at(
			PF_Manager::_fifo_cache->cache_counter)->pg_num = pageNum;
	PF_Manager::_fifo_cache->_fifo_cache_list.at(
			PF_Manager::_fifo_cache->cache_counter)->data = (void*) data;
	PF_Manager::_fifo_cache->_fifo_cache_list.at(
			PF_Manager::_fifo_cache->cache_counter)->d_bit = false;

	PF_Manager::_fifo_cache->cache_counter++;
	if (PF_Manager::_fifo_cache->cache_counter
			>= PF_Manager::_fifo_cache->num_blocks)
		PF_Manager::_fifo_cache->cache_counter =
				PF_Manager::_fifo_cache->cache_counter
						% PF_Manager::_fifo_cache->num_blocks;

	if (pageNum < 0 || !file.is_open() || GetNumberOfPages() < pageNum)
		return -1;
	else {
		file.seekg(pageNum * PF_PAGE_SIZE, ios::beg);
		file.read((char*) data, PF_PAGE_SIZE);
		return 0;
	}
}

RC PF_FileHandle::WritePage(PageNum pageNum, const void *data) {
	for (int i = 0; i < PF_Manager::_fifo_cache->num_blocks; i++) {
		if ((strcmp(
				PF_Manager::_fifo_cache->_fifo_cache_list.at(i)->fname.c_str(),
				filename.c_str()) == 0)
				&& (PF_Manager::_fifo_cache->_fifo_cache_list.at(i)->pg_num
						== (int) pageNum)) {
			PF_Manager::_fifo_cache->_fifo_cache_list.at(i)->fname = filename;
			PF_Manager::_fifo_cache->_fifo_cache_list.at(i)->pg_num = pageNum;
			PF_Manager::_fifo_cache->_fifo_cache_list.at(i)->d_bit = true;
			PF_Manager::_fifo_cache->_fifo_cache_list.at(i)->data =
					(void*) data;
		}
	}
	PF_Manager::_fifo_cache->_fifo_cache_list.at(
			PF_Manager::_fifo_cache->cache_counter)->fname = filename;
	PF_Manager::_fifo_cache->_fifo_cache_list.at(
			PF_Manager::_fifo_cache->cache_counter)->pg_num = pageNum;
	PF_Manager::_fifo_cache->_fifo_cache_list.at(
			PF_Manager::_fifo_cache->cache_counter)->data = (void*) data;
	PF_Manager::_fifo_cache->_fifo_cache_list.at(
			PF_Manager::_fifo_cache->cache_counter)->d_bit = true;

	PF_Manager::_fifo_cache->cache_counter++;
	if (PF_Manager::_fifo_cache->cache_counter
			>= PF_Manager::_fifo_cache->num_blocks)
		PF_Manager::_fifo_cache->cache_counter =
				PF_Manager::_fifo_cache->cache_counter
						% PF_Manager::_fifo_cache->num_blocks;

	if (pageNum < 0 || !file.is_open() || GetNumberOfPages() < pageNum)
		return -1;
	else {
		file.seekp(pageNum * PF_PAGE_SIZE, ios::beg);
		file.write((char*) data, PF_PAGE_SIZE);
		return 0;
	}
}

RC PF_FileHandle::AppendPage(const void *data) {
	int pg_num = GetNumberOfPages();
	if (PF_Manager::_fifo_cache->_fifo_cache_list.at(
			PF_Manager::_fifo_cache->cache_counter)->d_bit == true) {
		PF_Manager::_fifo_cache->_fifo_cache_list.at(
				PF_Manager::_fifo_cache->cache_counter)->fname = filename;
		PF_Manager::_fifo_cache->_fifo_cache_list.at(
				PF_Manager::_fifo_cache->cache_counter)->pg_num = pg_num;
		PF_Manager::_fifo_cache->_fifo_cache_list.at(
				PF_Manager::_fifo_cache->cache_counter)->d_bit = true;
		PF_Manager::_fifo_cache->_fifo_cache_list.at(
				PF_Manager::_fifo_cache->cache_counter)->data = (void*) data;
	}
	PF_Manager::_fifo_cache->_fifo_cache_list.at(
			PF_Manager::_fifo_cache->cache_counter)->fname = filename;
	PF_Manager::_fifo_cache->_fifo_cache_list.at(
			PF_Manager::_fifo_cache->cache_counter)->pg_num = pg_num;
	PF_Manager::_fifo_cache->_fifo_cache_list.at(
			PF_Manager::_fifo_cache->cache_counter)->data = (void*) data;
	PF_Manager::_fifo_cache->_fifo_cache_list.at(
			PF_Manager::_fifo_cache->cache_counter)->d_bit = true;

	PF_Manager::_fifo_cache->cache_counter++;
	if (PF_Manager::_fifo_cache->cache_counter
			>= PF_Manager::_fifo_cache->num_blocks)
		PF_Manager::_fifo_cache->cache_counter =
				PF_Manager::_fifo_cache->cache_counter
						% PF_Manager::_fifo_cache->num_blocks;

	if (!file.is_open())
		return -1;
	else {
		file.seekp(0, ios::end);
		file.write((char*) data, PF_PAGE_SIZE);
		return 0;
	}
}

unsigned PF_FileHandle::GetNumberOfPages() {
	int max = -1;
	for (int i = 0; i < PF_Manager::_fifo_cache->num_blocks; i++) {
		if (strcmp(
				PF_Manager::_fifo_cache->_fifo_cache_list.at(i)->fname.c_str(),
				filename.c_str()) == 0) {
			max =
					(max
							> PF_Manager::_fifo_cache->_fifo_cache_list.at(i)->pg_num) ?
							max :
							PF_Manager::_fifo_cache->_fifo_cache_list.at(i)->pg_num;
		}
	}
	if (!file.is_open())
		return 0;
	file.seekg(0, ios::end);
	if (file.tellg() == 0)
		return 0;
	int num_pages = file.tellg() / PF_PAGE_SIZE;
	return (num_pages);
}
