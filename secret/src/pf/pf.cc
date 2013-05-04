#include "pf.h"
#include <fstream>
#include <iostream>
#include <stdlib.h>
#include <string.h>
using namespace std;

PF_Manager* PF_Manager::_pf_manager = 0;
CacheRepPolicy* PF_Manager::_lru_cache = 0;

PF_Manager* PF_Manager::Instance(int cacheNumPages) {
	if (!_pf_manager)
		_pf_manager = new PF_Manager(cacheNumPages);
	return _pf_manager;
}

PF_Manager::PF_Manager(int cacheNumPages) {
	if (!_lru_cache)
		_lru_cache = new CacheRepPolicy(cacheNumPages);
}

PF_Manager::~PF_Manager() {
	delete _pf_manager;
	delete _lru_cache;
}

CacheRepPolicy::CacheRepPolicy(size_t size) {
	list = new CacheBlock[size];
	for (int i = 0; i < (int) size; i++)
		empty_blocks.push_back(list + i);
	begin = new CacheBlock;
	end = new CacheBlock;
	begin->prev = NULL;
	begin->next = end;
	end->next = NULL;
	end->prev = begin;
}

CacheRepPolicy::~CacheRepPolicy() {
	delete begin;
	delete end;
	delete[] list;
}

void CacheRepPolicy::set(block_info key, void* data, int rw) {
	CacheBlock *block = h_map[key];
	if (block) {
		remove_end(block);
		memcpy(&(block->data), &data, sizeof(data));
		if (rw == 1)
			block->d_bit = true;
		else
			block->d_bit = false;
		to_front(block);
	} else {
		if (empty_blocks.empty()) {
			block = end->prev;
			if (block->d_bit == true) {
				PF_FileHandle newHandle;
				newHandle.file.open(block->block.fname,
						ios::in | ios::out | ios::binary);
				if (newHandle.file.is_open()) {
					newHandle.file.seekp(block->block.pg_num * PF_PAGE_SIZE,
							ios::beg);
					newHandle.file.write((char*) block->data, PF_PAGE_SIZE);
				}
				newHandle.file.close();
			}
			int *page_arr;
			int count = 0;
			pair<multimap<char*, int>::iterator,
			::multimap<char*, int>::iterator> ret;
			ret = PF_Manager::_lru_cache->f_pages.equal_range(
					(char*) block->block.fname);
			for (multimap<char*, int>::iterator it = ret.first;
					it != ret.second; ++it)
				count++;
			page_arr = new int[count];
			count = 0;
			for (multimap<char*, int>::iterator it = ret.first;
					it != ret.second; ++it) {
				page_arr[count] = (*it).second;
				count++;
			}
			PF_Manager::_lru_cache->f_pages.erase((char*) block->block.fname);
			for (int i = 0; i < count; i++) {
				if (page_arr[count] != block->block.pg_num)
					PF_Manager::_lru_cache->f_pages.insert(
							pair<char*, int>(block->block.fname,
									page_arr[count]));
			}
			delete[] page_arr;
			remove_end(block);
			h_map.erase(block->block);
			memcpy(&(block->data), &data, sizeof(data));
			if (rw == 1)
				block->d_bit = true;
			else
				block->d_bit = false;
			block->block.fname = key.fname;
			block->block.pg_num = key.pg_num;
			h_map[key] = block;
			to_front(block);
		} else {
			block = empty_blocks.back();
			empty_blocks.pop_back();
			block->block.fname = key.fname;
			block->block.pg_num = key.pg_num;
			block->data = (void*) malloc(sizeof(data));
			memcpy(&(block->data), &data, sizeof(data));
			if (rw == 1)
				block->d_bit = true;
			else
				block->d_bit = false;
			h_map[key] = block;
			to_front(block);
		}
	}
}

CacheBlock* CacheRepPolicy::get(block_info key) {
	CacheBlock *block = h_map[key];
	if (block) {
		remove_end(block);
		to_front(block);
		return block;
	} else
		return NULL;
}

void CacheRepPolicy::remove_end(CacheBlock *block) {
	block->prev->next = block->next;
	block->next->prev = block->prev;
}

void CacheRepPolicy::to_front(CacheBlock *block) {
	block->next = begin->next;
	block->prev = begin;
	begin->next = block;
	block->next->prev = block;
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
	block_info temp;
	temp.fname = (char*) fileName;
	pair<multimap<char*, int>::iterator, ::multimap<char*, int>::iterator> ret;
	ret = _lru_cache->f_pages.equal_range((char*) fileName);
	for (multimap<char*, int>::iterator it = ret.first; it != ret.second;
			++it) {
		temp.pg_num = (*it).second;
		CacheBlock *data = PF_Manager::_lru_cache->get(temp);
		_lru_cache->empty_blocks.push_back(data);
	}
	_lru_cache->f_pages.erase((char*) fileName);
	if (remove(fileName) != 0)
		return -1;
	return 0;
}

RC PF_Manager::OpenFile(const char *fileName, PF_FileHandle &fileHandle) {
	if (!fileHandle.file.is_open()) {
		fileHandle.file.open(fileName, ios::in | ios::out | ios::binary);
		if (fileHandle.file.is_open()) {
			fileHandle.filename = (char*) malloc(sizeof(fileName));
			strcpy(fileHandle.filename, fileName);
			return 0;
		}
	}
	return -1;
}

RC PF_Manager::CloseFile(PF_FileHandle &fileHandle) {
	if (fileHandle.file.is_open()) {
		block_info temp;
		temp.fname = fileHandle.filename;
		pair<multimap<char*, int>::iterator, ::multimap<char*, int>::iterator> ret;
		ret = _lru_cache->f_pages.equal_range(fileHandle.filename);
		for (multimap<char*, int>::iterator it = ret.first; it != ret.second;
				++it) {
			temp.pg_num = (*it).second;
			CacheBlock *data = PF_Manager::_lru_cache->get(temp);
			if (data->d_bit == true) {
				fileHandle.file.seekp((*it).second * PF_PAGE_SIZE, ios::beg);
				fileHandle.file.write((char*) data, PF_PAGE_SIZE);
			}
			_lru_cache->empty_blocks.push_back(data);
		}
		_lru_cache->f_pages.erase((char*) fileHandle.filename);
		free(fileHandle.filename);
		fileHandle.file.close();
		if (fileHandle.file.is_open())
			return -1;
		fileHandle.filename = NULL;
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
	if (pageNum < 0 || !file.is_open() || GetNumberOfPages() < pageNum)
		return -1;
	else {
		block_info temp;
		temp.fname = filename;
		temp.pg_num = pageNum;
		CacheBlock* read_block = PF_Manager::_lru_cache->get(temp);
		memcpy(data, (read_block->data), 4096 * sizeof(unsigned));
		if (data != NULL)
			return 0;
		else {
			file.seekg(pageNum * PF_PAGE_SIZE, ios::beg);
			file.read((char*) data, PF_PAGE_SIZE);
			PF_Manager::_lru_cache->set(temp, data, 0);
			PF_Manager::_lru_cache->f_pages.insert(
					pair<char*, int>(filename, (int) pageNum));
			return 0;
		}
	}
}

RC PF_FileHandle::WritePage(PageNum pageNum, const void *data) {
	if (pageNum < 0 || !file.is_open() || GetNumberOfPages() < pageNum)
		return -1;
	else {
		block_info temp;
		temp.fname = filename;
		temp.pg_num = pageNum;
		PF_Manager::_lru_cache->set(temp, (char *) data, 1);
		PF_Manager::_lru_cache->f_pages.insert(
				pair<char*, int>(filename, (int) pageNum));
		return 0;
	}
}

RC PF_FileHandle::AppendPage(const void *data) {
	if (!file.is_open())
		return -1;
	else {
		block_info temp;
		temp.fname = filename;
		temp.pg_num = GetNumberOfPages();
		PF_Manager::_lru_cache->set(temp, (char*) data, 1);
		PF_Manager::_lru_cache->f_pages.insert(
				pair<char*, int>(filename, (int) (temp.pg_num)));
		return 0;
	}
}

unsigned PF_FileHandle::GetNumberOfPages() {
	int max = -1;
	if (!file.is_open())
		return 0;
	file.seekg(0, ios::end);
	pair<multimap<char*, int>::iterator, ::multimap<char*, int>::iterator> ret;
	ret = PF_Manager::_lru_cache->f_pages.equal_range(filename);
	for (multimap<char*, int>::iterator it = ret.first; it != ret.second;
			++it) {
		max = ((max > (*it).second) ? max : (*it).second);
	}
	unsigned file_page_count = file.tellg() / PF_PAGE_SIZE;
	if (file.tellg() == 0 and max == -1)
		return 0;
	max = max > ((int) file_page_count + 1) ? max : (file_page_count + 1);
	return ((unsigned int) max);
}
