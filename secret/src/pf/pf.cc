#include "pf.h"
#include <fstream>
#include <iostream>
#include <stdlib.h>
#include <string.h>
using namespace std;

PF_Manager* PF_Manager::_pf_manager=0;
CacheRepPolicy* PF_Manager::_lru_cache=0;

PF_Manager* PF_Manager::Instance(int cacheNumPages)
{
	if(!_pf_manager)
		_pf_manager=new PF_Manager(cacheNumPages);
	return _pf_manager;
}

PF_Manager::PF_Manager(int cacheNumPages)
{
	if(!_lru_cache)
		_lru_cache=new CacheRepPolicy(cacheNumPages);
}

PF_Manager::~PF_Manager()
{
}

CacheRepPolicy::CacheRepPolicy(size_t size)
{
	list=new CacheBlock[size];
	for(int i=0;i<(int)size;i++)
		empty_blocks.push_back(list+i);
	begin=new CacheBlock;
	end=new CacheBlock;
	begin->prev=NULL;
	begin->next=end;
	end->next=NULL;
	end->prev=begin;
}

CacheRepPolicy::~CacheRepPolicy()
{
	delete begin;
	delete end;
	delete []list;
}

void CacheRepPolicy::set(string key,void* data,int rw)
{
	CacheBlock *block=h_map[key];
	if(block)
	{
		remove_end(block);
		block->data = data;
		if(rw==1)
			block->d_bit=true;
		else
			block->d_bit=false;
		to_front(block);
	}
	else
	{
		if(empty_blocks.empty())
		{
			//TODO
			block=end->prev;
			if(block->d_bit==true)
			{
				//TODO new file handle for writing
				/*file.seekp(block->key*PF_PAGE_SIZE,ios::beg);
				file.write((char*)data,PF_PAGE_SIZE);*/
			}
			remove_end(block);
			h_map.erase(block->key);
			block->data=data;
			if(rw==1)
				block->d_bit=true;
			else
				block->d_bit=false;
			block->key=key;
			to_front(block);
		}
		else{
			block=empty_blocks.back();
			empty_blocks.pop_back();
			block->key=key;
			block->data=data;
			if(rw==1)
				block->d_bit=true;
			else
				block->d_bit=false;
			h_map[key]=block;
			to_front(block);
		}
	}
}

void* CacheRepPolicy::get(string key)
{
	CacheBlock *block=h_map[key];
	if(block)
	{
		remove_end(block);
		to_front(block);
		return block->data;
	}
	else
		return NULL;
}

void CacheRepPolicy::remove_end(CacheBlock *block)
{
	block->prev->next=block->next;
	block->next->prev=block->prev;
}

void CacheRepPolicy::to_front(CacheBlock *block)
{
	block->next=begin->next;
	block->prev=begin;
	begin->next=block;
	block->next->prev=block;
}

RC PF_Manager::CreateFile(const char *fileName)
{
	fstream file_str;
	file_str.open(fileName,ios::in|ios::binary);
	if(file_str.is_open())
	{
		file_str.close();
		return -1;
	}
	file_str.open(fileName,ios::out|ios::binary);
	file_str.close();
	return 0;
}

RC PF_Manager::DestroyFile(const char *fileName)
{
	//TODO empty cache blocks of that file before deleting
	if(remove(fileName)!=0)
		return -1;
	return 0;
}

RC PF_Manager::OpenFile(const char *fileName, PF_FileHandle &fileHandle)
{
	if(!fileHandle.file.is_open())
	{
		fileHandle.file.open(fileName,ios::in|ios::out|ios::binary);
		if(fileHandle.file.is_open())
		{
			strcpy(fileHandle.filename,fileName);
			return 0;
		}
	}
	return -1;
}

RC PF_Manager::CloseFile(PF_FileHandle &fileHandle)
{
	//TODO write cache blocks back to the file before deleting
	if(fileHandle.file.is_open())
	{
		fileHandle.file.close();
		if(fileHandle.file.is_open())
			return -1;
		fileHandle.filename=NULL;
		return 0;
	}
	return -1;
}

PF_FileHandle::PF_FileHandle()
{
}

PF_FileHandle::~PF_FileHandle()
{
	if(file.is_open())
		file.close();
}

RC PF_FileHandle::ReadPage(PageNum pageNum, void *data)
{
	if(pageNum<0 || !file.is_open() || GetNumberOfPages()<pageNum)
		return -1;
	else
	{
		char *key=strcat(filename,(char*)pageNum);
		data=PF_Manager::_lru_cache->get(key);
		if(data!=NULL)
			return 0;
		else
		{
			file.seekg(pageNum*PF_PAGE_SIZE,ios::beg);
			file.read((char*)data,PF_PAGE_SIZE);
			PF_Manager::_lru_cache->set(key,data,0);
			f_pages.insert((int)pageNum);
			return 0;
		}
	}
}

RC PF_FileHandle::WritePage(PageNum pageNum, const void *data)
{
	if(pageNum<0 || !file.is_open() || GetNumberOfPages()<pageNum)
		return -1;
	else
	{
		char *key=strcat(filename,(char*)pageNum);
		PF_Manager::_lru_cache->set(key,(char *)data,1);
		f_pages.insert((int)pageNum);
		return 0;

		/*void *t_data=PF_Manager::_lru_cache->get(key);
		if(t_data!=NULL)
		{
			PF_Manager::_lru_cache->set(key,(char *)data);
			return 0;
		}
		else //TODO use this code while replacing a block
		{
			file.seekp(pageNum*PF_PAGE_SIZE,ios::beg);
			file.write((char*)data,PF_PAGE_SIZE);
			return 0;
		}*/
	}
}

RC PF_FileHandle::AppendPage(const void *data)
{
	if(!file.is_open())
		return -1;
	else
	{
		//file.seekp(0,ios::end);
		//file.write((char*)data,PF_PAGE_SIZE);
		char *key=strcat(filename,(char*)(GetNumberOfPages()+1));
		PF_Manager::_lru_cache->set(key,(char*)data,1);
		f_pages.insert((int)(GetNumberOfPages()+1));
		return 0;
	}
}

unsigned PF_FileHandle::GetNumberOfPages()
{
	if(!file.is_open())
		return 0;
	file.seekg(0,ios::end);
	//TODO get max from cache pages
	return (file.tellg()/PF_PAGE_SIZE);
}
