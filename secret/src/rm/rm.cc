#include "rm.h"
#include <stdlib.h>
#include <stdio.h>
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
	createCatalog();
	num_tables=2;
}

RM::~RM() {
	delete _rm;
}

RC RM::insertTuple(const string tableName, const void *data, RID &rid, int tupsize)
{
	return 0;
}

RC RM::createTable(const string tableName, const vector<Attribute> &attrs)
{
	int offset=0;RC rc=0;RID rid;
	rc = pf->CreateFile(tableName.c_str());
	if(rc==-1)
		return rc;

	num_tables=num_tables+1;

	void *data = malloc(100);
	memcpy((char *)data+offset,&num_tables,sizeof(int));
	offset=offset+sizeof(int);

	int l = tableName.length();
	memcpy((char *)data+offset,&l,sizeof(int));
	offset=offset+sizeof(int);

	memcpy((char *)data+offset,tableName.c_str(),l);
	offset=offset+l;

	memcpy((char *)data+offset,&l,sizeof(int));
	offset=offset+sizeof(int);

	memcpy((char *)data+offset,tableName.c_str(),l);
	offset=offset+l;

	rc = insertTuple("TableInfo",data,rid,offset);

	free(data);
	data = malloc(100);

	for(int i=0;i<(int)attrs.size();i++)
	{
		offset=0;
		memcpy((char *)data+offset,&num_tables,sizeof(int));
		offset=offset+sizeof(int);

		l = (attrs.at(i).name).length();
		memcpy((char *)data+offset,&l,sizeof(int));
		offset=offset+sizeof(int);

		memcpy((char *)data+offset,attrs.at(i).name.c_str(),l);
		offset=offset+l;

		l=attrs.at(i).type;
		memcpy((char *)data+offset,&l,sizeof(int));
		offset=offset+sizeof(int);

		l=attrs.at(i).length;
		memcpy((char *)data+offset,&l,sizeof(unsigned));
		offset=offset+sizeof(unsigned);

		rc = insertTuple("Columns",data,rid,offset);
	}

	free(data);
	return rc;
}

RC RM::deleteTuples(const string tableName)
{
	PF_FileHandle fileHandle;
	RC rc;
	rc = pf->OpenFile(tableName.c_str(),fileHandle);
	if(rc == -1)
		return rc;
	void *data = malloc(PF_PAGE_SIZE);
	int free_space=0,nos=0;

	memcpy((char*)data+4092,&free_space,sizeof(int));
	memcpy((char*)data+4088,&nos,sizeof(int));

	int num_pages=fileHandle.GetNumberOfPages();

	for(int i=0;i<num_pages;i++)
		rc=fileHandle.WritePage(i,data);

	rc = pf->CloseFile(fileHandle);
	free(data);
	return rc;
}

void RM::createCatalog()
{
	int offset=0,t_id=1;
	RC rc;RID rid;
	rc = pf->CreateFile("TableInfo");
	if(rc == -1)
		return;
	rc = pf->CreateFile("ColumnInfo");
	if(rc == -1)
		return;

	void *data = malloc(100);
	memcpy((char*)data+offset,&t_id,sizeof(int));
	offset=offset+sizeof(int);

	char tab_name[]="TableInfo";
	int l = strlen(tab_name);
	cout<<l;

	memcpy((char *)data+offset,&l,sizeof(int));
	offset=offset+sizeof(int);

	memcpy((char *)data+offset,tab_name,l);
	offset=offset+l;

	memcpy((char *)data+offset,&l,sizeof(int));
	offset=offset+sizeof(int);

	memcpy((char *)data+offset,tab_name,l);
	offset=offset+l;

	rc = insertTuple("TableInfo",data,rid,offset);

	free(data);
	offset=0;
	t_id=2;
	strcpy(tab_name,"ColumnInfo");
	l=strlen(tab_name);
	cout<<l;
	data=malloc(100);

	memcpy((char *)data+offset,&t_id,sizeof(int));
	offset=offset+sizeof(int);

	memcpy((char *)data+offset,&l,sizeof(int));
	offset=offset+sizeof(int);

	memcpy((char *)data+offset,tab_name,l);
	offset=offset+l;

	memcpy((char *)data+offset,&l,sizeof(int));
	offset=offset+sizeof(int);

	memcpy((char *)data+offset,tab_name,l);
	offset=offset+l;

	rc = insertTuple("TableInfo",data,rid,offset);

	Attribute TableInfo_attr,ColumnInfo_attr;
	vector<Attribute> Attr_vector;

	TableInfo_attr.name="Table-Id";
	TableInfo_attr.type=TypeInt;
	TableInfo_attr.length=(AttrLength)5;
	Attr_vector.push_back(TableInfo_attr);

	TableInfo_attr.name="TableName";
	TableInfo_attr.type=TypeVarChar;
	TableInfo_attr.length=(AttrLength)20;
	Attr_vector.push_back(TableInfo_attr);

	TableInfo_attr.name="FileName";
	TableInfo_attr.type=TypeVarChar;
	TableInfo_attr.length=(AttrLength)20;
	Attr_vector.push_back(TableInfo_attr);

	t_id=1;
	for(int i=0;i<(int)Attr_vector.size();i++)
	{
		free(data);
		offset=0;
		data=malloc(100);

		memcpy((char*)data+offset,&t_id,sizeof(int));
		offset=offset+sizeof(int);

		l = (Attr_vector.at(i).name).length();
		memcpy((char *)data+offset,&l,sizeof(int));
		offset=offset+sizeof(int);

		memcpy((char *)data+offset,Attr_vector.at(i).name.c_str(),l);
		offset=offset+l;

		l=Attr_vector.at(i).type;
		memcpy((char *)data+offset,&l,sizeof(int));
		offset=offset+sizeof(int);

		l=Attr_vector.at(i).length;
		memcpy((char *)data+offset,&l,sizeof(int));
		offset=offset+sizeof(int);

		rc = insertTuple("ColumnInfo",data,rid,offset);
	}

	Attr_vector.clear();

	TableInfo_attr.name="Table-Id";
	TableInfo_attr.type=TypeInt;
	TableInfo_attr.length=(AttrLength)5;
	Attr_vector.push_back(TableInfo_attr);

	TableInfo_attr.name="ColumnName";
	TableInfo_attr.type=TypeVarChar;
	TableInfo_attr.length=(AttrLength)20;
	Attr_vector.push_back(TableInfo_attr);

	TableInfo_attr.name="ColumnType";
	TableInfo_attr.type=TypeVarChar;
	TableInfo_attr.length=(AttrLength)10;
	Attr_vector.push_back(TableInfo_attr);

	TableInfo_attr.name="ColumnLength";
	TableInfo_attr.type=TypeInt;
	TableInfo_attr.length=(AttrLength)5;
	Attr_vector.push_back(TableInfo_attr);

	t_id=2;
	for(int i=0;i<(int)Attr_vector.size();i++)
	{
		free(data);
		offset=0;
		data=malloc(100);

		memcpy((char*)data+offset,&t_id,sizeof(int));
		offset=offset+sizeof(int);

		l = (Attr_vector.at(i).name).length();
		memcpy((char *)data+offset,&l,sizeof(int));
		offset=offset+sizeof(int);

		memcpy((char *)data+offset,Attr_vector.at(i).name.c_str(),l);
		offset=offset+l;

		l=Attr_vector.at(i).type;
		memcpy((char *)data+offset,&l,sizeof(int));
		offset=offset+sizeof(int);

		l=Attr_vector.at(i).length;
		memcpy((char *)data+offset,&l,sizeof(int));
		offset=offset+sizeof(int);

		rc = insertTuple("ColumnInfo",data,rid,offset);
	}

	free(data);
}
