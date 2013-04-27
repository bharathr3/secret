#include <iostream>
#include <fstream>
#include <string>
#include <cassert>
#include <stdio.h>
#include <cstring>
#include <stdlib.h>
#include <sys/stat.h>

#include "pf.h"

using namespace std;

const int success = 0;

// Check if a file exists
bool FileExists(string fileName)
{
	struct stat stFileInfo;
	if(stat(fileName.c_str(), &stFileInfo) == 0)
		return true;
	else
		return false;
}

//Test Case 1 - File Creation
int PFTest_1(PF_Manager *pf)
{
	cout<<"****In PF Test Case 1****"<<endl;
	RC rc;
	string fileName="test";
	rc=pf->CreateFile(fileName.c_str());
	assert(rc==success);
	if(FileExists(fileName.c_str()))
	{
		cout<<"File "<<fileName<<" has been created."<<endl;
		return 0;
	}
	else
	{
		cout << "Failed to create file!" << endl;
		return -1;
	}
	rc = pf->CreateFile(fileName.c_str());
	assert(rc != success);
	return 0;
}

//Test Case 2 - File Recreation
int PFTest_2(PF_Manager *pf)
{
	cout<<"****In PF Test Case 2****"<<endl;
	RC rc;
	string fileName="test";
	rc=pf->CreateFile(fileName.c_str());
	assert(rc==success);
	if(FileExists(fileName.c_str()))
	{
		cout<<"File "<<fileName<<" has been created."<<endl;
	}
	else
	{
		cout << "Failed to create file!" << endl;
	}
	rc = pf->CreateFile(fileName.c_str());
	if(rc!=success)
		return 0;
	else
		return -1;
}

//Test Case 3
int PFTest_3(PF_Manager *pf)
{
	cout<<"****In PF Test Case 3****"<<endl;
	RC rc;
	string fileName = "test";

	// Open the file "test"
	PF_FileHandle fileHandle;
	rc = pf->OpenFile(fileName.c_str(), fileHandle);
	assert(rc == success);

	// Append the first page
	// Write ASCII characters from 32 to 125 (inclusive)
	void *data = malloc(PF_PAGE_SIZE);
	for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
	{
		*((char *)data+i) = i % 94 + 32;
	}
	rc = fileHandle.AppendPage(data);
	assert(rc == success);

	// Get the number of pages
	unsigned count = fileHandle.GetNumberOfPages();
	assert(count == (unsigned)1);

	// Update the first page
	// Write ASCII characters from 32 to 41 (inclusive)
	data = malloc(PF_PAGE_SIZE);
	for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
	{
		*((char *)data+i) = i % 10 + 32;
	}

	rc = fileHandle.WritePage(0, data);
	assert(rc == success);

	// Read the page
	void *buffer = malloc(PF_PAGE_SIZE);
	rc = fileHandle.ReadPage(0, buffer);
	assert(rc == success);

	// Check the integrity
	rc = memcmp(data, buffer, PF_PAGE_SIZE);
	assert(rc == success);

	// Close the file "test"
	rc = pf->CloseFile(fileHandle);
	assert(rc == success);

	free(data);
	free(buffer);

	// DestroyFile
	rc = pf->DestroyFile(fileName.c_str());
	assert(rc == success);

	if(!FileExists(fileName.c_str()))
	{
		cout << "File " << fileName << " has been destroyed." << endl;
		cout << "Test Case 3 Passed!" << endl << endl;
		return 0;
	}
	else
	{
		cout << "Failed to destroy file!" << endl;
		return -1;
	}
}

void pfTest()
{
	PF_Manager *pf = PF_Manager::Instance(10);

	remove("test");
	PFTest_1(pf);
	remove("test");
	PFTest_2(pf);
	PFTest_3(pf);
}

int main()
{
	cout<<"Begin"<<endl;
	pfTest();
	cout<<"End"<<endl;
	return 0;
}
