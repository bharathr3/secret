#include <fstream>
#include <iostream>
#include <cassert>
#include <string.h>

#include "rm.h"

using namespace std;

void rmTest()
{
	remove("Tables");
	remove("Columns");
	remove("TableInfo");
	remove("ColumnInfo");
	RM *rm = RM::Instance();
	// write your own testing cases here
}

int main()
{
	cout<<"Begin"<<endl;
	rmTest();
	//other tests go here
	cout<<"End"<<endl;
}
