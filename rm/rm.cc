#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance() {
	if (!_rm)
		_rm = new RelationManager();

	return _rm;
}

RelationManager::RelationManager() {
	myrbfm = RecordBasedFileManager::instance();
	myindex_manager = IndexManager::instance();
	createTableRecordDescriptor(attr_table);
	createColumnRecordDescriptor(attr_column);
	createIndexRecordDescriptor(attr_index);

}

RelationManager::~RelationManager() {
	free(myrbfm);
}

RC RelationManager::createCatalog() {
	RC rc;
	FileHandle file_table, file_column,file_index;
	RID rid;
	rc = myrbfm->createFile("Tables");
	if (rc != 0) {
		return -1;
	}
	rc = myrbfm->openFile("Tables", file_table);
	if (rc != 0) {
		return -1;
	}
	rc = myrbfm->createFile("Columns");
	if (rc != 0) {
		return -1;
	}
	rc = myrbfm->openFile("Columns", file_column);
	if (rc != 0) {
		return -1;
	}
	rc = myrbfm->createFile("Index");
	if (rc != 0) {
		return -1;
	}
	rc = myrbfm->openFile("Index", file_index);
	if (rc != 0) {
	return -1;
	}
	rc = prepareTableRecord(1, "Tables", "Tables", attr_table, file_table, rid);
	if (rc != 0) {
		return -1;
	}

	rc = prepareTableRecord(2, "Columns", "Columns", attr_table, file_table,
			rid);
	if (rc != 0) {
		return -1;
	}
	rc = prepareTableRecord(3, "Index", "Index", attr_table, file_table,rid);
	if (rc != 0)
		return -1;

	rc = prepareColumnRecord(1, "table-id", TypeInt, 4, 1, attr_column,
			file_column, rid);
	if (rc != 0) {
		return -1;
	}
	rc = prepareColumnRecord(1, "table-name", TypeVarChar, 50, 2, attr_column,
			file_column, rid);
	if (rc != 0) {
		return -1;
	}
	rc = prepareColumnRecord(1, "file-name", TypeVarChar, 50, 3, attr_column,
			file_column, rid);
	if (rc != 0) {
		return -1;
	}
	rc = prepareColumnRecord(2, "table-id", TypeInt, 4, 1, attr_column,
			file_column, rid);
	if (rc != 0) {
		return -1;
	}
	rc = prepareColumnRecord(2, "column-name", TypeVarChar, 50, 2, attr_column,
			file_column, rid);
	if (rc != 0) {
		return -1;
	}
	rc = prepareColumnRecord(2, "column-type", TypeInt, 4, 3, attr_column,
			file_column, rid);
	if (rc != 0) {
		return -1;
	}
	rc = prepareColumnRecord(2, "column-length", TypeInt, 4, 4, attr_column,
			file_column, rid);
	if (rc != 0) {
		return -1;
	}
	rc = prepareColumnRecord(2, "column-position", TypeInt, 4, 5, attr_column,
			file_column, rid);
	if (rc != 0) {
		return -1;
	}

	rc = prepareColumnRecord(3, "index-id", TypeInt, 4, 1, attr_column, file_column, rid);
	if (rc != 0)
		return -1;
	rc = prepareColumnRecord(3, "table-id", TypeInt, 4, 2, attr_column, file_column, rid);
	if (rc != 0)
		return -1;
	rc = prepareColumnRecord(3, "table-name", TypeVarChar, 50, 3, attr_column, file_column, rid);
	if (rc != 0)
		return -1;
	rc = prepareColumnRecord(3, "attribute-name", TypeVarChar, 50, 4, attr_column, file_column, rid);
	if (rc != 0)
		return -1;
	rc = prepareColumnRecord(3, "attribute-type", TypeInt, 4, 5, attr_column, file_column, rid);
	if (rc != 0)
		return -1;
	rc = prepareColumnRecord(3, "file-name", TypeVarChar, 50, 6, attr_column, file_column, rid);
	if (rc != 0)
		return -1;

	rc = myrbfm->closeFile(file_table);
	if(rc == -1)
		return -1;

	rc = myrbfm->closeFile(file_column);
	if(rc == -1)
		return -1;
	rc = myrbfm->closeFile(file_index);
	if(rc == -1)
		return -1;

	return 0;

}

RC RelationManager::deleteCatalog() {
	RC rc1, rc2,rc3;
	rc1 = myrbfm->destroyFile("Tables");
	rc2 = myrbfm->destroyFile("Columns");
	rc3 = myrbfm->destroyFile("Index");
	if (rc1 == 0 && rc2 == 0 && rc3 == 0)
		return 0;
	else
		return -1;
}

RC RelationManager::createTable(const string &tableName,
		const vector<Attribute> &attrs) {
	FileHandle file_table, file_column, file_new_table;
	RC loop, rc;

	RBFM_ScanIterator rmsi;
	string attr = "table-id";
	vector<string> attributes;
	attributes.push_back(attr);
	RID rid;
	RC table_id;
	char *returnedData = (char *) malloc(PAGE_SIZE);
	rc = myrbfm->openFile("Tables", file_table);
	if (rc != 0) {
		return -1;
	}
	rc = myrbfm->openFile("Columns", file_column);
	if (rc != 0) {
		return -1;
	}
	rc = myrbfm->createFile(tableName);
	if (rc != 0) {
		return -1;
	}
	rc = myrbfm->openFile(tableName, file_new_table);
	if (rc != 0) {
		return -1;
	}

	rc = myrbfm->scan(file_table, attr_table, "table-id", NO_OP, NULL,
			attributes, rmsi);

	if (rc != 0) {
		rmsi.close();
		return -1;
	}

	while (rmsi.getNextRecord(rid, returnedData) != RM_EOF) {
		table_id = *(int *) ((char *) returnedData + 1);
	}

	rc = prepareTableRecord((table_id + 1), tableName, tableName, attr_table,
			file_table, rid);
	if (rc != 0) {
		return -1;
	}
	for (loop = 0; (unsigned)loop < attrs.size(); loop++) {
		rc = prepareColumnRecord((table_id + 1), attrs[loop].name,
				attrs[loop].type, attrs[loop].length, loop + 1, attr_column,
				file_column, rid);
		if (rc != 0) {
			return -1;
		}
	}
	rc = myrbfm->closeFile(file_table);
	rc = myrbfm->closeFile(file_column);
	rc = myrbfm->closeFile(file_new_table);
	return 0;

}

RC RelationManager::deleteTable(const string &tableName) {

	if(tableName.compare("Tables") == 0 || tableName.compare("Columns") == 0 || tableName.compare("Index") == 0 )
		return -1;
	RC rc, loop;
	RID rid;
	char *returnedData = (char *) malloc(PAGE_SIZE);
	char *returnedData1 = (char *) malloc(PAGE_SIZE);
	FileHandle file_table, file_column,file_index;
	RBFM_ScanIterator rmsi, rmsi1,rbfmIterator;
	RC table_id;
	rc = myrbfm->openFile("Tables", file_table);
	if (rc != 0) {
		return -1;
	}
	rc = myrbfm->openFile("Columns", file_column);
	if (rc != 0) {
		return -1;
	}

	string attr = "table-id";
	vector<string> attributes;
	attributes.push_back(attr);
	char *tblName = (char *) malloc(100);
	int size = tableName.size();
	memcpy(tblName, &size, sizeof(int));
	memcpy(tblName + sizeof(int), tableName.c_str(), size);


	rc = myrbfm->scan(file_table, attr_table, "table-name", EQ_OP, (void *)tblName,
			attributes, rmsi);
	while (rmsi.getNextRecord(rid, returnedData) != RM_EOF) {
		table_id = *(int *) ((char *) returnedData + 1);
		rc = deleteTuple("Tables", rid);
		if (rc != 0) {
			rmsi.close();
			free(returnedData);
			return -1;

		}
	}

	rmsi.close();
	free(returnedData);

	loop = 0;
	string attr1 = "column-position";
	vector<string> attributes1;
	attributes1.push_back(attr1);
	rc = myrbfm->scan(file_column, attr_column, "table-id", EQ_OP, &table_id,
			attributes1, rmsi1);
	while (rmsi1.getNextRecord(rid, returnedData1) != RM_EOF) {
		rc = deleteTuple("Columns", rid);
		if (rc != 0) {
			loop++;
			rmsi1.close();
			free(returnedData1);
			return -1;
		}
	}

	rmsi1.close();
	free(returnedData1);

	rc = myrbfm->destroyFile(tableName);

	if (rc != 0) {
		return -1;
	}

	//Deleting data in index table and deleting the corresponding index file

	string attr3 = "file-name";
	vector<string> attributes_index;
	attributes_index.push_back(attr3);
	rc = myrbfm->openFile("Index",file_index);
	rc = myrbfm->scan(file_index, attr_index, "table-id", EQ_OP, &table_id,attributes_index, rbfmIterator);
	RID returnedrid;
	void* returnedData3 = malloc(PAGE_SIZE);
	while(rbfmIterator.getNextRecord(returnedrid, returnedData3) != RM_EOF)
	{
		int length;
		string index_file_name;
		memcpy(&length, (char*) returnedData3 + 1, sizeof(int));
		index_file_name.resize(length);
		memcpy(&index_file_name[0], (char*) returnedData3 + 1 + sizeof(int), length);
		rc = myindex_manager->destroyFile(index_file_name);
		if(rc == -1)
			return -1;
		rc = myrbfm->deleteRecord(file_index, attr_index,returnedrid);
		if(rc == -1)
			return -1;
	}
	rbfmIterator.close();
	free(returnedData3);
	rc = myrbfm->closeFile(file_table);
	if (rc == -1)
		return -1;
	rc = myrbfm->closeFile(file_column);
	if(rc == -1)
		return -1;
	rc = myrbfm->closeFile(file_index);
	if(rc == -1)
		return -1;

	return 0;

}

RC RelationManager::getAttributes(const string &tableName,
		vector<Attribute> &attrs) {
	RC rc;
	RID rid;
	char *returnedData = (char *) malloc(PAGE_SIZE);
	char *returnedData1 = (char *) malloc(PAGE_SIZE);
	FileHandle file_table, file_column;

	RBFM_ScanIterator rmsi, rmsi1;
	RC table_id;
	char column_name[100];
	int column_type, column_length, length;

	rc = myrbfm->openFile("Tables", file_table);
	if (rc != 0) {
		return -1;
	}
	rc = myrbfm->openFile("Columns", file_column);
	if (rc != 0) {
		return -1;
	}
	string attr = "table-id";
	vector<string> attributes;
	attributes.push_back(attr);
	char *tblName = (char *) malloc(100);
	int size = tableName.size();
	memcpy(tblName, &size, sizeof(int));
	memcpy(tblName + sizeof(int), tableName.c_str(), size);
	rc = myrbfm->scan(file_table, attr_table, "table-name", EQ_OP, (void *)tblName,
			attributes, rmsi);
	while (rmsi.getNextRecord(rid, returnedData) != RM_EOF) {
		table_id = *(int *) ((char *) returnedData + 1);
	}

	vector<string> attributes1;
	string attr10 = "column-name";
	attributes1.push_back(attr10);
	string attr11 = "column-type";
	attributes1.push_back(attr11);
	string attr12 = "column-length";
	attributes1.push_back(attr12);

	RC offset, nullbytes = 1;

	Attribute output_attr;

	rc = myrbfm->scan(file_column, attr_column, "table-id", EQ_OP, &table_id,
			attributes1, rmsi1);
	while (rmsi1.getNextRecord(rid, returnedData1) != RM_EOF) {
		offset = nullbytes;

		memcpy(&length, (char *) returnedData1 + offset, sizeof(int));
		offset += sizeof(int);
		memcpy(column_name, (char *) returnedData1 + offset, length);
		column_name[length] = '\0';
		output_attr.name = column_name;
		offset += length;

		memcpy(&column_type, (char *) returnedData1 + offset, sizeof(int));
		output_attr.type = static_cast<AttrType>(column_type);
		offset += sizeof(int);

		memcpy(&column_length, (char *) returnedData1 + offset, sizeof(int));
		output_attr.length = (AttrLength) column_length;
		offset += sizeof(int);

		attrs.push_back(output_attr);
	}
	rmsi.close();
	rmsi1.close();
	free(returnedData);
	free(returnedData1);

	rc = myrbfm->closeFile(file_table);
	rc = myrbfm->closeFile(file_column);

	return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data,
		RID &rid)
{
	RC rc;
	FileHandle file_table;
	vector<Attribute> attrs;

	rc = myrbfm->openFile(tableName, file_table);
	if (rc != 0) {
		return -1;
	}
	rc = getAttributes(tableName, attrs);
	if (rc != 0) {
		return -1;
	}
	rc = myrbfm->insertRecord(file_table, attrs, data, rid);

	if (rc != 0) {
		rc = myrbfm->closeFile(file_table);
		return -1;
	}

	rc = myrbfm->closeFile(file_table);
	if (rc != 0)
		return -1;

	//Inserting data into index file if an indexfile exists
	FileHandle file_index;

	rc = myrbfm->openFile("Index",file_index);
	if(rc == -1)
		return -1;
	RBFM_ScanIterator rbfmIterator;

	char *tblName = (char *) malloc(100);
	int size = tableName.size();
	memcpy(tblName, &size, sizeof(int));
	memcpy(tblName + sizeof(int), tableName.c_str(), size);

	vector <string> attr;
	attr.push_back("attribute-name");
	attr.push_back("file-name");

	rc = myrbfm->scan(file_index,attr_index,"table-name",EQ_OP,(void *)tblName,attr,rbfmIterator);
	RID returnedrid;
	void *returnedData = malloc(PAGE_SIZE);

	int nullsIndicator = ceil((double)attrs.size()/CHAR_BIT);

	void *key = malloc(PAGE_SIZE);
	while(rbfmIterator.getNextRecord(returnedrid,returnedData)!=RBFM_EOF)
	{
		int length;
		memcpy(&length, (char *)returnedData + 1, sizeof(int));
		string index_attribute;
		index_attribute.resize(length);
		memcpy(&index_attribute[0], (char *)returnedData + 1 + sizeof(int), length);

		int file_length;
		memcpy(&file_length, (char *)returnedData + 1 + sizeof(int) + length, sizeof(int));
		string index_file_name;
		index_file_name.resize(file_length);
		memcpy(&index_file_name[0], (char *)returnedData + 1 + sizeof(int) + length + sizeof(int), file_length);

		bool nullBit = false;
		int offset = nullsIndicator;
		for(int loop = 0; (unsigned)loop < attrs.size(); loop++)
		{

			nullBit = ((char*)data)[loop/8] & (1 << (7-(loop%8))) ;
			if(!nullBit)
			{
				if(index_attribute.compare(attrs[loop].name) == 0)
				{
					if(attrs[loop].type == TypeInt)
					{
						memcpy((char *)key, (char*) data + offset, sizeof(int));
						offset += sizeof(int);
						int temp;
						memcpy(&temp,(char *)data + offset - sizeof(int), sizeof(int));
						//cout <<"Rm.cc Insert Tupple B value "<<temp<<endl;

					}
					else if(attrs[loop].type == TypeReal)
					{
						memcpy((char *)key, (char*) data + offset, sizeof(float));
						offset += sizeof(float);
					}
					else
					{
						int length;
						memcpy(&length, (char *)data + offset, sizeof(int));
						//offset += sizeof(int);
						//memcpy((char *)key, &length, sizeof(int));
						memcpy((char *)key, (char *)data + offset, sizeof(int) + length);
						offset += (length + sizeof(int));
					}
					IXFileHandle ixfhandle;
					rc = myindex_manager->openFile(index_file_name, ixfhandle);
					if(rc == -1)
						return -1;
					rc = myindex_manager->insertEntry(ixfhandle, attrs[loop], key, rid);
					if(rc == -1)
						return -1;
					rc = myindex_manager->closeFile(ixfhandle);
					if(rc == -1)
						return -1;
					break;
				}
				else
				{

					if(attrs[loop].type == TypeInt)
						offset += sizeof(int);
					else if(attrs[loop].type == TypeReal)
						offset += sizeof(float);
					else
					{
						int length;
						memcpy(&length, (char*) data + offset, sizeof(int));
						offset += sizeof(int) + length;
					}
				}

			}
		}

	}

	free(tblName);
	free(key);
	rbfmIterator.close();
	rc = myrbfm->closeFile(file_index);
	if(rc == -1)
		return -1;
	return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
	RC rc;
	FileHandle file_table;
	vector<Attribute> attrs;

	rc = myrbfm->openFile(tableName, file_table);
	if (rc != 0) {
		return -1;
	}
	rc = getAttributes(tableName, attrs);
	if (rc != 0) {
		return -1;
	}

	void *recorddeleted = (char *)malloc(PAGE_SIZE);
	rc = myrbfm->readRecord(file_table,attrs,rid,recorddeleted);
	if(rc == -1)
		return -1;

	rc = myrbfm->deleteRecord(file_table, attrs, rid);
	if (rc != 0) {
		rc = myrbfm->closeFile(file_table);
		return -1;
	}
	rc = myrbfm->closeFile(file_table);
	if(rc == -1)
		return -1;

	//Deleting data from index file if an indexfile exists
	FileHandle file_index;

	rc = myrbfm->openFile("Index",file_index);
	if(rc == -1)
		return -1;
	RBFM_ScanIterator rbfmIterator;

	char *tblName = (char *) malloc(100);
	int size = tableName.size();
	memcpy(tblName, &size, sizeof(int));
	memcpy(tblName + sizeof(int), tableName.c_str(), size);

	vector <string> attr;
	attr.push_back("attribute-name");
	attr.push_back("file-name");

	rc = myrbfm->scan(file_index,attr_index,"table-name",EQ_OP,(void *)tblName,attr,rbfmIterator);
	RID returnedrid;
	void *returnedData = malloc(PAGE_SIZE);

	int nullsIndicator = ceil((double)attrs.size()/CHAR_BIT);
	bool nullBit = false;

	void *key = malloc(PAGE_SIZE);
	while(rbfmIterator.getNextRecord(returnedrid,returnedData)!=RBFM_EOF)
	{
		int length;
		memcpy(&length, (char *)returnedData + 1, sizeof(int));
		string index_attribute;
		index_attribute.resize(length);
		memcpy(&index_attribute[0], (char *)returnedData + 1 + sizeof(int), length);

		int file_length;
		memcpy(&file_length, (char *)returnedData + 1 + sizeof(int) + length, sizeof(int));
		string index_file_name;
		index_file_name.resize(file_length);
		memcpy(&index_file_name[0], (char *)returnedData + 1 + sizeof(int) + length + sizeof(int), file_length);


		int offset = nullsIndicator;
		for(int loop = 0; (unsigned)loop < attrs.size(); loop++)
		{

			nullBit = ((char*)recorddeleted)[loop/8] & (1 << (7-(loop%8)));
			if(!nullBit)
			{
				if(index_attribute.compare(attrs[loop].name) == 0)
				{
					if(attrs[loop].type == TypeInt)
					{
						memcpy((char *)key, (char*) recorddeleted + offset, sizeof(int));
						offset += sizeof(int);
					}
					if(attrs[loop].type == TypeReal)
					{
						memcpy((char *)key, (char*) recorddeleted + offset, sizeof(float));
						offset += sizeof(float);
					}
					else
					{
						int length;
						memcpy(&length, (char *)recorddeleted + offset, sizeof(int));
						offset += sizeof(int);
						memcpy((char *)key, &length, sizeof(int));
						memcpy((char *)key + sizeof(int), (char *)recorddeleted + offset, length);
						offset += length;
					}
					IXFileHandle ixfhandle;
					rc = myindex_manager->openFile(index_file_name, ixfhandle);
					if(rc == -1)
						return -1;
					rc = myindex_manager->deleteEntry(ixfhandle, attrs[loop], key, rid);
					if(rc == -1)
						return -1;
					rc = myindex_manager->closeFile(ixfhandle);
					if(rc == -1)
						return -1;
					break;
				}
				else
				{

					if(attrs[loop].type == TypeInt)
						offset += sizeof(int);
					else if(attrs[loop].type == TypeReal)
						offset += sizeof(float);
					else
					{
						int length;
						memcpy(&length, (char*) recorddeleted + offset, sizeof(int));
						offset += sizeof(int) + length;
					}
				}


			}
		}

	}

	free(tblName);
	free(key);
	rbfmIterator.close();
	free(recorddeleted);
	rc = myrbfm->closeFile(file_index);
	if(rc == -1)
		return -1;

	return 0;


}

RC RelationManager::updateTuple(const string &tableName, const void *data,
		const RID &rid) {
	RC rc;
	FileHandle file_table;
	vector<Attribute> attrs;

	rc = myrbfm->openFile(tableName, file_table);
	if (rc != 0) {
		return -1;
	}
	rc = getAttributes(tableName, attrs);
	if (rc != 0) {
		return -1;
	}
	void *recorddeleted = malloc(PAGE_SIZE);
	rc = myrbfm->readRecord(file_table,attrs,rid,recorddeleted);
	if(rc == -1)
		return -1;
	rc = myrbfm->updateRecord(file_table, attrs, data, rid);
	if (rc != 0) {
		rc = myrbfm->closeFile(file_table);
		return -1;
	}
	rc = myrbfm->closeFile(file_table);


	//Deleting data from index file if an indexfile exists
	FileHandle file_index;

	rc = myrbfm->openFile("Index",file_index);
	if(rc == -1)
		return -1;
	RBFM_ScanIterator rbfmIterator;

	char *tblName = (char *) malloc(100);
	int size = tableName.size();
	memcpy(tblName, &size, sizeof(int));
	memcpy(tblName + sizeof(int), tableName.c_str(), size);

	vector <string> attr;
	attr.push_back("attribute-name");
	attr.push_back("file-name");

	rc = myrbfm->scan(file_index,attr_index,"table-name",EQ_OP,(void *)tblName,attr,rbfmIterator);
	RID returnedrid;
	void *returnedData = malloc(PAGE_SIZE);

	int nullsIndicator = ceil((double)attrs.size()/CHAR_BIT);
	bool nullBit = false;

	void *key = malloc(PAGE_SIZE);
	while(rbfmIterator.getNextRecord(returnedrid,returnedData)!=RBFM_EOF)
	{
		int length;
		memcpy(&length, (char *)returnedData + 1, sizeof(int));
		string index_attribute;
		index_attribute.resize(length);
		memcpy(&index_attribute[0], (char *)returnedData + 1 + sizeof(int), length);

		int file_length;
		memcpy(&file_length, (char *)returnedData + 1 + sizeof(int) + length, sizeof(int));
		string index_file_name;
		index_file_name.resize(file_length);
		memcpy(&index_file_name[0], (char *)returnedData + 1 + sizeof(int) + length + sizeof(int), file_length);


		int offset = nullsIndicator;
		for(int loop = 0; (unsigned)loop < attrs.size(); loop++)
		{

			nullBit = ((char*)recorddeleted)[loop/8] & (1 << (7-(loop%8)));
			if(!nullBit)
			{
				if(index_attribute.compare(attrs[loop].name) == 0)
				{
					if(attrs[loop].type == TypeInt)
					{
						memcpy((char *)key, (char*) recorddeleted + offset, sizeof(int));
						offset += sizeof(int);
					}
					if(attrs[loop].type == TypeReal)
					{
						memcpy((char *)key, (char*) recorddeleted + offset, sizeof(float));
						offset += sizeof(float);
					}
					else
					{
						int length;
						memcpy(&length, (char *)recorddeleted + offset, sizeof(int));
						offset += sizeof(int);
						memcpy((char *)key, &length, sizeof(int));
						memcpy((char *)key + sizeof(int), (char *)recorddeleted + offset, length);
						offset += length;
					}
					IXFileHandle ixfhandle;
					rc = myindex_manager->openFile(index_file_name, ixfhandle);
					if(rc == -1)
						return -1;
					rc = myindex_manager->deleteEntry(ixfhandle, attrs[loop], key, rid);
					if(rc == -1)
						return -1;
					rc = myindex_manager->closeFile(ixfhandle);
					if(rc == -1)
						return -1;
					break;
				}
				else
				{

					if(attrs[loop].type == TypeInt)
						offset += sizeof(int);
					else if(attrs[loop].type == TypeReal)
						offset += sizeof(float);
					else
					{
						int length;
						memcpy(&length, (char*) recorddeleted + offset, sizeof(int));
						offset += sizeof(int) + length;
					}
				}


			}
		}

	}

	free(tblName);
	free(key);
	rbfmIterator.close();
	free(recorddeleted);
	rc = myrbfm->closeFile(file_index);
	if(rc == -1)
		return -1;



	//Inserting data into index file if an indexfile exists
	FileHandle file_index1;

	rc = myrbfm->openFile("Index",file_index1);
	if(rc == -1)
		return -1;
	RBFM_ScanIterator rbfmIterator1;

	char *tblName1 = (char *) malloc(100);
	int size1 = tableName.size();
	memcpy(tblName1, &size1, sizeof(int));
	memcpy(tblName1 + sizeof(int), tableName.c_str(), size1);

	vector <string> attr_insert;
	attr_insert.push_back("attribute-name");
	attr_insert.push_back("file-name");

	rc = myrbfm->scan(file_index1,attr_index,"table-name",EQ_OP,(void *)tblName1,attr_insert,rbfmIterator1);
	RID returnedrid1;
	void *returnedData1 = malloc(PAGE_SIZE);

	int nullsIndicator1 = ceil((double)attrs.size()/CHAR_BIT);

	void *key1 = malloc(PAGE_SIZE);
	while(rbfmIterator.getNextRecord(returnedrid1,returnedData1)!=RBFM_EOF)
	{
		int length1;
		memcpy(&length1, (char *)returnedData1 + 1, sizeof(int));
		string index_attribute1;
		index_attribute1.resize(length1);
		memcpy(&index_attribute1[0], (char *)returnedData1 + 1 + sizeof(int), length1);

		int file_length1;
		memcpy(&file_length1, (char *)returnedData1 + 1 + sizeof(int) + length1, sizeof(int));
		string index_file_name1;
		index_file_name1.resize(file_length1);
		memcpy(&index_file_name1[0], (char *)returnedData1 + 1 + sizeof(int) + length1 + sizeof(int), file_length1);

		bool nullBit1 = false;
		int offset1 = nullsIndicator1;
		for(int loop = 0; (unsigned)loop < attrs.size(); loop++)
		{
			nullBit1 = ((char*)data)[loop/8] & (1 << (7-(loop%8))) ;
			if(!nullBit1)
			{
				if(index_attribute1.compare(attrs[loop].name) == 0)
				{
					if(attrs[loop].type == TypeInt)
					{
						memcpy((char *)key, (char*) data + offset1, sizeof(int));
						offset1 += sizeof(int);
					}
					if(attrs[loop].type == TypeReal)
					{
						memcpy((char *)key1, (char*) data + offset1, sizeof(float));
						offset1 += sizeof(float);
					}
					else
					{
						int length1;
						memcpy(&length1, (char *)data + offset1, sizeof(int));
						offset1 += sizeof(int);
						memcpy((char *)key1, &length1, sizeof(int));
						memcpy((char *)key1 + sizeof(int), (char *)data + offset1, length1);
						offset1 += length1;
					}
					IXFileHandle ixfhandle1;
					rc = myindex_manager->openFile(index_file_name1, ixfhandle1);
					if(rc == -1)
						return -1;
					rc = myindex_manager->insertEntry(ixfhandle1, attrs[loop], key1, rid);
					if(rc == -1)
						return -1;
					rc = myindex_manager->closeFile(ixfhandle1);
					if(rc == -1)
						return -1;
					break;
				}
				else
				{

					if(attrs[loop].type == TypeInt)
						offset1 += sizeof(int);
					else if(attrs[loop].type == TypeReal)
						offset1 += sizeof(float);
					else
					{
						int length1;
						memcpy(&length1, (char*) data + offset1, sizeof(int));
						offset1 += sizeof(int) + length1;
					}
				}

			}
		}

	}

	free(tblName);
	free(key);
	rbfmIterator.close();
	rc = myrbfm->closeFile(file_index1);
	if(rc == -1)
		return -1;




	return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid,
		void *data) {
	RC rc;
	FileHandle file_table;
	vector<Attribute> attrs;

	rc = myrbfm->openFile(tableName, file_table);
	if (rc != 0) {
		return -1;
	}
	rc = getAttributes(tableName, attrs);
	if (rc != 0) {
		return -1;
	}
	rc = myrbfm->readRecord(file_table, attrs, rid, data);
	if (rc != 0) {
		rc = myrbfm->closeFile(file_table);
		return -1;

	}
	rc = myrbfm->closeFile(file_table);
	return 0;

}

RC RelationManager::printTuple(const vector<Attribute> &attrs,
		const void *data) {
	RC rc;
	rc = myrbfm->printRecord(attrs, data);
	if (rc != 0) {
		return -1;
	}
	return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid,
		const string &attributeName, void *data) {
	RC rc;
	FileHandle file_table;
	vector<Attribute> attrs;

	rc = myrbfm->openFile(tableName, file_table);
	if (rc != 0) {
		return -1;
	}
	rc = getAttributes(tableName, attrs);
	if (rc != 0) {
		return -1;
	}
	rc = myrbfm->readAttribute(file_table, attrs, rid, attributeName, data);
	if (rc != 0) {
		rc = myrbfm->closeFile(file_table);
		return -1;

	}
	rc = myrbfm->closeFile(file_table);
	return 0;

}

RC RelationManager::scan(const string &tableName,
		const string &conditionAttribute, const CompOp compOp,
		const void *value, const vector<string> &attributeNames,
		RM_ScanIterator &rm_ScanIterator) {
	RC rc;
	FileHandle fhandle;
	vector<Attribute> attrs;

	rc = myrbfm->openFile(tableName, fhandle);

	if (rc != 0) {
		return -1;
	}
	rc = getAttributes(tableName, attrs);
	if (rc != 0) {
		return -1;
	}
	rc = myrbfm->scan(fhandle, attrs, conditionAttribute, compOp, value,
			attributeNames, rm_ScanIterator.rbfm_rm_ScanIterator);

	return 0;

}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName,
		const string &attributeName) {
	return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName,
		const Attribute &attr) {
	return -1;
}

void RelationManager::createTableRecordDescriptor(
		vector<Attribute> &record_Descriptor) {
	Attribute attr;

	attr.name = "table-id";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	record_Descriptor.push_back(attr);

	attr.name = "table-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 50;
	record_Descriptor.push_back(attr);

	attr.name = "file-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 50;
	record_Descriptor.push_back(attr);

}

void RelationManager::createIndexRecordDescriptor(
		vector<Attribute> &record_Descriptor) {
	Attribute attr;

	attr.name = "index-id";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	record_Descriptor.push_back(attr);

	attr.name = "table-id";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	record_Descriptor.push_back(attr);

	attr.name = "table-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 50;
	record_Descriptor.push_back(attr);

	attr.name = "attribute-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 50;
	record_Descriptor.push_back(attr);

	attr.name = "attribute-type";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	record_Descriptor.push_back(attr);

	attr.name = "file-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 50;
	record_Descriptor.push_back(attr);



}
void RelationManager::createColumnRecordDescriptor(
		vector<Attribute> &record_Descriptor) {
	Attribute attr;

	attr.name = "table-id";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	record_Descriptor.push_back(attr);

	attr.name = "column-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 50;
	record_Descriptor.push_back(attr);

	attr.name = "column-type";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	record_Descriptor.push_back(attr);

	attr.name = "column-length";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	record_Descriptor.push_back(attr);

	attr.name = "column-position";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	record_Descriptor.push_back(attr);

}
RC RelationManager::prepareTableRecord(const int table_id,
		const string &table_name, const string &file_name,
		const vector<Attribute> &recordDescriptor, FileHandle &fileHandle,
		RID &rid) {
	char *data = (char *) malloc(PAGE_SIZE);
	RC offset = 0, length, rc;
	int nullDescBytes = ceil((double) recordDescriptor.size() / CHAR_BIT);
	unsigned char *nullsIndicator = (unsigned char *) malloc(nullDescBytes);
	memset(nullsIndicator, 0, nullDescBytes);

	memcpy(data, nullsIndicator, nullDescBytes);
	offset += nullDescBytes;

	memcpy(data + offset, &table_id, sizeof(int));
	offset += sizeof(int);

	length = table_name.length();
	memcpy(data + offset, &length, sizeof(int));
	offset += sizeof(int);
	memcpy(data + offset, table_name.c_str(), length);
	offset += length;

	length = file_name.length();
	memcpy(data + offset, &length, sizeof(int));
	offset += sizeof(int);
	memcpy(data + offset, file_name.c_str(), length);
	offset += length;

	rc = myrbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
	return rc;
}

RC RelationManager::prepareColumnRecord(const int table_id,
		const string &column_name, const int column_type,
		const int column_length, const int column_position,
		const vector<Attribute> &recordDescriptor, FileHandle &fileHandle,
		RID &rid) {
	char *data = (char *) malloc(PAGE_SIZE);
	RC offset = 0, length, rc;
	int nullDescBytes = ceil((double) recordDescriptor.size() / CHAR_BIT);
	unsigned char *nullsIndicator = (unsigned char *) malloc(nullDescBytes);
	memset(nullsIndicator, 0, nullDescBytes);

	memcpy(data, nullsIndicator, nullDescBytes);

	offset += nullDescBytes;

	memcpy(data + offset, &table_id, sizeof(int));
	offset += sizeof(int);

	length = column_name.length();
	memcpy(data + offset, &length, sizeof(int));
	offset += sizeof(int);
	memcpy(data + offset, column_name.c_str(), length);
	offset += length;

	memcpy(data + offset, &column_type, sizeof(int));
	offset += sizeof(int);
	memcpy(data + offset, &column_length, sizeof(int));
	offset += sizeof(int);
	memcpy(data + offset, &column_position, sizeof(int));
	offset += sizeof(int);
	rc = myrbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
	return rc;

}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
	return this->rbfm_rm_ScanIterator.getNextRecord(rid, data);
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	vector<Attribute> attributes;
	Attribute key;
	getAttributes(tableName,attributes);
	for(int loop = 0; (unsigned)loop < attributes.size(); loop++)
	{
		if(attributes[loop].name == attributeName)
		{
			key.length = attributes[loop].length;
			key.name = attributes[loop].name;
			key.type = attributes[loop].type;
			break;
		}
	}

	//Scan to get index_id

	int index_id = 0;
	RBFM_ScanIterator rbfmIterator;
	vector<string> index_attribute;
	index_attribute.push_back("index-id");
	FileHandle index_filehandle;

	RC rc = myrbfm->openFile("Index",index_filehandle);
	if (rc == -1)
		return -1;

	void* returnedData = malloc(PAGE_SIZE);
	RID rid;
	rc = myrbfm->scan(index_filehandle, attr_index, "index-id",NO_OP,NULL, index_attribute, rbfmIterator);
	while(rbfmIterator.getNextRecord(rid,returnedData) != RBFM_EOF)
	{
		memcpy(&index_id,(char*)returnedData+1,sizeof(int));
	}
	if(index_id < 1)
		index_id = 1;
	else
		index_id++;
	rbfmIterator.close();
	rc = myrbfm->closeFile(index_filehandle);
	if (rc == -1)
		return -1;
	free(returnedData);

	//Scan to get the table-id

	RBFM_ScanIterator rbfmIterator1;
	FileHandle tables_filehandle;
	rc =myrbfm->openFile("Tables",tables_filehandle);
	if (rc == -1)
		return -1;

	char *tblName = (char *) malloc(100);
	int size = tableName.size();
	memcpy(tblName, &size, sizeof(int));
	memcpy(tblName + sizeof(int), tableName.c_str(), size);
	vector<string>attributeNames;
	attributeNames.push_back("table-id");

	rc = myrbfm->scan(tables_filehandle,attr_table,"table-name",EQ_OP,(void *)tblName,attributeNames,rbfmIterator1);

	void *returnedData1 = malloc(PAGE_SIZE);
	int table_id;
	while(rbfmIterator.getNextRecord(rid,returnedData1)!=RBFM_EOF)
	{
		memcpy(&table_id,(char*)returnedData1+1,sizeof(int));
	}

	free(returnedData1);
	rbfmIterator1.close();
	free(tblName);
	rc =myrbfm->closeFile(tables_filehandle);
	if (rc == -1)
		return -1;

	//Insert the data into the index_table

	FileHandle index_filehandle1;
	rc = myrbfm->openFile("Index",index_filehandle1);
	if (rc == -1)
		return -1;
	string fileName = tableName + "_" + attributeName + ".idx";

	rc = prepareIndexRecord(index_id, table_id, tableName, attributeName,
			key.type, fileName, attr_index, index_filehandle1, rid);
	if (rc == -1)
		return -1;
	rc = myrbfm->closeFile(index_filehandle1);
	if (rc == -1)
		return -1;


	//Inserting the key into index
	rc = myindex_manager->createFile(fileName);
	if(rc == -1)
		return -1;
	IXFileHandle ix_filehandle;
	rc = myindex_manager->openFile(fileName, ix_filehandle);
	if(rc == -1)
		return -1;

	FileHandle table_filehandle;
	rc = myrbfm->openFile(tableName, table_filehandle);
	if(rc == -1)
		return -1;
	RBFM_ScanIterator rbfmIterator3;
	vector<string> attribute_names;
	attribute_names.push_back(attributeName);

	rc = myrbfm->scan(table_filehandle, attributes, "", NO_OP, NULL, attribute_names, rbfmIterator3);
	RID returnedRid;
	void* returnedData3 = malloc(PAGE_SIZE);
	void* index_key = malloc(PAGE_SIZE);
	while(rbfmIterator3.getNextRecord(returnedRid, returnedData3) != RBFM_EOF)
	{
		if(key.type == TypeInt){
			memcpy((char*) index_key, (char*) returnedData3 + 1, sizeof(int));
			int temp;
			memcpy(&temp,(char *)index_key,sizeof(int));
			//cout<<"CreateIndex  int "<<temp<<endl;
		}
		else if(key.type == TypeReal){
			memcpy((char*) index_key, (char*) returnedData3 + 1, sizeof(float));
			float temp;
			memcpy(&temp,(char *)index_key,sizeof(float));
			//cout<<"CreateIndex  Float "<<temp<<endl;
		}
		else{
			int keylength;
			memcpy(&keylength, (char*) returnedData3 + 1, sizeof(int));
			memcpy((char*) index_key, &keylength, sizeof(int));
			memcpy((char*) index_key + sizeof(int), (char*) returnedData3 + 1 + sizeof(int), keylength);
		}
		rc = myindex_manager->insertEntry(ix_filehandle, key, index_key, returnedRid);
		if(rc == -1)
			return -1;

	}

	rbfmIterator3.close();
	rc = myrbfm->closeFile(table_filehandle);
	if (rc == -1)
		return -1;
//	//added by me
//	cout <<"In CreateIndex .. Print B-Tree"<<endl;
//
//	myindex_manager->printBtree(ix_filehandle,key);
//	//Added by me

	rc = myindex_manager->closeFile(ix_filehandle);
	if (rc == -1)
		return -1;

	free(index_key);
	free(returnedData3);

	return 0;
}

RC RelationManager::prepareIndexRecord(const int index_id,
		const int table_id, const string &table_name,
		const string &attribute_name,const int attribute_type,const string &file_name,
		const vector<Attribute> &recordDescriptor, FileHandle &fileHandle,
		RID &rid)
{
	char *data = (char *) malloc(PAGE_SIZE);
	RC offset = 0, length, rc;
	int nullDescBytes = ceil((double) recordDescriptor.size() / CHAR_BIT);
	unsigned char *nullsIndicator = (unsigned char *) malloc(nullDescBytes);
	memset(nullsIndicator, 0, nullDescBytes);

	memcpy(data, nullsIndicator, nullDescBytes);

	offset += nullDescBytes;

	memcpy(data + offset, &index_id, sizeof(int));
	offset += sizeof(int);

	memcpy(data + offset, &table_id, sizeof(int));
	offset += sizeof(int);

	length = table_name.length();
	memcpy(data + offset, &length, sizeof(int));
	offset += sizeof(int);
	memcpy(data + offset, table_name.c_str(), length);
	offset += length;

	length = attribute_name.length();
	memcpy(data + offset, &length, sizeof(int));
	offset += sizeof(int);
	memcpy(data + offset, attribute_name.c_str(), length);
	offset += length;

	memcpy(data + offset, &attribute_type, sizeof(int));
	offset += sizeof(int);

	length = file_name.length();
	memcpy(data + offset, &length, sizeof(int));
	offset += sizeof(int);
	memcpy(data + offset, file_name.c_str(), length);
	offset += length;

	rc = myrbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
	return rc;

}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	FileHandle index_handle;
	RC rc = myrbfm->openFile("Index",index_handle);
	vector <string> attribute_index;
	attribute_index.push_back("file-name");
	RBFM_ScanIterator rbfmIterator;
	char *tblName = (char *) malloc(100);
	int size = tableName.size();
	memcpy(tblName, &size, sizeof(int));
	memcpy(tblName + sizeof(int), tableName.c_str(), size);
	rc = myrbfm->scan(index_handle, attr_index, "table-name", EQ_OP, (void *)tblName, attribute_index, rbfmIterator);
	void* returnedData = malloc(PAGE_SIZE);
	RID returnedrid;
	while(rbfmIterator.getNextRecord(returnedrid, returnedData) != RBFM_EOF)
	{

		int length;
		string index_file_name;
		memcpy(&length, (char *) returnedData+ 1, sizeof(int));
		index_file_name.resize(length);
		memcpy(&index_file_name[0], (char *)returnedData +1 + sizeof(int),length);
		rc = myindex_manager->destroyFile(index_file_name);
		if(rc == -1)
			return -1;
		rc = myrbfm->deleteRecord(index_handle,attr_index,returnedrid);
		if(rc == -1)
			return -1;
	}
	rbfmIterator.close();
	free(returnedData);
	free(tblName);
	rc = myrbfm->closeFile(index_handle);
	if(rc == -1)
		return -1;

	return 0;

}

RC RelationManager::indexScan(const string &tableName, const string &attributeName, const void *lowKey,
		const void *highKey, bool lowKeyInclusive, bool highKeyInclusive,
		RM_IndexScanIterator &rm_IndexScanIterator)
{
	vector <Attribute> attributes;
	getAttributes(tableName, attributes);
	Attribute attr;
	for(int loop = 0; (unsigned)loop < attributes.size(); loop++)
	{
		if(attributes[loop].name == attributeName)
		{
			attr.name = attributeName;
			attr.length = attributes[loop].length;
			attr.type = attributes[loop].type;
		}
	}
	FileHandle file_index;
	RC rc = myrbfm->openFile("Index",file_index);
	if (rc == -1)
		return -1;

	RBFM_ScanIterator rbfmIterator;

	vector <string> attr1;
	attr1.push_back("attribute-name");
	attr1.push_back("file-name");

	char *tblName = (char *) malloc(100);
	int size = tableName.size();
	memcpy(tblName, &size, sizeof(int));
	memcpy(tblName + sizeof(int), tableName.c_str(), size);

	rc = myrbfm->scan(file_index, attr_index, "table-name", EQ_OP, (void *)tblName, attr1, rbfmIterator);
	void *returnedData = malloc(PAGE_SIZE);
	RID returnedrid;
	string index_filename;
	while(rbfmIterator.getNextRecord(returnedrid,returnedData) != RBFM_EOF)
	{
		int length;
		int offset = 1;
		memcpy(&length,(char *)returnedData + offset, sizeof(int));
		string attribute_name;
		attribute_name.resize(length);
		offset += sizeof(int);
		memcpy(&attribute_name[0],(char *)returnedData + offset, length);
		offset += length;
		if(attribute_name.compare(attributeName) == 0)
		{

			int file_length;
			memcpy(&file_length,(char *)returnedData + offset, sizeof(int));
			offset += sizeof(int);
			index_filename.resize(file_length);
			memcpy(&index_filename[0],(char*)returnedData + offset, file_length);
			break;
		}
	}
	IXFileHandle fhandle;
	rc = myindex_manager->openFile(index_filename,fhandle);
	if(rc == -1)
		return -1;
	rc = myindex_manager->scan(fhandle,attr,lowKey, highKey, lowKeyInclusive,
			highKeyInclusive, rm_IndexScanIterator.ixScanIterator);
	if(rc == -1)
		return -1;
	rc = myrbfm->closeFile(file_index);
	if (rc == -1)
		return -1;
	free(returnedData);
	rbfmIterator.close();
	return 0;
}


RC RM_IndexScanIterator::getNextEntry(RID &rid, void* key)
{
	if(ixScanIterator.getNextEntry(rid,key) != IX_EOF)
		return 0;
	return -1;
}

RC RM_IndexScanIterator::close()
{
	ixScanIterator.close();
	return (myindex_manager->closeFile(ixScanIterator.ixfileHandle));
}
