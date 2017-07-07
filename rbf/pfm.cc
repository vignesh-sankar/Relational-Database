#include "pfm.h"
fstream f;
PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance() {
	if (!_pf_manager)
		_pf_manager = new PagedFileManager();

	return _pf_manager;
}

PagedFileManager::PagedFileManager() {
}

PagedFileManager::~PagedFileManager() {
}

RC PagedFileManager::createFile(const string &fileName) {
	struct stat info;

	if (stat(fileName.c_str(), &info) == 0)
		return -1;
	else {
		std::fstream file(fileName.c_str(), fstream::out | fstream::binary);
		return 0;
	}
}

RC PagedFileManager::destroyFile(const string &fileName) {
	struct stat info;
	if (stat(fileName.c_str(), &info) == 0) {
		std::remove(fileName.c_str());
		return 0;
	} else
		return -1;
}

RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	struct stat info;

	if (fileHandle.myfilehandler != NULL) {
		//cout<<"pfm.c openFile .. FileHandle is already a handle for an open file "<<endl;
		return -1;
	}
	if (stat(fileName.c_str(), &info) == 0) {

		fileHandle.myfilehandler = fopen(fileName.c_str(), "rb+");

		if (fileHandle.myfilehandler == NULL) {
			//cout<<"pfm.c opeFile $$ Error in opening the file "<<fileName.c_str()<<endl;
			return -1;
		} else {
			//cout<<"pfm.c openFile $$$ file opened successfully "<<fileName.c_str()<<endl;
			return 0;

		}
	} else
		return -1;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {

	if (fileHandle.myfilehandler != NULL) {
		RC rc;
		fflush(fileHandle.myfilehandler);
		rc = fclose(fileHandle.myfilehandler);
		if (rc == 0) {
			//cout<<"pfm. closeFile ... File closed successfully"<<endl;
			return 0;
		} else {
			//cout<<"pfm.c closeFile .. Problems with file closing"<<endl;
			return -1;
		}
	} else {
		//cout <<"File pointer is NULL *** pfm.c closefile()"<<endl;
		return -1;
	}
}

FileHandle::FileHandle() {
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
	myfilehandler = NULL;
}

FileHandle::~FileHandle() {
}

RC FileHandle::readPage(PageNum pageNum, void *data) {
	RC rc, totalPages;
	totalPages = getNumberOfPages() - 1;

	if (pageNum >= 0 && pageNum <= (unsigned)totalPages) {

		rc = fseek(myfilehandler, (long) ((pageNum) * (PAGE_SIZE)), SEEK_SET);
		if (rc != 0) {
			//cout<<"pfm.c readPage Error in fseek rc = "<<rc<<endl;
			return -1;
		}

		rc = fread(data, PAGE_SIZE, 1, myfilehandler);
		if (rc != 1) {
			//cout<<"pfm.c readPage ... Error in fread rc "<<rc<<endl;
			return -1;
		}
		//}
		//cout<<"pfm.c readPage successfully executed "<<endl;
		readPageCounter++;

		return 0;
	} else {
		return -1;
	}
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {

	RC rc, totalPages;
	totalPages = getNumberOfPages() - 1;

	if (pageNum >= 0 && pageNum <= (unsigned)totalPages) {

		rc = fseek(myfilehandler, (long) ((pageNum) * (PAGE_SIZE)), SEEK_SET);
		if (rc != 0) {
			//cout<<"pfm.c readPage Error in fseek rc = "<<rc<<endl;
			return -1;
		}
		if (myfilehandler == NULL) {
			//cout<<"myfilehandler after fseek is NULL"<<endl;
			return -1;
		}

		//while(!feof(myfilehandler))
		//{
		rc = fwrite(data, PAGE_SIZE, 1, myfilehandler);
		if (rc != 1) {
			return -1;
		}
		writePageCounter++;

		return 0;
	} else
		return -1;

}

RC FileHandle::appendPage(const void *data) {
//	f.seekp(0, f.end);
//	f.write ((char *)data,PAGE_SIZE);
//	appendPageCounter = appendPageCounter + 1;
//	return 0;

	RC rc;
	if (myfilehandler == NULL) {
		//cout<<"pfm.c appendPage FileHandler is null"<<endl;
		return -1;
	}
	//cout<<"Before fseek "<<endl;
	rc = fseek(myfilehandler, 0, SEEK_END);
	if (rc != 0) {
		//cout<<"pfm.c appendPage Error in fseek() Return value = "<<rc<<endl;
		return -1;
	}
	//cout<<"After fseek Return value of fseek "<<rc<<endl;

	rc = fwrite(data, PAGE_SIZE, 1, myfilehandler);
	//cout<<"pfm.c Return value of fwrite = "<<rc<<endl;

	if (rc != 1) /*Confirm this */
	{
		//cout<<"Appending is not successfull .. No of bytes written = "<<rc<<endl;
		return -1;
	}
	appendPageCounter++;
	return 0;
}

unsigned FileHandle::getNumberOfPages() {
//	f.seekg(0, f.end);
//	int pageCounter = f.tellg()/PAGE_SIZE;
//	return pageCounter;
//
	RC rc, size, noOfPages;
	rc = fseek(myfilehandler, 0, SEEK_END);
	//cout<<"getNumberOfPages Return value of fseek "<<rc<<endl;
	if (rc != 0) {
		//cout<<"pfm.c getNumberofPages Error in fseek function"<<endl;
		return -1;
	}
	size = ftell(myfilehandler);
	//cout<<"Value of size = "<<size<<endl;
	noOfPages = size / PAGE_SIZE;
	//cout<<"value of no.of pages = "<<noOfPages<<endl;
	return noOfPages;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount,
		unsigned &writePageCount, unsigned &appendPageCount) {
	readPageCount = readPageCounter;
	writePageCount = writePageCounter;
	appendPageCount = appendPageCounter;

	return 0;
}
