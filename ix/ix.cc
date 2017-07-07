
#include "ix.h"
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <vector>
#include <cstdio>
#include <algorithm>

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if (!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
    pfm = PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string& fileName)
{
    return pfm->createFile(fileName);
}

RC IndexManager::destroyFile(const string& fileName)
{
    return pfm->destroyFile(fileName);
}

RC IndexManager::openFile(const string& fileName, IXFileHandle& ixfileHandle)
{

    return pfm->openFile(fileName, ixfileHandle.fileHandle);
}

RC IndexManager::closeFile(IXFileHandle& ixfileHandle)
{
    return pfm->closeFile(ixfileHandle.fileHandle);
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::close()
{
    free(arr);
    isEOF = true;
    return 0;
}

IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::readPage(PageNum pageNum, void* data)
{
    return fileHandle.readPage(pageNum, data);
}
RC IXFileHandle::writePage(PageNum pageNum, const void* data)
{
    return fileHandle.writePage(pageNum, data);
}
RC IXFileHandle::appendPage(const void* data)
{
    return fileHandle.appendPage(data);
}
unsigned IXFileHandle::getNumberOfPages()
{
    return fileHandle.getNumberOfPages();
}
RC IXFileHandle::collectCounterValues(unsigned& readPageCount, unsigned& writePageCount, unsigned& appendPageCount)
{
    readPageCount = fileHandle.readPageCounter;
    writePageCount = fileHandle.writePageCounter;
    appendPageCount = fileHandle.appendPageCounter;
    return 0;
}

RC IndexManager::deleteEntry(IXFileHandle& ixfileHandle, const Attribute& attribute, const void* key, const RID& rid)
{
    return deletedatafromTree(ixfileHandle, attribute, key, rid, 0);
}
RC IndexManager::deletedatafromTree(IXFileHandle& ixfileHandle, const Attribute& attribute, const void* key, const RID& rid, int pageNum)
{
    char* arr = (char*)malloc(PAGE_SIZE);
    ixfileHandle.readPage(pageNum, arr);
    bool isLeaf;
    memcpy(&isLeaf, (char*)arr + 4095, sizeof(bool));
    if (!isLeaf) {
        int child;
        findKey(attribute, arr, key, child);
        RC rc = deletedatafromTree(ixfileHandle, attribute, key, rid, child);
        free(arr);
        return rc;
    }
    else {
        short freeSpacePointer;
        memcpy(&freeSpacePointer, (char*)arr + 4093, sizeof(short));
        short totalEntries;
        memcpy(&totalEntries, (char*)arr + 4091, sizeof(short));
        int count = 0;
        if (attribute.type == TypeInt) {
            int key1;
            int offset1 = -1;
            memcpy(&key1, (char*)key, sizeof(int));
            for (int loop = 0; loop < totalEntries; loop++) {
                int offset = 3 * sizeof(int) * loop;
                int key2;
                memcpy(&key2, (char*)arr + offset, sizeof(int));
                offset += sizeof(int);
                int pageData;
                memcpy(&pageData, (char*)arr + offset, sizeof(int));
                offset += sizeof(int);
                int slotData;
                memcpy(&slotData, (char*)arr + offset, sizeof(int));
                if (key1 == key2 && (unsigned)pageData == rid.pageNum && (unsigned)slotData == rid.slotNum) {
                    offset1 = loop * 3 * sizeof(int);
                    break;
                }
            }
            if (offset1 != -1) {
                int shiftstartpos = offset1 + 3 * sizeof(int);
                int length = freeSpacePointer - shiftstartpos;
                memmove((char*)arr + offset1, (char*)arr + shiftstartpos, length);
                freeSpacePointer -= (3 * sizeof(int));
                memcpy((char*)arr + 4093, &freeSpacePointer, sizeof(short));
                totalEntries -= 1;
                memcpy((char*)arr + 4091, &totalEntries, sizeof(short));
                count = 1;
            }
        }
        else if (attribute.type == TypeReal) {
            float key1;
            int offset1 = -1;
            memcpy(&key1, (char*)key, sizeof(float));
            for (int loop = 0; loop < totalEntries; loop++) {
                int offset = (2 * sizeof(int) + sizeof(float)) * loop;
                float key2;
                memcpy(&key2, (char*)arr + offset, sizeof(float));
                offset += sizeof(float);
                int pageData;
                memcpy(&pageData, (char*)arr + offset, sizeof(int));
                offset += sizeof(int);
                int slotData;
                memcpy(&slotData, (char*)arr + offset, sizeof(int));
                if (key1 == key2 && (unsigned)pageData == rid.pageNum && (unsigned)slotData == rid.slotNum) {
                    offset1 = loop * (sizeof(float) + 2 * sizeof(int));
                    break;
                }
            }
            if (offset1 != -1) {
                int shiftstartpos = offset1 + (sizeof(float) + 2 * sizeof(int));
                int length = freeSpacePointer - shiftstartpos;
                memmove((char*)arr + offset1, (char*)arr + shiftstartpos, length);
                freeSpacePointer -= (sizeof(float) + 2 * sizeof(int));
                memcpy((char*)arr + 4093, &freeSpacePointer, sizeof(short));
                totalEntries -= 1;
                memcpy((char*)arr + 4091, &totalEntries, sizeof(short));
                count = 1;
            }
        }
        else {
            int len1, len2;
            memcpy(&len1, (char*)key, sizeof(int));
            string key1;
            key1.resize(len1);
            memcpy(&key1[0], (char*)key + sizeof(int), len1);
            int offset = 0, offset1 = -1;

            for (int loop = 0; loop < totalEntries; loop++) {
                int datalen;
                memcpy(&datalen, (char*)arr + offset, sizeof(int));
                offset += sizeof(int);
                string key2;
                key2.resize(datalen);
                memcpy(&key2[0], (char*)arr + offset, datalen);
                offset += datalen;
                int pageData;
                memcpy(&pageData, (char*)arr + offset, sizeof(int));
                offset += sizeof(int);
                int slotData;
                memcpy(&slotData, (char*)arr + offset, sizeof(int));
                offset += sizeof(int);
                if ((key1.compare(key2) == 0) && (unsigned)pageData == rid.pageNum && (unsigned)slotData == rid.slotNum) {
                    offset1 = offset - 3 * sizeof(int) - datalen;
                    len2 = datalen;
                    break;
                }
            }
            if (offset1 != -1) {
                int shiftstartpos = offset1 + sizeof(int) + len2 + 2 * sizeof(int);
                int length = freeSpacePointer - shiftstartpos;
                memmove((char*)arr + offset1, (char*)arr + shiftstartpos, length);
                freeSpacePointer -= (sizeof(int) + len2 + 2 * sizeof(int));
                memcpy((char*)arr + 4093, &freeSpacePointer, sizeof(short));
                totalEntries -= 1;
                memcpy((char*)arr + 4091, &totalEntries, sizeof(short));
                count = 1;
            }
        }
        if (count == 0) {
            free(arr);
            return -1;
        }
        ixfileHandle.writePage(pageNum, arr);
        free(arr);
        return 0;
    }
}

RC IndexManager::scan(IXFileHandle& ixfileHandle,
    const Attribute& attribute,
    const void* lowKey,
    const void* highKey,
    bool lowKeyInclusive,
    bool highKeyInclusive,
    IX_ScanIterator& ix_ScanIterator)
{
    if (ixfileHandle.fileHandle.myfilehandler == NULL)
        return -1;

    ix_ScanIterator.ixfileHandle = ixfileHandle;
    ix_ScanIterator.attribute = attribute;
    ix_ScanIterator.highKey = highKey;
    ix_ScanIterator.highKeyInclusive = highKeyInclusive;
    ix_ScanIterator.arr = (char*)malloc(PAGE_SIZE);
    ix_ScanIterator.currPage = LeafPagevalue(ixfileHandle, 0, attribute, lowKey);
    if (ix_ScanIterator.currPage == -1) {
        ix_ScanIterator.isEOF = true;
        return 0;
    }
    ixfileHandle.readPage(ix_ScanIterator.currPage, ix_ScanIterator.arr);
    short freeSpacePointer;
    memcpy(&freeSpacePointer, (char*)ix_ScanIterator.arr + 4093, sizeof(short));
    ix_ScanIterator.freeSpacePointer = freeSpacePointer;
    short totalEntries;
    memcpy(&totalEntries, (char*)ix_ScanIterator.arr + 4091, sizeof(short));
    bool lowoffset = false;
    if (lowKey == NULL) {
        ix_ScanIterator.offset = 0;
        lowoffset = true;
    }
    bool highoffset = false;
    if (highKey == NULL) {
        ix_ScanIterator.isEOF = false;
        highoffset = true;
    }
    if (lowKey == NULL && highKey == NULL)
        return 0;
    if (ix_ScanIterator.attribute.type == TypeInt) {
        int lowKey1;
        if (lowKey != NULL)
            memcpy(&lowKey1, lowKey, sizeof(int));
        int highKey1;
        if (highKey != NULL)
            memcpy(&highKey1, highKey, sizeof(int));
        for (int loop = 0; loop < totalEntries; loop++) {
            int key;
            memcpy(&key, (char*)ix_ScanIterator.arr + (loop * 3 * sizeof(int)), sizeof(int));
            if (lowKey != NULL)
                lowoffset = lowKeyInclusive ? ((lowKey1 <= key) ? 1 : 0) : ((lowKey1 < key) ? 1 : 0);
            if (lowoffset) {
                if (lowKey != NULL)
                    ix_ScanIterator.offset = loop * 3 * sizeof(int);
                if (highKey != NULL) {
                    highoffset = highKeyInclusive ? ((key <= highKey1) ? 1 : 0) : ((key < highKey1) ? 1 : 0);
                    ix_ScanIterator.isEOF = highoffset ? false : true;
                }
                break;
            }
            else
                continue;
        }
        if (totalEntries == 0)
            ix_ScanIterator.isEOF = false;
        if (!lowoffset)
            ix_ScanIterator.isEOF = true;
    }
    else if (ix_ScanIterator.attribute.type == TypeReal) {
        float lowKey1;
        if (lowKey != NULL)
            memcpy(&lowKey1, lowKey, sizeof(float));
        float highKey1;
        if (highKey != NULL)
            memcpy(&highKey1, highKey, sizeof(float));
        for (int loop = 0; loop < totalEntries; loop++) {
            float key;
            memcpy(&key, (char*)ix_ScanIterator.arr + (loop * (sizeof(float) + 2 * sizeof(int))), sizeof(float));
            if (lowKey != NULL)
                lowoffset = lowKeyInclusive ? ((lowKey1 <= key) ? 1 : 0) : ((lowKey1 < key) ? 1 : 0);
            if (lowoffset) {
                if (lowKey != NULL)
                    ix_ScanIterator.offset = loop * (sizeof(float) + 2 * sizeof(int));
                if (highKey != NULL) {
                    highoffset = highKeyInclusive ? ((key <= highKey1) ? 1 : 0) : ((key < highKey1) ? 1 : 0);
                    ix_ScanIterator.isEOF = highoffset ? false : true;
                }
                break;
            }
            else
                continue;
        }
        if (totalEntries == 0)
            ix_ScanIterator.isEOF = false;
        if (!lowoffset)
            ix_ScanIterator.isEOF = true;
    }
    else {

        string s1;

        if (lowKey != NULL) {
            int length1;
            memcpy(&length1, lowKey, sizeof(int));
            s1.resize(length1);
            memcpy(&s1[0], (char*)lowKey + sizeof(int), length1);
        }

        string s2;
        if (highKey != NULL) {
            int length2;
            memcpy(&length2, highKey, sizeof(int));
            s2.resize(length2);
            memcpy(&s2[0], (char*)highKey + sizeof(int), length2);
        }

        int offset = 0;
        for (int loop = 0; loop < totalEntries; loop++) {
            int datalen;
            memcpy(&datalen, (char*)ix_ScanIterator.arr + offset, sizeof(int));
            string s3;
            s3.resize(datalen);
            memcpy(&s3[0], (char*)ix_ScanIterator.arr + offset + sizeof(int), datalen);
            if (lowKey != NULL)
                lowoffset = lowKeyInclusive ? ((s1.compare(s3) <= 0) ? 1 : 0) : ((s1.compare(s3) < 0) ? 1 : 0);
            if (lowoffset) {
                if (highKey != NULL) {
                    highoffset = highKeyInclusive ? ((s3.compare(s2) <= 0) ? 1 : 0) : ((s3.compare(s2) < 0) ? 1 : 0);
                    ;
                    ix_ScanIterator.isEOF = highoffset ? false : true;
                }
                if (lowKey != NULL)
                    ix_ScanIterator.offset = offset;

                break;
            }
            else {
                offset += sizeof(int) + datalen + 2 * sizeof(int);
                continue;
            }
        }
        if (totalEntries == 0)
            ix_ScanIterator.isEOF = false;

        if (!lowoffset)
            ix_ScanIterator.isEOF = true;
    }
    return 0;
}

int IndexManager::LeafPagevalue(IXFileHandle& ixfileHandle, int nodePageNum, const Attribute& attribute, const void* key)
{
    char* arr1 = (char*)malloc(PAGE_SIZE);
    ixfileHandle.readPage(nodePageNum, arr1);
    bool isLeaf;
    memcpy(&isLeaf, (char*)arr1 + 4095, sizeof(bool));

    if (isLeaf) {
        short freeSpacePointer;
        memcpy(&freeSpacePointer, (char*)arr1 + 4093, sizeof(short));
        if (freeSpacePointer == 0) {
            int nextPage;
            memcpy(&nextPage, (char*)arr1 + 4087, sizeof(int));
            RC rc = -1;
            if (nextPage != -1) {
                rc = LeafPagevalue(ixfileHandle, nextPage, attribute, key);
            }
            free(arr1);
            return rc;
        }
        free(arr1);
        return nodePageNum;
    }
    else {
        int newpageNum;
        if (key == NULL)
            memcpy(&newpageNum, arr1, 4);
        else
            findKey(attribute, arr1, key, newpageNum);
        RC rc = LeafPagevalue(ixfileHandle, newpageNum, attribute, key);
        free(arr1);
        return rc;
    }
}

RC IX_ScanIterator::getNextEntry(RID& rid, void* key)
{
    int noOfPages = ixfileHandle.getNumberOfPages();
    if (isEOF) {
        return -1;
    }

    while (offset >= freeSpacePointer) {

        void* arr2 = malloc(PAGE_SIZE);
        ixfileHandle.readPage(currPage, arr2);
        memcpy(&currPage, (char*)arr2 + 4087, sizeof(int));

        if (currPage >= noOfPages || currPage < 0) {
            isEOF = true;
            return -1;
        }
        offset = 0;

        ixfileHandle.readPage(currPage, arr);
        short freeSpacePointer1;
        memcpy(&freeSpacePointer1, (char*)arr + 4093, sizeof(short));
        freeSpacePointer = freeSpacePointer1;
        free(arr2);
    }

    bool isNullHK = false;
    bool success = false;

    if (!highKey) {
        isNullHK = true;
        success = true;
    }
    if (attribute.type == TypeInt) {
        if (!isNullHK) {
            int key1;
            memcpy(&key1, (char*)arr + offset, sizeof(int));

            int highKey1;
            memcpy(&highKey1, (char*)highKey, sizeof(int));

            success = highKeyInclusive ? (key1 <= highKey1) : (key1 < highKey1);
        }
        if (!success) {
            isEOF = true;
            return -1;
        }

        memcpy(key, (char*)arr + offset, sizeof(int));
        offset += sizeof(int);
        int pageNum;
        memcpy(&pageNum, (char*)arr + offset, sizeof(int));
        offset += sizeof(int);
        int slotNum;
        memcpy(&slotNum, (char*)arr + offset, sizeof(int));
        offset += sizeof(int);

        rid.pageNum = pageNum;
        rid.slotNum = slotNum;
    }
    else if (attribute.type == TypeReal) {
        if (!isNullHK) {
            float key1;
            memcpy(&key1, (char*)arr + offset, sizeof(int));

            float highKey1;
            memcpy(&highKey1, (char*)highKey, sizeof(int));

            success = highKeyInclusive ? (key1 <= highKey1) : (key1 < highKey1);
        }
        if (!success) {
            isEOF = true;
            return -1;
        }

        memcpy(key, (char*)arr + offset, sizeof(int));
        offset += sizeof(int);
        int pageNum;
        memcpy(&pageNum, (char*)arr + offset, sizeof(int));
        offset += sizeof(int);
        int slotNum;
        memcpy(&slotNum, (char*)arr + offset, sizeof(int));
        offset += sizeof(int);

        rid.pageNum = pageNum;
        rid.slotNum = slotNum;
    }
    else {

        int len;
        memcpy(&len, (char*)arr + offset, sizeof(int));
        offset += sizeof(int);

        if (!isNullHK) {
            int len1;
            memcpy(&len1, (char*)highKey, sizeof(int));

            //string maximum length fails so have to use char *
            //string highKey1;
            //highKey1.resize(len1);
            char* highKey1 = (char*)malloc(PAGE_SIZE);
            memcpy(highKey1, (char*)highKey + sizeof(int), len1);
            highKey1[len1] = '\0';
            //string key1;
            //key1.resize(len);
            char* key1 = (char*)malloc(PAGE_SIZE);
            memcpy(key1, (char*)arr + offset, len);
            key1[len] = '\0';

            //success = highKeyInclusive?(key1.compare(highKey1) <= 0): (key1.compare(highKey1) < 0);
            success = highKeyInclusive ? (string(key1) <= string(highKey1)) : (string(key1) < string(highKey1));

            free(highKey1);
            free(key1);
        }
        if (!success) {
            isEOF = true;
            return -1;
        }

        memcpy(key, &len, sizeof(int));
        memcpy((char*)key + sizeof(int), (char*)arr + offset, len);
        offset += len;
        int pageNum;
        memcpy(&pageNum, (char*)arr + offset, sizeof(int));
        offset += sizeof(int);
        int slotNum;
        memcpy(&slotNum, (char*)arr + offset, sizeof(int));
        offset += sizeof(int);

        rid.pageNum = pageNum;
        rid.slotNum = slotNum;
    }

    return 0;
}

bool pairSort(const pair<int, int>& value1, const pair<int, int>& value2)
{
    if (value1.first < value2.first)
        return true;
    if (value1.first == value2.first) {
        if (value1.second <= value2.second)
            return true;
        return false;
    }
    return false;
}

RC IndexManager::insertEntry(IXFileHandle& ixfileHandle, const Attribute& attribute, const void* key, const RID& rid)
{
    int noOfPages = ixfileHandle.getNumberOfPages();

    int keyLength = 0;
    if (attribute.type == TypeInt) {
        keyLength = sizeof(int);
    }
    else if (attribute.type == TypeReal) {
        keyLength = sizeof(float);
    }
    else {
        int length = 0;
        memcpy(&length, (char*)key, sizeof(int));
        keyLength = sizeof(int) + length;
    }

    if (noOfPages == 0) {
        bool isLeaf = true;
        void* arr = malloc(PAGE_SIZE);

        int nextPointer = -1;
        memcpy((char*)arr + 4095, &isLeaf, sizeof(bool));
        short freeSpacePointer = 0;
        memcpy((char*)arr + 4093, &freeSpacePointer, sizeof(short));
        short noOfRecords = 0;
        memcpy((char*)arr + 4091, &noOfRecords, sizeof(short));
        memcpy((char*)arr + 4087, &nextPointer, sizeof(int));

        insertLeafRecord(ixfileHandle, arr, 0, attribute, key, rid, keyLength);
        free(arr);
        return 0;
    }
    if (noOfPages == 1) {
        void* arr = malloc(PAGE_SIZE);
        ixfileHandle.readPage(0, arr);

        short noOfRecords;
        memcpy(&noOfRecords, (char*)arr + 4091, sizeof(short));
        if (attribute.type == TypeInt) {
            int key1;
            memcpy(&key1, (char*)key, sizeof(int));
            for (int i = 0; i < noOfRecords; i++) {
                int offset = i * (3 * sizeof(int));
                int key2;
                memcpy(&key2, (char*)arr + offset, sizeof(int));
                offset += sizeof(int);
                int pageNum;
                memcpy(&pageNum, (char*)arr + offset, sizeof(int));
                offset += sizeof(int);
                int slotNum;
                memcpy(&slotNum, (char*)arr + offset, sizeof(int));
                if (key1 == key2 && rid.pageNum == (unsigned)pageNum && rid.slotNum == (unsigned)slotNum) {
                    free(arr);
                    return -1;
                }
            }
        }
        else if (attribute.type == TypeReal) {
            float key1;
            memcpy(&key1, (char*)key, sizeof(float));
            for (int i = 0; i < noOfRecords; i++) {
                int offset = i * (sizeof(float) + 2 * sizeof(int));
                float key2;
                memcpy(&key2, (char*)arr + offset, sizeof(float));
                offset += sizeof(float);
                int pageNum;
                memcpy(&pageNum, (char*)arr + offset, sizeof(int));
                offset += sizeof(int);
                int slotNum;
                memcpy(&slotNum, (char*)arr + offset, sizeof(int));
                if (key1 == key2 && rid.pageNum == (unsigned)pageNum && rid.slotNum == (unsigned)slotNum) {
                    free(arr);
                    return -1;
                }
            }
        }
        else {
            int len;
            memcpy(&len, (char*)key, sizeof(int));
            string key1;
            key1.resize(len);
            memcpy(&key1[0], (char*)key + sizeof(int), len);
            int offset = 0;
            for (int i = 0; i < noOfRecords; i++) {
                len = 0;
                memcpy(&len, (char*)arr + offset, sizeof(int));
                offset += sizeof(int);
                string key2;
                key2.resize(len);
                memcpy(&key2[0], (char*)arr + offset, len);
                offset += len;
                int pageNum;
                memcpy(&pageNum, (char*)arr + offset, sizeof(int));
                offset += sizeof(int);
                int slotNum;
                memcpy(&slotNum, (char*)arr + offset, sizeof(int));
                offset += sizeof(int);
                if (key1.compare(key2) == 0 && rid.pageNum == (unsigned)pageNum && rid.slotNum == (unsigned)slotNum) {
                    free(arr);
                    return -1;
                }
            }
        }
        bool isLeaf;
        memcpy(&isLeaf, (char*)arr + 4095, sizeof(bool));
        if (!isLeaf) {
            free(arr);
            return -1;
        }

        short freeSpacePointer;
        memcpy(&freeSpacePointer, (char*)arr + 4093, sizeof(short));
        short freeSpace;
        short totalBytesNeeded;

        freeSpace = PAGE_SIZE - freeSpacePointer - LSIZE;
        totalBytesNeeded = keyLength + RID_SIZE;

        if (freeSpace >= totalBytesNeeded) {
            insertLeafRecord(ixfileHandle, arr, 0, attribute, key, rid, keyLength);
        }
        else {
            void* newKey = malloc(PAGE_SIZE);
            int newPageId = -1;
            splitLeafPage(ixfileHandle, arr, 0, attribute, key, rid, keyLength, newKey, newPageId);
            void* arr1 = malloc(PAGE_SIZE);
            int leftPointer = ixfileHandle.getNumberOfPages(), rightPointer = newPageId;

            short freeSpacePointer = 0;
            if (attribute.type == TypeInt) {
                int offset = 0;
                memcpy((char*)arr1 + offset, &leftPointer, sizeof(int));
                offset += sizeof(int);
                memcpy((char*)arr1 + offset, (char*)newKey, sizeof(int));
                offset += sizeof(int);
                memcpy((char*)arr1 + offset, &rightPointer, sizeof(int));
                offset += sizeof(int);
                freeSpacePointer = offset;
            }
            else if (attribute.type == TypeReal) {
                int offset = 0;
                memcpy((char*)arr1, &leftPointer, sizeof(int));
                offset += sizeof(int);
                memcpy((char*)arr1 + offset, (char*)newKey, sizeof(float));
                offset += sizeof(float);
                memcpy((char*)arr1 + offset, &rightPointer, sizeof(int));
                offset += sizeof(int);
                freeSpacePointer = offset;
            }
            else {
                int offset = 0;
                memcpy((char*)arr1 + offset, &leftPointer, sizeof(int));
                offset += sizeof(int);
                int len;
                memcpy(&len, (char*)newKey, sizeof(int));
                memcpy((char*)arr1 + offset, (char*)newKey, sizeof(int) + len);
                offset += sizeof(int) + len;
                memcpy((char*)arr1 + offset, &rightPointer, sizeof(int));
                offset += sizeof(int);
                freeSpacePointer = offset;
            }
            bool isLeaf = false;
            memcpy((char*)arr1 + 4095, &isLeaf, sizeof(bool));
            memcpy((char*)arr1 + 4093, &freeSpacePointer, sizeof(short));
            short noOfRecords = 1;
            memcpy((char*)arr1 + 4091, &noOfRecords, sizeof(short));

            void* oldLeaf = malloc(PAGE_SIZE);
            ixfileHandle.readPage(0, oldLeaf);
            ixfileHandle.writePage(0, arr1);
            ixfileHandle.appendPage(oldLeaf);
            free(arr1);
            free(oldLeaf);
            free(newKey);
        }
        free(arr);
        return 0;
    }
    void* newKey = malloc(PAGE_SIZE);
    int newPageId = -1;
    int success = insertUtil(ixfileHandle, 0, attribute, key, rid, keyLength, newKey, newPageId);
    free(newKey);
    return success;
}

RC IndexManager::insertUtil(IXFileHandle& ixfileHandle, int pageNumber, const Attribute& attribute,
    const void* key, const RID& rid, int keyLength, void* newKey, int& newPageId)
{
    void* arr = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pageNumber, arr);
    bool isLeaf;
    memcpy(&isLeaf, (char*)arr + 4095, sizeof(bool));

    if (!isLeaf) {
        int pagePointer;
        findKey(attribute, arr, key, pagePointer);
        insertUtil(ixfileHandle, pagePointer, attribute, key, rid, keyLength, newKey, newPageId);
        if (newPageId == -1) {
            free(arr);
            return 0;
        }
        else {

            short freeSpacePointer;
            memcpy(&freeSpacePointer, (char*)arr + 4093, sizeof(short));
            short freeSpace;
            short totalBytesNeeded;

            freeSpace = 4096 - freeSpacePointer - NSIZE;
            totalBytesNeeded = keyLength + sizeof(int);

            if (freeSpace >= totalBytesNeeded) {
                insertNonLeafRecord(ixfileHandle, arr, pageNumber, attribute, newKey, newPageId);
                newPageId = -1;
            }
            else {
                int noOfPages = ixfileHandle.getNumberOfPages();
                short freeSpacePointer;
                memcpy(&freeSpacePointer, (char*)arr + 4093, sizeof(short));
                short noOfRecords;
                memcpy(&noOfRecords, (char*)arr + 4091, sizeof(short));
                short d = noOfRecords / 2;

                short newFreeSpacePointer;

                if (attribute.type == TypeInt) {
                    newFreeSpacePointer = sizeof(int) + (d * 8);
                }
                else if (attribute.type == TypeReal) {
                    newFreeSpacePointer = sizeof(int) + (d * 8);
                }
                else {
                    int offset = sizeof(int);
                    for (int var = 0; var < d; ++var) {
                        int len;
                        memcpy(&len, (char*)arr + offset, sizeof(int));
                        offset += (2 * sizeof(int)) + len;
                    }
                    newFreeSpacePointer = offset;
                }
                memcpy((char*)arr + 4093, &newFreeSpacePointer, sizeof(short));
                memcpy((char*)arr + 4091, &d, sizeof(short));
                ixfileHandle.writePage(pageNumber, arr);

                int bytesMove = freeSpacePointer - newFreeSpacePointer;
                void* arr2 = malloc(PAGE_SIZE);
                memcpy((char*)arr2 + sizeof(int), (char*)arr + newFreeSpacePointer, bytesMove);
                bool isLeaf = false;
                memcpy((char*)arr2 + 4095, &isLeaf, sizeof(bool));
                int nullPage = -1;
                memcpy(arr2, &nullPage, sizeof(int));
                short updatedFreeSpacePointer = freeSpacePointer - newFreeSpacePointer + sizeof(int);
                memcpy((char*)arr2 + 4093, &updatedFreeSpacePointer, sizeof(short));
                short updatedNoOfRecords = noOfRecords - d;
                memcpy((char*)arr2 + 4091, &updatedNoOfRecords, sizeof(short));
                ixfileHandle.appendPage(arr2);

                if (attribute.type == TypeInt) {
                    int key2;
                    memcpy(&key2, (char*)arr2 + sizeof(int), sizeof(int));
                    int key1;
                    memcpy(&key1, newKey, sizeof(int));

                    if (key1 >= key2) {
                        insertNonLeafRecord(ixfileHandle, arr2, noOfPages, attribute, newKey, newPageId);
                    }
                    else {
                        insertNonLeafRecord(ixfileHandle, arr, pageNumber, attribute, newKey, newPageId);
                    }
                }
                else if (attribute.type == TypeReal) {
                    float key2;
                    memcpy(&key2, (char*)arr2 + sizeof(int), sizeof(float));
                    float key1;
                    memcpy(&key1, newKey, sizeof(float));
                    if (key1 >= key2) {
                        insertNonLeafRecord(ixfileHandle, arr2, noOfPages, attribute, newKey, newPageId);
                    }
                    else {
                        insertNonLeafRecord(ixfileHandle, arr, pageNumber, attribute, newKey, newPageId);
                    }
                }
                else {
                    int len2;
                    string key2;
                    memcpy(&len2, (char*)arr2 + sizeof(int), sizeof(int));
                    key2.resize(len2);
                    memcpy(&key2[0], (char*)arr2 + sizeof(int) + sizeof(int), len2);
                    int len1;
                    memcpy(&len1, (char*)newKey, sizeof(int));
                    string key1;
                    key1.resize(len1);
                    memcpy(&key1[0], (char*)newKey + sizeof(int), len1);

                    if (key1.compare(key2) >= 0) {
                        insertNonLeafRecord(ixfileHandle, arr2, noOfPages, attribute, newKey, newPageId);
                    }
                    else {
                        insertNonLeafRecord(ixfileHandle, arr, pageNumber, attribute, newKey, newPageId);
                    }
                }

                if (attribute.type == TypeInt) {
                    memcpy((char*)newKey, (char*)arr2 + sizeof(int), sizeof(int));
                }
                else if (attribute.type == TypeReal) {
                    memcpy((char*)newKey, (char*)arr2 + sizeof(int), sizeof(float));
                }
                else {
                    int keyLength;
                    memcpy(&keyLength, (char*)arr2 + sizeof(int), sizeof(int));
                    memcpy((char*)newKey, &keyLength, sizeof(int));
                    memcpy((char*)newKey + sizeof(int), (char*)arr2 + sizeof(int) + sizeof(int), keyLength);
                }
                newPageId = noOfPages;

                short freeSpacePointer1;
                memcpy(&freeSpacePointer1, (char*)arr2 + 4093, sizeof(short));

                if (attribute.type == TypeInt) {
                    int start = 2 * sizeof(int);
                    memmove((char*)arr2, (char*)arr2 + start, freeSpacePointer1 - start);
                    freeSpacePointer1 -= start;
                }
                else if (attribute.type == TypeReal) {
                    int start = sizeof(int) + sizeof(float);
                    memmove((char*)arr2, (char*)arr2 + start, freeSpacePointer1 - start);
                    freeSpacePointer1 -= start;
                }
                else {
                    int keyLength;
                    memcpy(&keyLength, (char*)arr2 + sizeof(int), sizeof(int));
                    int start = sizeof(int) + sizeof(int) + keyLength;
                    memmove((char*)arr2, (char*)arr2 + start, freeSpacePointer1 - start);
                    freeSpacePointer1 -= start;
                }
                memcpy((char*)arr2 + 4093, &freeSpacePointer1, sizeof(short));
                short noOfRecords1;
                memcpy(&noOfRecords1, (char*)arr2 + 4091, sizeof(short));
                noOfRecords1 -= 1;
                memcpy((char*)arr2 + 4091, &noOfRecords1, sizeof(short));
                ixfileHandle.writePage(noOfPages, arr2);
                free(arr2);
                if (pageNumber == 0) {

                    void* arr2 = malloc(PAGE_SIZE);
                    int leftPointer = ixfileHandle.getNumberOfPages();
                    int rightPointer = newPageId;
                    short offset = 0;
                    memcpy((char*)arr2 + offset, &leftPointer, sizeof(int));
                    offset += sizeof(int);
                    if (attribute.type == TypeInt) {
                        memcpy((char*)arr2 + offset, newKey, sizeof(int));
                        offset += sizeof(int);
                    }
                    else if (attribute.type == TypeReal) {
                        memcpy((char*)arr2 + offset, newKey, sizeof(float));
                        offset += sizeof(int);
                    }
                    else {
                        int keyLength;
                        memcpy(&keyLength, newKey, sizeof(int));
                        memcpy((char*)arr2 + sizeof(int), newKey, sizeof(int) + keyLength);
                        offset += sizeof(int) + keyLength;
                    }
                    memcpy((char*)arr2 + offset, &rightPointer, sizeof(int));
                    offset += sizeof(int);
                    bool isLeaf = false;
                    memcpy((char*)arr2 + 4095, &isLeaf, sizeof(bool));
                    memcpy((char*)arr2 + 4093, &offset, sizeof(short));
                    short numEntries = 1;
                    memcpy((char*)arr2 + 4091, &numEntries, sizeof(short));

                    ixfileHandle.writePage(pageNumber, arr2);
                    ixfileHandle.appendPage(arr);

                    free(arr2);
                }
            }
        }
    }
    else {
        short noOfRecords;
        memcpy(&noOfRecords, (char*)arr + 4091, sizeof(short));
        if (attribute.type == TypeInt) {
            int key1;
            memcpy(&key1, (char*)key, sizeof(int));
            for (int i = 0; i < noOfRecords; i++) {
                int offset = i * (3 * sizeof(int));
                int key2;
                memcpy(&key2, (char*)arr + offset, sizeof(int));
                offset += sizeof(int);
                int pageNum;
                memcpy(&pageNum, (char*)arr + offset, sizeof(int));
                offset += sizeof(int);
                int slotNum;
                memcpy(&slotNum, (char*)arr + offset, sizeof(int));
                if (key1 == key2 && rid.pageNum == (unsigned)pageNum && rid.slotNum == (unsigned)slotNum) {
                    free(arr);
                    return -1;
                }
            }
        }
        else if (attribute.type == TypeReal) {
            float key1;
            memcpy(&key1, (char*)key, sizeof(float));
            for (int i = 0; i < noOfRecords; i++) {
                int offset = i * (sizeof(float) + 2 * sizeof(int));
                float key2;
                memcpy(&key2, (char*)arr + offset, sizeof(float));
                offset += sizeof(float);
                int pageNum;
                memcpy(&pageNum, (char*)arr + offset, sizeof(int));
                offset += sizeof(int);
                int slotNum;
                memcpy(&slotNum, (char*)arr + offset, sizeof(int));
                if (key1 == key2 && rid.pageNum == (unsigned)pageNum && rid.slotNum == (unsigned)slotNum) {
                    free(arr);
                    return -1;
                }
            }
        }
        else {
            int len;
            memcpy(&len, (char*)key, sizeof(int));
            string key1;
            key1.resize(len);
            memcpy(&key1[0], (char*)key + sizeof(int), len);
            int offset = 0;
            for (int i = 0; i < noOfRecords; i++) {
                len = 0;
                memcpy(&len, (char*)arr + offset, sizeof(int));
                string key2;
                key2.resize(len);

                offset += sizeof(int);
                memcpy(&key2[0], (char*)arr + offset, len);
                offset += len;
                int pageNum;
                memcpy(&pageNum, (char*)arr + offset, sizeof(int));
                offset += sizeof(int);
                int slotNum;
                memcpy(&slotNum, (char*)arr + offset, sizeof(int));
                offset += sizeof(int);
                if (key1.compare(key2) && rid.pageNum == (unsigned)pageNum && rid.slotNum == (unsigned)slotNum) {
                    free(arr);
                    return -1;
                }
            }
        }

        short freeSpacePointer;
        memcpy(&freeSpacePointer, (char*)arr + 4093, sizeof(short));
        short freeSpace;
        short totalBytesNeeded;

        freeSpace = PAGE_SIZE - freeSpacePointer - LSIZE;
        totalBytesNeeded = keyLength + RID_SIZE;

        if (freeSpace >= totalBytesNeeded) {
            insertLeafRecord(ixfileHandle, arr, pageNumber, attribute, key, rid, keyLength);
        }
        else {
            splitLeafPage(ixfileHandle, arr, pageNumber, attribute, key, rid, keyLength, newKey, newPageId);
        }
    }
    free(arr);
    return 0;
}

RC IndexManager::insertNonLeafRecord(IXFileHandle& ixfileHandle, void* arr, int pageNumber, const Attribute& attribute, void* newKey, int& newPageId)
{
    short freeSpacePointer;
    memcpy(&freeSpacePointer, (char*)arr + 4093, sizeof(short));
    short noOfRecords;
    memcpy(&noOfRecords, (char*)arr + 4091, sizeof(short));
    if (attribute.type == TypeInt) {
        int key1;
        int index = noOfRecords;
        memcpy(&key1, (char*)newKey, sizeof(int));
        for (int i = 0; i < noOfRecords; i++) {
            int offset = sizeof(int) + i * 2 * sizeof(int);
            int key2;
            memcpy(&key2, (char*)arr + offset, sizeof(int));
            if (key1 > key2) {
                continue;
            }
            else {
                index = i;
                break;
            }
        }
        if (index >= noOfRecords) {
            memcpy((char*)arr + freeSpacePointer, &key1, sizeof(int));
            memcpy((char*)arr + freeSpacePointer + sizeof(int), &newPageId, sizeof(int));
        }
        else {
            short offset = sizeof(int) + index * (sizeof(int) + sizeof(int));
            short shiftStartByte = offset + 2 * sizeof(int);
            short totalBytesShifted = freeSpacePointer - offset;
            memmove((char*)arr + shiftStartByte, (char*)arr + offset, totalBytesShifted);
            memcpy((char*)arr + offset, &key1, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)arr + offset, &newPageId, sizeof(int));
        }
        freeSpacePointer += 2 * sizeof(int);
        memcpy((char*)arr + 4093, &freeSpacePointer, sizeof(short));
        noOfRecords += 1;
        memcpy((char*)arr + 4091, &noOfRecords, sizeof(short));
    }
    else if (attribute.type == TypeReal) {
        float key1;
        int index = noOfRecords;
        memcpy(&key1, (char*)newKey, sizeof(float));
        for (int i = 0; i < noOfRecords; i++) {
            int offset = sizeof(int) + i * (sizeof(float) + sizeof(int));
            float key2;
            memcpy(&key2, (char*)arr + offset, sizeof(float));
            if (key1 > key2) {
                continue;
            }
            else {
                index = i;
                break;
            }
        }
        if (index >= noOfRecords) {
            memcpy((char*)arr + freeSpacePointer, &key1, sizeof(float));
            memcpy((char*)arr + freeSpacePointer + sizeof(float), &newPageId, sizeof(int));
        }
        else {
            short offset = sizeof(int) + index * (sizeof(float) + sizeof(int));
            short shiftStartByte = offset + sizeof(float) + sizeof(int);
            short totalBytesShifted = freeSpacePointer - offset;
            memmove((char*)arr + shiftStartByte, (char*)arr + offset, totalBytesShifted);
            memcpy((char*)arr + offset, &key1, sizeof(float));
            offset += sizeof(float);
            memcpy((char*)arr + offset, &newPageId, sizeof(int));
        }
        freeSpacePointer += sizeof(float) + sizeof(int);
        memcpy((char*)arr + 4093, &freeSpacePointer, sizeof(short));
        noOfRecords += 1;
        memcpy((char*)arr + 4091, &noOfRecords, sizeof(short));
    }
    else {
        int offset = 0;
        int len;
        memcpy(&len, (char*)newKey, sizeof(int));
        offset += sizeof(int);
        string key1;
        key1.resize(len);
        memcpy(&key1[0], (char*)newKey + offset, len);
        int insertOffset = freeSpacePointer;
        short shiftStartByte;
        short totalBytesShifted;
        for (int i = 0; i < noOfRecords; i++) {
            int len2;
            memcpy(&len2, (char*)arr + offset, sizeof(int));
            offset += sizeof(int);
            string key2;
            key2.resize(len2);
            memcpy(&key2[0], (char*)arr + offset, len2);
            offset += len2;
            offset += sizeof(int);
            if (key1.compare(key2) > 0) {
                continue;
            }
            else {
                insertOffset = offset - len2 - (2 * sizeof(int));
                shiftStartByte = insertOffset + len + (2 * sizeof(int));
                totalBytesShifted = freeSpacePointer - insertOffset;
                break;
            }
        }
        if (insertOffset == freeSpacePointer) {
            memcpy((char*)arr + freeSpacePointer, &len, sizeof(int));
            memcpy((char*)arr + freeSpacePointer + sizeof(int), (char*)key1.c_str(), len);
            memcpy((char*)arr + freeSpacePointer + sizeof(int) + len, &newPageId, sizeof(int));
        }
        else {
            memmove((char*)arr + shiftStartByte, (char*)arr + insertOffset, totalBytesShifted);
            memcpy((char*)arr + insertOffset, &len, sizeof(int));
            insertOffset += sizeof(int);
            memcpy((char*)arr + insertOffset, (char*)key1.c_str(), len);
            insertOffset += len;
            memcpy((char*)arr + insertOffset, &newPageId, sizeof(int));
        }
        freeSpacePointer += sizeof(int) + len + sizeof(int);
        memcpy((char*)arr + 4093, &freeSpacePointer, sizeof(short));
        noOfRecords += 1;
        memcpy((char*)arr + 4091, &noOfRecords, sizeof(short));
    }
    ixfileHandle.writePage(pageNumber, arr);
    return 0;
}

RC IndexManager::splitLeafPage(IXFileHandle& ixfileHandle, void* arr, int pageNumber, const Attribute& attribute,
    const void* key, const RID& rid, int keyLength, void* newKey, int& newPageId)
{
    int noOfPages = ixfileHandle.getNumberOfPages();
    short freeSpacePointer;
    memcpy(&freeSpacePointer, (char*)arr + 4093, sizeof(short));

    short noOfRecords;
    memcpy(&noOfRecords, (char*)arr + 4091, sizeof(short));

    short d = noOfRecords / 2;
    int oldNextPointer;
    memcpy(&oldNextPointer, (char*)arr + 4087, sizeof(int));

    short newFreeSpacePointer;

    if (attribute.type == TypeInt) {
        newFreeSpacePointer = d * RSIZE;
    }
    else if (attribute.type == TypeReal) {
        newFreeSpacePointer = d * RSIZE;
    }
    else {
        int offset = 0;
        for (int var = 0; var < d; ++var) {
            int len;
            memcpy(&len, (char*)arr + offset, sizeof(int));
            offset += (3 * sizeof(int)) + len;
        }
        newFreeSpacePointer = offset;
    }

    // newFreeSpacePointer needs to be updated to handle duplicate entries
    if (attribute.type == TypeInt) {
        int current;
        memcpy(&current, (char*)arr + newFreeSpacePointer, sizeof(int));
        int leftKey;
        memcpy(&leftKey, (char*)arr + newFreeSpacePointer - RSIZE, sizeof(int));

        if (current == leftKey) {
            int leftDuplicates = 1;
            short leftOffset = newFreeSpacePointer - RSIZE;
            int leftRecords = noOfRecords / 2;
            for (int i = 1; i < leftRecords; ++i) {
                int key;
                memcpy(&key, (char*)arr + newFreeSpacePointer - ((i + 1) * RSIZE), sizeof(int));
                if (key == current) {
                    leftDuplicates++;
                    leftOffset -= RSIZE;
                }
                else {
                    break;
                }
            }
            int rightDuplicates = 1;
            short rightOffset = newFreeSpacePointer + RSIZE;
            int rightRecords = noOfRecords - leftRecords;
            for (int i = 1; i < rightRecords; ++i) {
                int key;
                memcpy(&key, (char*)arr + newFreeSpacePointer + (i * RSIZE), sizeof(int));
                if (key == current) {
                    rightDuplicates++;
                    rightOffset += RSIZE;
                }
                else {
                    break;
                }
            }
            if (leftDuplicates < rightDuplicates) {
                newFreeSpacePointer = leftOffset;
                d = d - leftDuplicates;
            }
            else {
                newFreeSpacePointer = rightOffset;
                d = d + rightDuplicates;
            }
        }
    }
    else if (attribute.type == TypeReal) {
        float current;
        memcpy(&current, (char*)arr + newFreeSpacePointer, sizeof(int));
        float leftKey;
        memcpy(&leftKey, (char*)arr + newFreeSpacePointer - RSIZE, sizeof(int));

        if (current == leftKey) {
            int leftDuplicates = 1;
            short leftOffset = newFreeSpacePointer - RSIZE;
            int leftRecords = noOfRecords / 2;
            for (int i = 1; i < leftRecords; ++i) {
                float key;
                memcpy(&key, (char*)arr + newFreeSpacePointer - ((i + 1) * RSIZE), sizeof(int));
                if (key == current) {
                    leftDuplicates++;
                    leftOffset -= RSIZE;
                }
                else {
                    break;
                }
            }
            int rightDuplicates = 1;
            short rightOffset = newFreeSpacePointer + RSIZE;
            int rightRecords = noOfRecords - leftRecords;
            for (int i = 1; i < rightRecords; ++i) {
                float key;
                memcpy(&key, (char*)arr + newFreeSpacePointer + (i * RSIZE), sizeof(int));
                if (key == current) {
                    rightDuplicates++;
                    rightOffset += RSIZE;
                }
                else {
                    break;
                }
            }
            if (leftDuplicates < rightDuplicates) {
                newFreeSpacePointer = leftOffset;
                d = d - leftDuplicates;
            }
            else {
                newFreeSpacePointer = rightOffset;
                d = d + rightDuplicates;
            }
        }
    }
    else {
        int len1;
        memcpy(&len1, (char*)arr + newFreeSpacePointer, sizeof(int));
        string current;
        current.resize(len1);
        memcpy(&current[0], (char*)arr + newFreeSpacePointer + sizeof(int), len1);

        int leftDuplicates = 0;
        int offset = 0;
        bool leftDuplicateFound = false;
        int leftOffset;
        while (offset < newFreeSpacePointer) {
            int len2;
            memcpy(&len2, (char*)arr + offset, sizeof(int));
            string leftKey;
            leftKey.resize(len2);
            offset += sizeof(int);
            memcpy(&leftKey[0], (char*)arr + offset, len2);
            offset += len2;
            if (current.compare(leftKey) == 0) {
                if (!leftDuplicateFound) {
                    leftDuplicateFound = true;
                    leftOffset = offset - sizeof(int) - len2;
                }
                leftDuplicates++;
            }
            offset += RID_SIZE;
        }

        if (leftDuplicates > 0) {
            int rightDuplicates = 1;
            short rightOffset = newFreeSpacePointer + (sizeof(int) + len1) + RID_SIZE;
            int rightRecords = noOfRecords - noOfRecords / 2;
            for (int i = 1; i < rightRecords; ++i) {
                int offset = newFreeSpacePointer + i * (sizeof(int) + len1 + RID_SIZE) + sizeof(int);
                string key;
                key.resize(len1);
                memcpy(&key[0], (char*)arr + offset, len1);
                if (key.compare(current) == 0) {
                    rightDuplicates++;
                    rightOffset += (sizeof(int) + len1) + RID_SIZE;
                }
                else {
                    break;
                }
            }
            if (leftDuplicates < rightDuplicates) {
                newFreeSpacePointer = leftOffset;
                d = d - leftDuplicates;
            }
            else {
                newFreeSpacePointer = rightOffset;
                d = d + rightDuplicates;
            }
        }
    }
    memcpy((char*)arr + 4093, &newFreeSpacePointer, sizeof(short));
    memcpy((char*)arr + 4091, &d, sizeof(short));
    memcpy((char*)arr + 4087, &noOfPages, sizeof(int));

    ixfileHandle.writePage(pageNumber, arr);

    void* arr2 = malloc(PAGE_SIZE);
    memcpy((char*)arr2, (char*)arr + newFreeSpacePointer, (freeSpacePointer - newFreeSpacePointer));
    bool isLeaf = true;
    memcpy((char*)arr2 + 4095, &isLeaf, sizeof(bool));
    short updatedFreeSpacePointer = freeSpacePointer - newFreeSpacePointer;
    memcpy((char*)arr2 + 4093, &updatedFreeSpacePointer, sizeof(short));
    short updatedNoOfRecords = noOfRecords - d;
    memcpy((char*)arr2 + 4091, &updatedNoOfRecords, sizeof(short));
    memcpy((char*)arr2 + 4087, &oldNextPointer, sizeof(int));
    ixfileHandle.appendPage(arr2);

    if (updatedNoOfRecords == 0) {
        insertLeafRecord(ixfileHandle, arr2, noOfPages, attribute, key, rid, keyLength);
    }
    else {
        if (attribute.type == TypeInt) {
            int key2;
            memcpy(&key2, arr2, sizeof(int));
            int key1;
            memcpy(&key1, key, sizeof(int));

            if (key1 >= key2) {
                insertLeafRecord(ixfileHandle, arr2, noOfPages, attribute, key, rid, keyLength);
            }
            else {
                insertLeafRecord(ixfileHandle, arr, pageNumber, attribute, key, rid, keyLength);
            }
        }
        else if (attribute.type == TypeReal) {
            float key2;
            memcpy(&key2, arr2, sizeof(float));
            float key1;
            memcpy(&key1, key, sizeof(float));
            if (key1 >= key2) {
                insertLeafRecord(ixfileHandle, arr2, noOfPages, attribute, key, rid, keyLength);
            }
            else {
                insertLeafRecord(ixfileHandle, arr, pageNumber, attribute, key, rid, keyLength);
            }
        }
        else {
            int len2;
            string key2;
            memcpy(&len2, arr2, sizeof(int));
            key2.resize(len2);
            memcpy(&key2[0], (char*)arr2 + sizeof(int), len2);
            int len1;
            memcpy(&len1, (char*)key, sizeof(int));
            string key1;
            key1.resize(len1);
            memcpy(&key1[0], (char*)key + sizeof(int), len1);

            if (key1.compare(key2) >= 0) {
                insertLeafRecord(ixfileHandle, arr2, noOfPages, attribute, key, rid, keyLength);
            }
            else {
                insertLeafRecord(ixfileHandle, arr, pageNumber, attribute, key, rid, keyLength);
            }
        }
    }

    if (attribute.type == TypeInt) {
        memcpy(newKey, arr2, sizeof(int));
    }
    else if (attribute.type == TypeReal) {
        memcpy(newKey, arr2, sizeof(int));
    }
    else {
        int keyLength;
        memcpy(&keyLength, arr2, sizeof(int));
        memcpy((char*)newKey, &keyLength, sizeof(int));
        memcpy((char*)newKey + sizeof(int), (char*)arr2 + sizeof(int), keyLength);
    }
    newPageId = noOfPages;
    free(arr2);
    return 0;
}

RC IndexManager::insertLeafRecord(IXFileHandle& ixfileHandle, void* arr, int pageNumber, const Attribute attribute,
    const void* key, const RID& rid, int keyLength)
{
    short freeSpacePointer;
    memcpy(&freeSpacePointer, (char*)arr + 4093, sizeof(short));
    short noOfRecords;
    memcpy(&noOfRecords, (char*)arr + 4091, sizeof(short));

    if (attribute.type == TypeInt) {
        int key1;
        int index = noOfRecords;
        memcpy(&key1, (char*)key, sizeof(int));
        for (int i = 0; i < noOfRecords; i++) {
            int offset = i * (sizeof(int) + 2 * sizeof(int));
            int key2;
            memcpy(&key2, (char*)arr + offset, sizeof(int));
            if (key1 > key2) {
                continue;
            }
            else {
                index = i;
                break;
            }
        }
        if (index >= noOfRecords) {
            short offset = freeSpacePointer;
            memcpy((char*)arr + offset, &key1, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)arr + offset, &rid.pageNum, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)arr + offset, &rid.slotNum, sizeof(int));
        }
        else {
            short offset = (sizeof(int) + 2 * sizeof(int)) * index;
            short startShiftByte = offset + sizeof(int) + 2 * (sizeof(int));
            short totalBytesShifted = freeSpacePointer - offset;
            memmove((char*)arr + startShiftByte, (char*)arr + offset, totalBytesShifted);
            memcpy((char*)arr + offset, &key1, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)arr + offset, &rid.pageNum, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)arr + offset, &rid.slotNum, sizeof(int));
        }
        freeSpacePointer += sizeof(int) + 2 * sizeof(int);
        memcpy((char*)arr + 4093, &freeSpacePointer, sizeof(short));
        noOfRecords += 1;
        memcpy((char*)arr + 4091, &noOfRecords, sizeof(short));
    }
    else if (attribute.type == TypeReal) {
        float key1;
        int index = noOfRecords;
        memcpy(&key1, (char*)key, sizeof(float));
        for (int i = 0; i < noOfRecords; i++) {
            int offset = i * (sizeof(float) + 2 * sizeof(int));
            float key2;
            memcpy(&key2, (char*)arr + offset, sizeof(float));
            if (key1 > key2) {
                continue;
            }
            else {
                index = i;
                break;
            }
        }
        if (index >= noOfRecords) {
            short offset = freeSpacePointer;
            memcpy((char*)arr + offset, &key1, sizeof(float));
            offset += sizeof(float);
            memcpy((char*)arr + offset, &rid.pageNum, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)arr + offset, &rid.slotNum, sizeof(int));
        }
        else {
            short offset = (sizeof(float) + 2 * sizeof(int)) * index;
            short startShiftByte = offset + (sizeof(float) + 2 * sizeof(int));
            short totalBytesShifted = freeSpacePointer - offset;
            memmove((char*)arr + startShiftByte, (char*)arr + offset, totalBytesShifted);
            memcpy((char*)arr + offset, &key1, sizeof(float));
            offset += sizeof(float);
            memcpy((char*)arr + offset, &rid.pageNum, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)arr + offset, &rid.slotNum, sizeof(int));
        }
        freeSpacePointer += sizeof(float) + 2 * (sizeof(int));
        memcpy((char*)arr + 4093, &freeSpacePointer, sizeof(short));
        noOfRecords += 1;
        memcpy((char*)arr + 4091, &noOfRecords, sizeof(short));
    }
    else {
        int len;
        memcpy(&len, (char*)key, sizeof(int));
        string key1;
        key1.resize(len);
        memcpy(&key1[0], (char*)key + sizeof(int), len);
        int offset = 0;
        short newOffset = freeSpacePointer;
        short startShiftByte;
        short totalBytesShifted;
        for (int i = 0; i < noOfRecords; i++) {
            int len1 = 0;
            memcpy(&len1, (char*)arr + offset, sizeof(int));
            offset += sizeof(int);
            string key2;
            key2.resize(len1);
            memcpy(&key2[0], (char*)arr + offset, len1);
            offset += len1;
            offset += (2 * sizeof(int));
            if (key1.compare(key2) > 0) {
                continue;
            }
            else {
                newOffset = offset - (2 * sizeof(int)) - len1 - sizeof(int);
                startShiftByte = newOffset + sizeof(int) + len + 2 * sizeof(int);
                totalBytesShifted = freeSpacePointer - newOffset;
                break;
            }
        }
        if (newOffset == freeSpacePointer) {
            short offset = freeSpacePointer;
            memcpy((char*)arr + offset, &len, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)arr + offset, (char*)key1.c_str(), len);
            offset += len;
            memcpy((char*)arr + offset, &rid.pageNum, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)arr + offset, &rid.slotNum, sizeof(int));
            offset += sizeof(int);
        }
        else {
            memmove((char*)arr + startShiftByte, (char*)arr + newOffset, totalBytesShifted);
            memcpy((char*)arr + newOffset, &len, sizeof(int));
            newOffset += sizeof(int);
            memcpy((char*)arr + newOffset, (char*)key1.c_str(), len);
            newOffset += len;
            memcpy((char*)arr + newOffset, &rid.pageNum, sizeof(int));
            newOffset += sizeof(int);
            memcpy((char*)arr + newOffset, &rid.slotNum, sizeof(int));
        }
        freeSpacePointer += (len + sizeof(int) + 2 * sizeof(int));
        memcpy((char*)arr + 4093, &freeSpacePointer, sizeof(short));
        noOfRecords += 1;
        memcpy((char*)arr + 4091, &noOfRecords, sizeof(short));
    }
    ixfileHandle.writePage(pageNumber, arr);
    return 0;
}

RC IndexManager::findKey(const Attribute& attribute, const void* arr, const void* key, int& pagePointer)
{
    short noOfRecords;
    memcpy(&noOfRecords, (char*)arr + 4091, sizeof(short));
    if (attribute.type == TypeInt) {
        int key1;
        memcpy(&key1, (char*)key, sizeof(int));
        for (int i = 0; i < noOfRecords; i++) {
            int key2;
            memcpy(&key2, (char*)arr + i * (2 * sizeof(int)) + sizeof(int), sizeof(int));
            if (key1 > key2 && (i + 1) < noOfRecords) {
                continue;
            }
            else {
                if (key1 >= key2) {
                    int pageNum;
                    memcpy(&pageNum, (char*)arr + i * (2 * sizeof(int)) + (2 * sizeof(int)), sizeof(int));
                    pagePointer = pageNum;
                }
                else {
                    int pageNum;
                    memcpy(&pageNum, (char*)arr + i * (2 * sizeof(int)), sizeof(int));
                    pagePointer = pageNum;
                }
                break;
            }
        }
    }
    else if (attribute.type == TypeReal) {
        float key1;
        memcpy(&key1, (char*)key, sizeof(float));
        for (int i = 0; i < noOfRecords; i++) {
            float key2;
            memcpy(&key2, (char*)arr + i * (2 * sizeof(int)) + sizeof(int), sizeof(float));
            if (key1 > key2 && (i + 1) < noOfRecords) {
                continue;
            }
            else {
                if (key1 >= key2) {
                    int pageNum;
                    memcpy(&pageNum, (char*)arr + i * (2 * sizeof(int)) + (2 * sizeof(int)), sizeof(int));
                    pagePointer = pageNum;
                }
                else {
                    int pageNum;
                    memcpy(&pageNum, (char*)arr + i * (2 * sizeof(int)), sizeof(int));
                    pagePointer = pageNum;
                }
                break;
            }
        }
    }
    else {
        int len;
        memcpy(&len, (char*)key, sizeof(int));
        string key1;
        key1.resize(len);
        memcpy(&key1[0], (char*)key + sizeof(int), len);
        int offset = sizeof(int);
        for (int i = 0; i < noOfRecords; i++) {
            int len1;
            memcpy(&len1, (char*)arr + offset, sizeof(int));
            offset += sizeof(int);
            string key2;
            key2.resize(len1);
            memcpy(&key2[0], (char*)arr + offset, len1);
            offset += len1;
            if (key1.compare(key2) > 0 && (i + 1) < noOfRecords) {
                offset += sizeof(int);
                continue;
            }
            else {
                if (key1.compare(key2) >= 0) {
                    int pageNum;
                    memcpy(&pageNum, (char*)arr + offset, sizeof(int));
                    pagePointer = pageNum;
                }
                else {
                    int pageNum;
                    memcpy(&pageNum, (char*)arr + offset - 2 * sizeof(int) - len1, sizeof(int));
                    pagePointer = pageNum;
                }
                break;
            }
        }
    }
    return 0;
}

RC IndexManager::printLeafPage(void* arr, const Attribute& attribute, int noOfTabs)
{
    for (int i = 0; i < noOfTabs; i++)
        cout << "\t";
    short noOfRecords;
    memcpy(&noOfRecords, (char*)arr + 4091, sizeof(short));
    cout << "{\"keys\": [";
    if (attribute.type == TypeInt) {
        vector<pair<int, pair<int, int> > > print;
        for (int i = 0; i < noOfRecords; i++) {
            int offset = i * 3 * sizeof(int);
            int key;
            memcpy(&key, (char*)arr + offset, sizeof(int));
            offset += sizeof(int);
            int pageNum;
            memcpy(&pageNum, (char*)arr + offset, sizeof(int));
            offset += sizeof(int);
            int slotNum;
            memcpy(&slotNum, (char*)arr + offset, sizeof(int));
            print.push_back(make_pair(key, make_pair(pageNum, slotNum)));
        }
        int len = print.size();
        int i = 0;
        while (i < len) {
            vector<pair<int, int> > rids;
            int keyValue = print[i].first;
            rids.push_back(make_pair(print[i].second.first, print[i].second.second));
            i++;
            while (i < len && print[i].first == keyValue) {
                rids.push_back(make_pair(print[i].second.first, print[i].second.second));
                i++;
            }
            sort(rids.begin(), rids.end(), pairSort);
            int j = 1;
            cout << "\"" << keyValue << ":[";
            cout << "(" << rids[0].first << "," << rids[0].second << ")";
            while ((unsigned)j < rids.size()) {
                cout << ",(" << rids[j].first << "," << rids[j].second << ")";
                j++;
            }
            cout << "]\"";
            if (i < len)
                cout << ",";
        }
        cout << "]}";
    }
    else if (attribute.type == TypeReal) {
        vector<pair<float, pair<int, int> > > print;
        for (int i = 0; i < noOfRecords; i++) {
            int offset = i * (sizeof(float) + 2 * sizeof(int));
            float key;
            memcpy(&key, (char*)arr + offset, sizeof(float));
            offset += sizeof(float);
            int pageNum;
            memcpy(&pageNum, (char*)arr + offset, sizeof(int));
            offset += sizeof(int);
            int slotNum;
            memcpy(&slotNum, (char*)arr + offset, sizeof(int));
            print.push_back(make_pair(key, make_pair(pageNum, slotNum)));
        }
        int len = print.size();
        int i = 0;
        while (i < len) {
            vector<pair<int, int> > rids;
            float keyValue = print[i].first;
            rids.push_back(make_pair(print[i].second.first, print[i].second.second));
            i++;
            while (i < len && print[i].first == keyValue) {
                rids.push_back(make_pair(print[i].second.first, print[i].second.second));
                i++;
            }
            sort(rids.begin(), rids.end(), pairSort);
            int j = 1;
            cout << "\"" << keyValue << ":[";
            cout << "(" << rids[0].first << "," << rids[0].second << ")";
            while ((unsigned)j < rids.size()) {
                cout << ",(" << rids[j].first << "," << rids[j].second << ")";
                j++;
            }
            cout << "]\"";
            if (i < len)
                cout << ",";
        }
        cout << "]}";
    }
    else {
        int offset = 0;
        vector<pair<string, pair<int, int> > > print;
        for (int i = 0; i < noOfRecords; i++) {
            int len;
            memcpy(&len, (char*)arr + offset, sizeof(int));
            offset += sizeof(int);
            string key;
            key.resize(len);
            memcpy(&key[0], (char*)arr + offset, len);
            offset += len;
            int pageNum;
            memcpy(&pageNum, (char*)arr + offset, sizeof(int));
            offset += sizeof(int);
            int slotNum;
            memcpy(&slotNum, (char*)arr + offset, sizeof(int));
            offset += sizeof(int);
            print.push_back(make_pair(key, make_pair(pageNum, slotNum)));
        }
        int len = print.size();
        int i = 0;
        while (i < len) {
            vector<pair<int, int> > rids;
            string keyValue = print[i].first;
            rids.push_back(make_pair(print[i].second.first, print[i].second.second));
            i++;
            while (i < len && print[i].first == keyValue) {
                rids.push_back(make_pair(print[i].second.first, print[i].second.second));
                i++;
            }
            sort(rids.begin(), rids.end(), pairSort);
            int j = 1;
            cout << "\"" << keyValue << ":[";
            cout << "(" << rids[0].first << "," << rids[0].second << ")";
            while ((unsigned)j < rids.size()) {
                cout << ",(" << rids[j].first << "," << rids[j].second << ")";
                j++;
            }
            cout << "]\"";
            if (i < len)
                cout << ",";
        }
        cout << "]}";
    }
    return 0;
}

RC IndexManager::printNonLeafPage(void* arr, const Attribute& attribute, int noOfTabs)
{
    for (int i = 0; i < noOfTabs; i++)
        cout << "\t";
    cout << "{" << endl;
    for (int i = 0; i < noOfTabs; i++)
        cout << "\t";
    short noOfRecords;
    memcpy(&noOfRecords, (char*)arr + 4091, sizeof(short));
    cout << "\"keys\":[";
    if (attribute.type == TypeInt) {
        for (int i = 0; i < noOfRecords; i++) {
            int offset = sizeof(int) + i * 2 * sizeof(int);
            int key;
            memcpy(&key, (char*)arr + offset, sizeof(int));
            cout << "\"" << key << "\"";
            if (i != noOfRecords - 1) {
                cout << ",";
            }
        }
        cout << "]," << endl;
        for (int i = 0; i < noOfTabs; i++)
            cout << "\t";
        cout << "\"children\": [" << endl;
    }
    else if (attribute.type == TypeReal) {
        for (int i = 0; i < noOfRecords; i++) {
            int offset = sizeof(int) + i * (sizeof(float) + sizeof(int));
            float key;
            memcpy(&key, (char*)arr + offset, sizeof(float));
            cout << "\"" << key << "\"";
            if (i != noOfRecords - 1) {
                cout << ",";
            }
        }
        cout << "]," << endl;
        for (int i = 0; i < noOfTabs; i++)
            cout << "\t";
        cout << " \"children\": [" << endl;
    }
    else {
        int offset = sizeof(int);
        for (int i = 0; i < noOfRecords; i++) {
            int len;
            memcpy(&len, (char*)arr + offset, sizeof(int));
            offset += sizeof(int);
            string key;
            key.resize(len);
            memcpy(&key[0], (char*)arr + offset, len);
            offset += len;
            offset += sizeof(int);
            cout << "\"" << key << "\"";
            if (i != noOfRecords - 1) {
                cout << ",";
            }
        }
        cout << "]," << endl;
        for (int i = 0; i < noOfTabs; i++)
            cout << "\t";
        cout << " \"children\": [" << endl;
    }
    return 0;
}
RC IndexManager::printBTreeUtil(IXFileHandle& ixfileHandle, int pageNum, const Attribute& attribute, int noOfTabs)
{
    bool isLeaf;
    void* arr = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pageNum, arr);
    memcpy(&isLeaf, (char*)arr + 4095, sizeof(bool));
    if (isLeaf) {
        if (pageNum == 0) {
            printLeafPage(arr, attribute, noOfTabs);
        }
        else {
            printLeafPage(arr, attribute, noOfTabs + 1);
        }
    }
    else {
        printNonLeafPage(arr, attribute, noOfTabs);
        short noOfRecords;
        memcpy(&noOfRecords, (char*)arr + 4091, sizeof(short));
        if (attribute.type == TypeInt || attribute.type == TypeReal) {
            for (int i = 0; i <= noOfRecords; i++) {
                int offset = i * (2 * sizeof(int));
                int nextPointer;
                memcpy(&nextPointer, (char*)arr + offset, sizeof(int));
                if (nextPointer < 0) {
                    break;
                }
                printBTreeUtil(ixfileHandle, nextPointer, attribute, noOfTabs + 1);
                if (i < noOfRecords) {
                    cout << "," << endl;
                }
                else {
                    cout << endl;
                }
            }
        }
        else {
            int offset = 0;
            for (int i = 0; i <= noOfRecords; i++) {
                int nextPointer;
                memcpy(&nextPointer, (char*)arr + offset, sizeof(int));
                if (i != noOfRecords) {
                    offset += sizeof(int);
                    int len;
                    memcpy(&len, (char*)arr + offset, sizeof(int));
                    offset += len + sizeof(int);
                }
                if (nextPointer < 0) {
                    break;
                }
                printBTreeUtil(ixfileHandle, nextPointer, attribute, noOfTabs + 1);
                if (i < noOfRecords) {
                    cout << "," << endl;
                }
            }
        }
        for (int i = 0; i < noOfTabs; i++)
            cout << "\t";
        cout << "]}" << endl;
    }
    free(arr);
    return 0;
}

void IndexManager::printBtree(IXFileHandle& ixfileHandle, const Attribute& attribute) const
{
    int noOfPages = ixfileHandle.getNumberOfPages();
    if (noOfPages == 0) {
        cout << "{\"keys\": []}" << endl;
        return;
    }
    printBTreeUtil(ixfileHandle, 0, attribute, 0);
    cout << endl;
}
