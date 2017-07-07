#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
# define RID_SIZE 8 //RID size (pageNum + slotNum)
# define LSIZE 9 //leaf directory size
# define NSIZE 5 //nonleaf directory size
# define RSIZE 12 //leaf record size

class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
	PagedFileManager *pfm;

        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

	//custom functions
	RC insertUtil(IXFileHandle &ixfileHandle, int nodePageNum, const Attribute &attribute,
        		const void *key, const RID &rid, int keyLength, void *childKey, int &childPageId);
        RC findKey(const Attribute &attribute, const void *nodeData, const void *key, int &keyPointer);
        RC splitLeafPage(IXFileHandle &ixfileHandle, void *nodeData, int nodePageNum, const Attribute &attribute, const void *key,
        		const RID &rid, int keyLength, void* newChildKey, int &newChildPageId);
        RC insertLeafRecord(IXFileHandle &ixfileHandle, void* nodeData, int nodePageNum, const Attribute attribute, const void* key, 				const RID &rid, int keyLength);
        RC insertNonLeafRecord(IXFileHandle &ixfileHandle, void * nodeData, int nodePageNum, const Attribute &attribute,
        		void* newChildKey, int &newChildPageId);
        static RC printBTreeUtil(IXFileHandle &ixfileHandle, int pageNum, const Attribute &attribute, int numTabs);
        static RC printLeafPage(void* nodeData, const Attribute &attribute, int numTabs);
        static RC printNonLeafPage(void* nodeData, const Attribute &attribute, int numTabs);
        RC deletedatafromTree(IXFileHandle &ixfileHandle, const Attribute &attribute,
                		const void *key, const RID &rid, int pageNum);
        RC LeafPagevalue(IXFileHandle &ixfileHandle, int nodePageNum, const Attribute &attribute, const void*key);

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
};

class IXFileHandle {
    public:

    FileHandle fileHandle;
    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

	//custom functions
	RC readPage(PageNum pageNum, void *data);
	RC writePage(PageNum pageNum, const void *data);
	RC appendPage(const void *data);
	unsigned getNumberOfPages();


};

class IX_ScanIterator {
    public:
	IXFileHandle ixfileHandle;
	Attribute attribute;
	const void *lowKey;
	const void *highKey;
	bool lowKeyInclusive;
	bool highKeyInclusive;
	void* arr;
	bool isEOF;
	int currPage;
	int offset;
	short freeSpacePointer;

	// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
};

#endif
