#include "rbfm.h"
#include <math.h>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance() {
	if (!_rbf_manager)
		_rbf_manager = new RecordBasedFileManager();

	return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() {
	pfm = PagedFileManager::instance();
}

void readAttributes(vector<string> attributeNames,
		vector<Attribute> recordDescriptor, int nextRecordPos, char *arr,
		void *data) {
	bool nullBit = false;
	int nullfieldindicatorAttributes = ceil(
			(double) attributeNames.size() / CHAR_BIT);

	unsigned char *nullsIndicator1 = (unsigned char *) malloc(
			nullfieldindicatorAttributes);
	memset(nullsIndicator1, 0, nullfieldindicatorAttributes);
	//memcpy(data, nullsIndicator1, nullfieldindicatorAttributes);
	int dataOffset = nullfieldindicatorAttributes;

	int nullfieldindicator = ceil((double) recordDescriptor.size() / CHAR_BIT);

	unsigned char *nullsIndicator = (unsigned char *) malloc(
			nullfieldindicator);

	int offset = nextRecordPos;

	char *record = (char *) malloc(sizeof(char));
	memcpy(record, arr + offset, sizeof(char));
	offset += sizeof(char);

	memcpy(nullsIndicator, arr + offset, nullfieldindicator);

	offset += nullfieldindicator;

	for (int i = 0; (unsigned)i < attributeNames.size(); i++) {
		int pointers = 0;

		int index = -1;
		for (int k = 0; (unsigned)k < recordDescriptor.size(); k++) {
			nullBit = (((char *) nullsIndicator)[0 + (k / CHAR_BIT)])
					& (1 << (7 - (k % CHAR_BIT)));

			if (!nullBit) {
				if (recordDescriptor[k].name == attributeNames[i]) {
					index = k;
					break;
				} else
					pointers += sizeof(int);
			}

		}

//		offset += pointers;
		nullBit = nullsIndicator[0 + (index / CHAR_BIT)]
				& (1 << (7 - (index % CHAR_BIT)));
		if (!nullBit) {
			//nullsIndicator1[0] = 0; // 00000000

			if (recordDescriptor[index].type == TypeVarChar) {
				int strPos = 0;
				memcpy(&strPos, arr + offset + pointers, sizeof(int));
				short strLength = 0;
				memcpy(&strLength, arr + nextRecordPos + strPos, sizeof(short));
				strPos += sizeof(short);

				int intStrLength = strLength;
				memcpy((char *) data + dataOffset, &intStrLength, sizeof(int));
				dataOffset += sizeof(int);
				memcpy((char *) data + dataOffset, arr + nextRecordPos + strPos,
						intStrLength);
				dataOffset += intStrLength;
			} else if (recordDescriptor[index].type == TypeReal) {
				int strPos = 0;
				memcpy(&strPos, arr + offset + pointers, sizeof(int));
				memcpy((char *) data + dataOffset, arr + nextRecordPos + strPos,
						sizeof(float));
				dataOffset += sizeof(float);
			} else if (recordDescriptor[index].type == TypeInt) {
				int strPos = 0;
				memcpy(&strPos, arr + offset + pointers, sizeof(int));
//				int x = *((int *) ((char *) arr + nextRecordPos + strPos));
				memcpy((char *) data + dataOffset, arr + nextRecordPos + strPos,
						sizeof(int));
				dataOffset += sizeof(int);
			}
		} else {
			int x = 7 - (i % CHAR_BIT);
			nullsIndicator1[0 + (i / CHAR_BIT)] += (int) pow(2.0, (float) x);
//			nullsIndicator1[0] = 128; // 10000000
//			memcpy(data, nullsIndicator1, sizeof(char));
		}

	}
	memcpy((char *) data, nullsIndicator1, nullfieldindicatorAttributes);
}

RecordBasedFileManager::~RecordBasedFileManager() {
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
	int noOfPages;

//	RecordBasedFileManager * rbfmm = RecordBasedFileManager::instance();

	noOfPages = this->fileHandle.getNumberOfPages();
//	cout << this->conditionAttribute;
	for (int i = this->test.pageNum; i < noOfPages; i++) {
		int nullfieldindicator;
		char *arr = (char *) malloc(4096);
		this->fileHandle.readPage(i, arr);

		int noOfSlots = 0;
		memcpy(&noOfSlots, arr + 4084, sizeof(int));

		for (int j = this->test.slotNum + 1; j < noOfSlots; j++) {

			int slotOffset = 4080 - (j * sizeof(int));

			bool nullBit = false;

			nullfieldindicator = ceil(
					(double) this->recordDescriptor.size() / CHAR_BIT);

			unsigned char *nullsIndicator = (unsigned char *) malloc(
					nullfieldindicator);

			int offset = 0;

			memcpy(&offset, arr + slotOffset, sizeof(int));
			if (offset == -1) {
				continue;
			}

			int nextRecordPos = offset;

			char *record = (char *) malloc(sizeof(char));
			memcpy(record, arr + offset, sizeof(char));
			offset += sizeof(char);

			bool checkBit = record[0] & (1 << 6);
			if (checkBit == true) {
				continue;
			}

			bool checkBit1 = record[0] & (1 << 7);
			if (checkBit1 == true) {
				RID rid_tomb;
				memcpy(&rid_tomb.pageNum, arr + offset, sizeof(int));
				memcpy(&rid_tomb.slotNum, arr + offset + sizeof(int),
						sizeof(int));

				///////////////////////////////////////////////////////////////////
				char *arr0 = (char *) malloc(4096);
				this->fileHandle.readPage(rid_tomb.pageNum, arr0);

				int slotOffset = 4080 - (rid_tomb.slotNum * sizeof(int));

				bool nullBit = false;

				nullfieldindicator = ceil(
						(double) this->recordDescriptor.size() / CHAR_BIT);

				unsigned char *nullsIndicator = (unsigned char *) malloc(
						nullfieldindicator);

				int offset = 0;

				memcpy(&offset, arr0 + slotOffset, sizeof(int));
				if (offset == -1) {
					continue;
				}

				int nextRecordPos = offset;

				char *record = (char *) malloc(sizeof(char));
				memcpy(record, arr0 + offset, sizeof(char));
				offset += sizeof(char);

				memcpy(nullsIndicator, arr0 + offset, nullfieldindicator);

				offset += nullfieldindicator;

				int pointers = 0;

				int index = -1;
				for (int k = 0; (unsigned)k < this->recordDescriptor.size(); k++) {
					nullBit = (((char *) nullsIndicator)[0 + (k / CHAR_BIT)])
							& (1 << (7 - (k % CHAR_BIT)));

					if (!nullBit) {
						if (this->recordDescriptor[k].name
								== this->conditionAttribute) {
							index = k;
							break;
						} else
							pointers += sizeof(int);
					}

				}

				if (index == -1 && this->compOp == NO_OP
						&& this->conditionAttribute == "") {
					readAttributes(this->attributeNames, this->recordDescriptor,
							nextRecordPos, arr0, data);
					this->test.pageNum = i;
					this->test.slotNum = j;
					rid = this->test;
					return 0;
				}

				//			offset += pointers;
				nullBit = nullsIndicator[0 + (index / CHAR_BIT)]
						& (1 << (7 - (index % CHAR_BIT)));
				if (!nullBit) {

					if (this->recordDescriptor[index].type == TypeVarChar) {

						int strPos = 0;
						memcpy(&strPos, arr0 + offset + pointers, sizeof(int));
						short strLength = 0;
						memcpy(&strLength, arr0 + nextRecordPos + strPos,
								sizeof(short));
						strPos += sizeof(short);

						int intStrLength = strLength;

						//int l = *((int *)((char *)value));
						int dataLength = 0;
						memcpy(&dataLength, (char *) this->value, sizeof(int));

						std::string s;
						s.resize(intStrLength);
						memcpy(&s[0], arr0 + nextRecordPos + strPos,
								intStrLength);

						std::string s1;
						s1.resize(dataLength);
						memcpy(&s1[0], (char *) this->value + sizeof(int),
								dataLength);

						switch (this->compOp) {
						case EQ_OP:
							if (s.compare(s1) == 0) {
								readAttributes(this->attributeNames,
										this->recordDescriptor, nextRecordPos,
										arr0, data);
								this->test.pageNum = i;
								this->test.slotNum = j;
								rid = test;
								return 0;
							} else {
								continue;
							}
						case LT_OP:
							if (s.compare(s1) < 0) {
								readAttributes(this->attributeNames,
										this->recordDescriptor, nextRecordPos,
										arr0, data);
								this->test.pageNum = i;
								this->test.slotNum = j;
								rid = this->test;
								return 0;
							} else {
								continue;
							}
						case LE_OP:
							if (s.compare(s1) <= 0) {
								readAttributes(this->attributeNames,
										this->recordDescriptor, nextRecordPos,
										arr0, data);
								this->test.pageNum = i;
								this->test.slotNum = j;
								rid = this->test;
								return 0;
							} else {
								continue;
							}
						case GT_OP:
							if (s.compare(s1) > 0) {
								readAttributes(this->attributeNames,
										this->recordDescriptor, nextRecordPos,
										arr0, data);
								this->test.pageNum = i;
								this->test.slotNum = j;
								rid = this->test;
								return 0;
							} else {
								continue;
							}
						case GE_OP:
							if (s.compare(s1) >= 0) {
								readAttributes(this->attributeNames,
										this->recordDescriptor, nextRecordPos,
										arr0, data);
								this->test.pageNum = i;
								this->test.slotNum = j;
								rid = this->test;
								return 0;
							} else {
								continue;
							}
						case NE_OP:
							if (s.compare(s1) != 0) {
								readAttributes(this->attributeNames,
										this->recordDescriptor, nextRecordPos,
										arr0, data);
								this->test.pageNum = i;
								this->test.slotNum = j;
								rid = this->test;
								return 0;
							} else {
								continue;
							}
						case NO_OP:
							readAttributes(this->attributeNames,
									this->recordDescriptor, nextRecordPos, arr0,
									data);
							this->test.pageNum = i;
							this->test.slotNum = j;
							rid = this->test;
							return 0;
						}

					} else if (this->recordDescriptor[index].type == TypeReal) {
						float valueFloat;
						int strPos = 0;
						memcpy(&strPos, arr0 + offset + pointers, sizeof(int));
						memcpy(&valueFloat, arr0 + nextRecordPos + strPos,
								sizeof(float));
						switch (this->compOp) {
						case EQ_OP:
							if (valueFloat == *(float *) (this->value)) {
								readAttributes(this->attributeNames,
										this->recordDescriptor, nextRecordPos,
										arr0, data);
								this->test.pageNum = i;
								this->test.slotNum = j;
								rid = this->test;
								return 0;
							} else {
								continue;
							}
						case LT_OP:
							if (valueFloat < *(float *) (this->value)) {
								readAttributes(this->attributeNames,
										this->recordDescriptor, nextRecordPos,
										arr0, data);
								this->test.pageNum = i;
								this->test.slotNum = j;
								rid = this->test;
								return 0;
							} else {
								continue;
							}
						case LE_OP:
							if (valueFloat <= *(float *) (this->value)) {
								readAttributes(this->attributeNames,
										this->recordDescriptor, nextRecordPos,
										arr0, data);
								this->test.pageNum = i;
								this->test.slotNum = j;
								rid = this->test;
								return 0;
							} else {
								continue;
							}
						case GT_OP:
							if (valueFloat > *(float *) (this->value)) {
								readAttributes(this->attributeNames,
										this->recordDescriptor, nextRecordPos,
										arr0, data);
								this->test.pageNum = i;
								this->test.slotNum = j;
								rid = this->test;
								return 0;
							} else {
								continue;
							}
						case GE_OP:
							if (valueFloat >= *(float *) (this->value)) {
								readAttributes(this->attributeNames,
										this->recordDescriptor, nextRecordPos,
										arr0, data);
								this->test.pageNum = i;
								this->test.slotNum = j;
								rid = this->test;
								return 0;
							} else {
								continue;
							}
						case NE_OP:
							if (valueFloat != *(float *) (this->value)) {
								readAttributes(this->attributeNames,
										this->recordDescriptor, nextRecordPos,
										arr0, data);
								this->test.pageNum = i;
								this->test.slotNum = j;
								rid = this->test;
								return 0;
							} else {
								continue;
							}
						case NO_OP:
							readAttributes(this->attributeNames,
									this->recordDescriptor, nextRecordPos, arr0,
									data);
							this->test.pageNum = i;
							this->test.slotNum = j;
							rid = this->test;
							return 0;
						}
					} else if (this->recordDescriptor[index].type == TypeInt) {
						int strPos = 0;
						int valueInt;
						memcpy(&strPos, arr0 + offset + pointers, sizeof(int));
						memcpy(&valueInt, arr0 + nextRecordPos + strPos,
								sizeof(int));

						switch (this->compOp) {
						case EQ_OP:
							if (valueInt == *((int *) this->value)) {
								readAttributes(this->attributeNames,
										this->recordDescriptor, nextRecordPos,
										arr0, data);
								this->test.pageNum = i;
								this->test.slotNum = j;
								rid = this->test;
								return 0;
							} else {
								continue;
							}
						case LT_OP:
							if (valueInt < *(int *) (this->value)) {
								readAttributes(this->attributeNames,
										this->recordDescriptor, nextRecordPos,
										arr0, data);
								this->test.pageNum = i;
								this->test.slotNum = j;
								rid = this->test;
								return 0;
							} else {
								continue;
							}
						case LE_OP:
							if (valueInt <= *(int *) (this->value)) {
								readAttributes(this->attributeNames,
										this->recordDescriptor, nextRecordPos,
										arr0, data);
								this->test.pageNum = i;
								this->test.slotNum = j;
								rid = this->test;
								return 0;
							} else {
								continue;
							}
						case GT_OP:
							if (valueInt > *(int *) (this->value)) {
								readAttributes(this->attributeNames,
										this->recordDescriptor, nextRecordPos,
										arr0, data);
								this->test.pageNum = i;
								this->test.slotNum = j;
								rid = this->test;
								return 0;
							} else {
								continue;
							}
						case GE_OP:
							if (valueInt >= *(int *) (this->value)) {
								readAttributes(this->attributeNames,
										this->recordDescriptor, nextRecordPos,
										arr0, data);
								this->test.pageNum = i;
								this->test.slotNum = j;
								rid = this->test;
								return 0;
							} else {
								continue;
							}
						case NE_OP:
							if (valueInt != *(int *) (this->value)) {
								readAttributes(this->attributeNames,
										this->recordDescriptor, nextRecordPos,
										arr0, data);
								this->test.pageNum = i;
								this->test.slotNum = j;
								rid = this->test;
								return 0;
							} else {
								continue;
							}
						case NO_OP:
							readAttributes(this->attributeNames,
									this->recordDescriptor, nextRecordPos, arr0,
									data);
							this->test.pageNum = i;
							this->test.slotNum = j;
							rid = this->test;
							return 0;
						}
					}

				} else {
					continue;
				}
				///////////////////////////////////////////////////////////////////

			}

			memcpy(nullsIndicator, arr + offset, nullfieldindicator);

			offset += nullfieldindicator;

			int pointers = 0;

			int index = -1;
			for (int k = 0; (unsigned)k < this->recordDescriptor.size(); k++) {
				nullBit = (((char *) nullsIndicator)[0 + (k / CHAR_BIT)])
						& (1 << (7 - (k % CHAR_BIT)));

				if (!nullBit) {
					if (this->recordDescriptor[k].name
							== this->conditionAttribute) {
						index = k;
						break;
					} else
						pointers += sizeof(int);
				}

			}

			if (index == -1 && this->compOp == NO_OP
					&& this->conditionAttribute == "") {
				readAttributes(this->attributeNames, this->recordDescriptor,
						nextRecordPos, arr, data);
				this->test.pageNum = i;
				this->test.slotNum = j;
				rid = this->test;
				return 0;
			}

//			offset += pointers;
			nullBit = nullsIndicator[0 + (index / CHAR_BIT)]
					& (1 << (7 - (index % CHAR_BIT)));
			if (!nullBit) {

				if (this->recordDescriptor[index].type == TypeVarChar) {

					int strPos = 0;
					memcpy(&strPos, arr + offset + pointers, sizeof(int));
					short strLength = 0;
					memcpy(&strLength, arr + nextRecordPos + strPos,
							sizeof(short));
					strPos += sizeof(short);

					int intStrLength = strLength;

					//int l = *((int *)((char *)value));
					int dataLength = 0;
					memcpy(&dataLength, (char *) this->value, sizeof(int));

					std::string s;
					s.resize(intStrLength);
					memcpy(&s[0], arr + nextRecordPos + strPos, intStrLength);

					std::string s1;
					s1.resize(dataLength);
					memcpy(&s1[0], (char *) this->value + sizeof(int),
							dataLength);

					switch (this->compOp) {
					case EQ_OP:
						if (s.compare(s1) == 0) {
							readAttributes(this->attributeNames,
									this->recordDescriptor, nextRecordPos, arr,
									data);
							this->test.pageNum = i;
							this->test.slotNum = j;
							rid = test;
							return 0;
						} else {
							continue;
						}
					case LT_OP:
						if (s.compare(s1) < 0) {
							readAttributes(this->attributeNames,
									this->recordDescriptor, nextRecordPos, arr,
									data);
							this->test.pageNum = i;
							this->test.slotNum = j;
							rid = this->test;
							return 0;
						} else {
							continue;
						}
					case LE_OP:
						if (s.compare(s1) <= 0) {
							readAttributes(this->attributeNames,
									this->recordDescriptor, nextRecordPos, arr,
									data);
							this->test.pageNum = i;
							this->test.slotNum = j;
							rid = this->test;
							return 0;
						} else {
							continue;
						}
					case GT_OP:
						if (s.compare(s1) > 0) {
							readAttributes(this->attributeNames,
									this->recordDescriptor, nextRecordPos, arr,
									data);
							this->test.pageNum = i;
							this->test.slotNum = j;
							rid = this->test;
							return 0;
						} else {
							continue;
						}
					case GE_OP:
						if (s.compare(s1) >= 0) {
							readAttributes(this->attributeNames,
									this->recordDescriptor, nextRecordPos, arr,
									data);
							this->test.pageNum = i;
							this->test.slotNum = j;
							rid = this->test;
							return 0;
						} else {
							continue;
						}
					case NE_OP:
						if (s.compare(s1) != 0) {
							readAttributes(this->attributeNames,
									this->recordDescriptor, nextRecordPos, arr,
									data);
							this->test.pageNum = i;
							this->test.slotNum = j;
							rid = this->test;
							return 0;
						} else {
							continue;
						}
					case NO_OP:
						readAttributes(this->attributeNames,
								this->recordDescriptor, nextRecordPos, arr,
								data);
						this->test.pageNum = i;
						this->test.slotNum = j;
						rid = this->test;
						return 0;
					}

				} else if (this->recordDescriptor[index].type == TypeReal) {
					float valueFloat;
					int strPos = 0;
					memcpy(&strPos, arr + offset + pointers, sizeof(int));
					memcpy(&valueFloat, arr + nextRecordPos + strPos,
							sizeof(float));
					switch (this->compOp) {
					case EQ_OP:
						if (valueFloat == *(float *) (this->value)) {
							readAttributes(this->attributeNames,
									this->recordDescriptor, nextRecordPos, arr,
									data);
							this->test.pageNum = i;
							this->test.slotNum = j;
							rid = this->test;
							return 0;
						} else {
							continue;
						}
					case LT_OP:
						if (valueFloat < *(float *) (this->value)) {
							readAttributes(this->attributeNames,
									this->recordDescriptor, nextRecordPos, arr,
									data);
							this->test.pageNum = i;
							this->test.slotNum = j;
							rid = this->test;
							return 0;
						} else {
							continue;
						}
					case LE_OP:
						if (valueFloat <= *(float *) (this->value)) {
							readAttributes(this->attributeNames,
									this->recordDescriptor, nextRecordPos, arr,
									data);
							this->test.pageNum = i;
							this->test.slotNum = j;
							rid = this->test;
							return 0;
						} else {
							continue;
						}
					case GT_OP:
						if (valueFloat > *(float *) (this->value)) {
							readAttributes(this->attributeNames,
									this->recordDescriptor, nextRecordPos, arr,
									data);
							this->test.pageNum = i;
							this->test.slotNum = j;
							rid = this->test;
							return 0;
						} else {
							continue;
						}
					case GE_OP:
						if (valueFloat >= *(float *) (this->value)) {
							readAttributes(this->attributeNames,
									this->recordDescriptor, nextRecordPos, arr,
									data);
							this->test.pageNum = i;
							this->test.slotNum = j;
							rid = this->test;
							return 0;
						} else {
							continue;
						}
					case NE_OP:
						if (valueFloat != *(float *) (this->value)) {
							readAttributes(this->attributeNames,
									this->recordDescriptor, nextRecordPos, arr,
									data);
							this->test.pageNum = i;
							this->test.slotNum = j;
							rid = this->test;
							return 0;
						} else {
							continue;
						}
					case NO_OP:
						readAttributes(this->attributeNames,
								this->recordDescriptor, nextRecordPos, arr,
								data);
						this->test.pageNum = i;
						this->test.slotNum = j;
						rid = this->test;
						return 0;
					}
				} else if (this->recordDescriptor[index].type == TypeInt) {
					int strPos = 0;
					int valueInt;
					memcpy(&strPos, arr + offset + pointers, sizeof(int));
					memcpy(&valueInt, arr + nextRecordPos + strPos,
							sizeof(int));

					switch (this->compOp) {
					case EQ_OP:
						if (valueInt == *((int *) this->value)) {
							readAttributes(this->attributeNames,
									this->recordDescriptor, nextRecordPos, arr,
									data);
							this->test.pageNum = i;
							this->test.slotNum = j;
							rid = this->test;
							return 0;
						} else {
							continue;
						}
					case LT_OP:
						if (valueInt < *(int *) (this->value)) {
							readAttributes(this->attributeNames,
									this->recordDescriptor, nextRecordPos, arr,
									data);
							this->test.pageNum = i;
							this->test.slotNum = j;
							rid = this->test;
							return 0;
						} else {
							continue;
						}
					case LE_OP:
						if (valueInt <= *(int *) (this->value)) {
							readAttributes(this->attributeNames,
									this->recordDescriptor, nextRecordPos, arr,
									data);
							this->test.pageNum = i;
							this->test.slotNum = j;
							rid = this->test;
							return 0;
						} else {
							continue;
						}
					case GT_OP:
						if (valueInt > *(int *) (this->value)) {
							readAttributes(this->attributeNames,
									this->recordDescriptor, nextRecordPos, arr,
									data);
							this->test.pageNum = i;
							this->test.slotNum = j;
							rid = this->test;
							return 0;
						} else {
							continue;
						}
					case GE_OP:
						if (valueInt >= *(int *) (this->value)) {
							readAttributes(this->attributeNames,
									this->recordDescriptor, nextRecordPos, arr,
									data);
							this->test.pageNum = i;
							this->test.slotNum = j;
							rid = this->test;
							return 0;
						} else {
							continue;
						}
					case NE_OP:
						if (valueInt != *(int *) (this->value)) {
							readAttributes(this->attributeNames,
									this->recordDescriptor, nextRecordPos, arr,
									data);
							this->test.pageNum = i;
							this->test.slotNum = j;
							rid = this->test;
							return 0;
						} else {
							continue;
						}
					case NO_OP:
						readAttributes(this->attributeNames,
								this->recordDescriptor, nextRecordPos, arr,
								data);
						this->test.pageNum = i;
						this->test.slotNum = j;
						rid = this->test;
						return 0;
					}
				}

			} else {
				continue;
			}

		}
		this->test.slotNum = -1;

	}
//	RC suc = rbfmm->closeFile(fileHandle);
//	if(suc != 0)
//		return -1;
	return RBFM_EOF;
}

RC RBFM_ScanIterator::close() {
//	RecordBasedFileManager * rbfm = RecordBasedFileManager::instance();
//	return rbfm->closeFile(this->fileHandle);;
	return -1;
}

RC RecordBasedFileManager::createFile(const string &fileName) {
	return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName,
		FileHandle &fileHandle) {
	return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const void *data, RID &rid,
		int mode) {

	bool nullBit = false;
	int pointersNeeded = 0;
	int nullDescBytes = ceil((double) recordDescriptor.size() / CHAR_BIT);

	for (int j = 0; (unsigned)j < recordDescriptor.size(); j++) {
		nullBit = (((char *) data)[0 + (j / CHAR_BIT)])
				& (1 << (7 - (j % CHAR_BIT)));
		if (!nullBit) {
			pointersNeeded += 1;
		}
	}

	int varCharCount = 0;

	int offset = nullDescBytes;
	int ptr = 0;
	char *arr = (char *) malloc(4096);

	char *record = (char *) malloc(sizeof(char));
	memset(record, 0, sizeof(char));
	if (mode == 1) {
		record[0] |= (1 << 6);
		//cout << "tombstone inserted" << endl;
	}

	memcpy(arr, record, sizeof(char));
	memcpy(arr + sizeof(char), data, nullDescBytes);
	ptr += sizeof(char);
	ptr += nullDescBytes;
	int nullCount = 0;
	ptr += pointersNeeded * sizeof(int);
	for (int j = 0; (unsigned)j < recordDescriptor.size(); j++) {

		nullBit = (((char *) data)[0 + (j / CHAR_BIT)])
				& (1 << (7 - (j % CHAR_BIT)));

		if (!nullBit) {

			switch (recordDescriptor[j].type) {

			case TypeVarChar: {
				int g1;
				memcpy(&g1, (char *) data + offset, sizeof(int));
				short shortVal = g1;
				memcpy(arr + ptr, &shortVal, sizeof(short));

				memcpy(
						arr + sizeof(bool) + nullDescBytes
								+ (nullCount * sizeof(int)), &ptr, sizeof(int));
				ptr += sizeof(short);
				offset = offset + sizeof(int);

				memcpy(arr + ptr, (char *) data + offset, g1);

				offset = offset + g1;
				ptr += g1;
				varCharCount += 1;
				break;
			}
			case TypeInt: {
				memcpy(
						arr + sizeof(bool) + nullDescBytes
								+ (nullCount * sizeof(int)), &ptr, sizeof(int));

				memcpy(arr + ptr, (char *) data + offset, sizeof(int));
				offset += sizeof(int);
				ptr += sizeof(int);
				break;
			}
			case TypeReal: {
				memcpy(
						arr + sizeof(bool) + nullDescBytes
								+ (nullCount * sizeof(int)), &ptr, sizeof(int));

				memcpy(arr + ptr, (char *) data + offset, sizeof(float));
				offset += sizeof(float);
				ptr += sizeof(float);
				break;
			}
			}
			nullCount += 1;
		}

	}

	int totalBytesNeeded = offset + (pointersNeeded * sizeof(int))
			+ sizeof(bool) - (varCharCount * (sizeof(int) - sizeof(short)))
			+ sizeof(int);
	int pageNum = fileHandle.getNumberOfPages();

	int freeSpace = 0;
	int freeSpacePointer = 0;
	int noOfDir = 0;
	int startLoc = 0;
	int pageNumStore = -1;

	char *arr5 = (char *) malloc(PAGE_SIZE);
	if (pageNum == 0) {

		memcpy(arr5, arr, ptr);
		freeSpace = PAGE_SIZE - ptr - (sizeof(int) * 4);
		freeSpacePointer = ptr;
		noOfDir = 1;
		startLoc = 0;

		memcpy(arr5 + 4092, &freeSpace, sizeof(int));
		memcpy(arr5 + 4088, &freeSpacePointer, sizeof(int));
		memcpy(arr5 + 4084, &noOfDir, sizeof(int));
		memcpy(arr5 + 4080, &startLoc, sizeof(int));
		rid.pageNum = 0;
		rid.slotNum = 0;

		fileHandle.appendPage(arr5);
	} else {
		fileHandle.readPage(pageNum - 1, arr5);
		memcpy(&freeSpace, arr5 + 4092, sizeof(int));

		if (freeSpace >= totalBytesNeeded) {

			memcpy(&freeSpacePointer, arr5 + 4088, sizeof(int));
			memcpy(&noOfDir, arr5 + 4084, sizeof(int));
			rid.pageNum = pageNum - 1;
			rid.slotNum = noOfDir;
			memcpy(arr5 + freeSpacePointer, arr, ptr);

			freeSpace = freeSpace - ptr - sizeof(int);
			memcpy(arr5 + 4092, &freeSpace, sizeof(int));

			noOfDir += 1;
			memcpy(arr5 + 4084, &noOfDir, sizeof(int));

			memcpy(arr5 + (4084 - (noOfDir * sizeof(int))), &freeSpacePointer,
					sizeof(int));

			freeSpacePointer = freeSpacePointer + ptr;

			memcpy(arr5 + 4088, &freeSpacePointer, sizeof(int));
			fileHandle.writePage(pageNum - 1, arr5);
		} else {

			for (int i = 0; i < pageNum - 1; i++) {

				fileHandle.readPage(pageNum - 1, arr5);
				memcpy(&freeSpace, arr5 + 4092, sizeof(int));

				if (freeSpace >= totalBytesNeeded) {

					pageNumStore = i;
					memcpy(&freeSpacePointer, arr5 + 4088, sizeof(int));
					memcpy(&noOfDir, arr5 + 4084, sizeof(int));
					rid.pageNum = i;
					rid.slotNum = noOfDir;
					memcpy(arr5 + freeSpacePointer, arr, ptr);
					freeSpace = freeSpace - ptr - sizeof(int);
					memcpy(arr5 + 4092, &freeSpace, sizeof(int));

					noOfDir += 1;

					memcpy(arr5 + 4084, &noOfDir, sizeof(int));
					memcpy(arr5 + 4084 - (noOfDir * sizeof(int)),
							&freeSpacePointer, sizeof(int));
					freeSpacePointer = freeSpacePointer + ptr;
					memcpy(arr5 + 4088, &freeSpacePointer, sizeof(int));
					fileHandle.writePage(i, arr5);
				}
			}

			if (pageNumStore == -1) {
				memcpy(arr5, arr, ptr);
				int freeSpace = PAGE_SIZE - ptr - sizeof(int) * 4;
				int freeSpacePointer = ptr;
				int NoOfDir = 1;
				int startLoc = 0;
				memcpy(arr5 + 4092, &freeSpace, sizeof(int));
				memcpy(arr5 + 4088, &freeSpacePointer, sizeof(int));
				memcpy(arr5 + 4084, &NoOfDir, sizeof(int));
				memcpy(arr5 + 4080, &startLoc, sizeof(int));

				rid.pageNum = pageNum;
				rid.slotNum = 0;
				fileHandle.appendPage(arr5);
			}
		}
	}

//	free(arr);
//	free(arr5);
	return 0;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

	int nullfieldindicator, loop;
	char *arr = (char *) malloc(4096);
	fileHandle.readPage(rid.pageNum, arr);

	int slotOffset = 4080 - (rid.slotNum * sizeof(int));
	bool nullBit = false;

	nullfieldindicator = ceil((double) recordDescriptor.size() / CHAR_BIT);

	unsigned char *nullsIndicator = (unsigned char *) malloc(
			nullfieldindicator);

	int offset = 0;

	memcpy(&offset, arr + slotOffset, sizeof(int));

	if (offset == -1) {
		return -1;
	}
	int nextRecordPos = offset;

	char *record = (char *) malloc(sizeof(char));
	memcpy(record, arr + offset, sizeof(char));
	offset += sizeof(char);

	bool checkBit = record[0] & (1 << 7);
	if (checkBit == true) {
		RID rid_tomb;
		memcpy(&rid_tomb.pageNum, arr + offset, sizeof(int));
		memcpy(&rid_tomb.slotNum, arr + offset + sizeof(int), sizeof(int));

		return readRecord(fileHandle, recordDescriptor, rid_tomb, data);
	}

	memcpy(data, arr + offset, nullfieldindicator);

	memcpy(nullsIndicator, arr + offset, nullfieldindicator);

	offset += nullfieldindicator;

	int dataOffset = nullfieldindicator;
	int strPos = 0;

	for (loop = 0; (unsigned)loop < recordDescriptor.size(); loop++) {
		nullBit = nullsIndicator[0 + (loop / CHAR_BIT)]
				& (1 << (7 - (loop % CHAR_BIT)));
		if (!nullBit) {
			if (recordDescriptor[loop].type == TypeVarChar) {
				memcpy(&strPos, arr + offset, sizeof(int));
				short strLength = 0;
				memcpy(&strLength, arr + nextRecordPos + strPos, sizeof(short));
				strPos += sizeof(short);
				int intStrLength = strLength;
				memcpy((char *) data + dataOffset, &intStrLength, sizeof(int));
				dataOffset += sizeof(int);
				memcpy((char *) data + dataOffset, arr + nextRecordPos + strPos,
						intStrLength);
				dataOffset += intStrLength;
				offset += sizeof(int);

			} else if (recordDescriptor[loop].type == TypeReal) {
				int strPos = 0;
				memcpy(&strPos, arr + offset, sizeof(int));
				memcpy((char *) data + dataOffset, arr + nextRecordPos + strPos,
						sizeof(float));
				dataOffset += sizeof(float);
				offset += sizeof(int);

			} else if (recordDescriptor[loop].type == TypeInt) {
				int strPos = 0;
				memcpy(&strPos, arr + offset, sizeof(int));
				memcpy((char *) data + dataOffset, arr + nextRecordPos + strPos,
						sizeof(int));
				dataOffset += sizeof(int);
				offset += sizeof(int);

			}

		}
	}
	//free(arr);
	return 0;
}

RC RecordBasedFileManager::printRecord(
		const vector<Attribute> &recordDescriptor, const void *data) {

	int nullfieldindicator, loop, offset1, name_length;
	bool nullBit = false;
	int intVar;
	float floatVar;
	nullfieldindicator = ceil((double) recordDescriptor.size() / CHAR_BIT);
	unsigned char *nullsIndicator = (unsigned char *) malloc(
			nullfieldindicator);
	memcpy((char *) nullsIndicator, (char *) data, nullfieldindicator);
	offset1 = nullfieldindicator;

	for (loop = 0; (unsigned)loop < recordDescriptor.size(); loop++) {
		nullBit = nullsIndicator[0 + (loop / CHAR_BIT)]
				& (1 << (7 - (loop % CHAR_BIT)));
		if (!nullBit) {
			if (recordDescriptor[loop].type == TypeVarChar) {
				memcpy(&name_length, (char *) data + offset1, sizeof(int));
				offset1 += sizeof(int);
				char *temp_data = (char *) malloc(name_length + 1);

				memcpy((char *) temp_data, (char *) data + offset1,
						name_length);
				offset1 += name_length;
				temp_data[name_length] = '\0';
				cout << recordDescriptor[loop].name << ": " << temp_data
						<< "\t";

			} else if (recordDescriptor[loop].type == TypeInt) {
				memcpy(&intVar, (char *) data + offset1, sizeof(int));
				offset1 += sizeof(int);

				cout << recordDescriptor[loop].name << ": " << intVar << "\t";

			} else if (recordDescriptor[loop].type == TypeReal) {
				memcpy(&floatVar, (char *) data + offset1, sizeof(int));
				offset1 += sizeof(float);

				cout << recordDescriptor[loop].name << ": " << floatVar << "\t";

			}

		} else {
			cout << recordDescriptor[loop].name << ": " << "NULL" << "\t";
		}
	}
	cout << endl;
	return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid) {
	int offset_count = 0;
	int nil_value = -1;

	int nullfieldindicator, loop;
	char *arr = (char *) malloc(4096);
	fileHandle.readPage(rid.pageNum, arr);

	int slotOffset = 4080 - (rid.slotNum * sizeof(int));
	bool nullBit = false;
	//cout << slotOffset << " haha";
	nullfieldindicator = ceil((double) recordDescriptor.size() / CHAR_BIT);
	unsigned char *nullsIndicator = (unsigned char *) malloc(
			nullfieldindicator);

	int offset = 0;
	memcpy(&offset, arr + slotOffset, sizeof(int));

	if (offset == -1) {
		return -1;
	}
	int nextRecordPos = offset;

	char *record = (char *) malloc(sizeof(char));
	memcpy(record, arr + offset, sizeof(char));
	offset += sizeof(char);
	offset_count += sizeof(char);

	bool checkBit = record[0] & (1 << 7);
	if (checkBit == true) {
		RID rid_tomb;
		memcpy(&rid_tomb.pageNum, arr + offset, sizeof(int));
		memcpy(&rid_tomb.slotNum, arr + offset + sizeof(int), sizeof(int));

		int nextLoc = 0;
		memcpy(&nextLoc, arr + 4088, sizeof(int));

		int nextRecordOffset = nextRecordPos + 9;
		int bytesMove = nextLoc - nextRecordOffset;
		//cout << " moved " << bytesMove << endl;
		memmove(arr + nextRecordPos, arr + nextRecordOffset, bytesMove);

		int totalSlots = *(int *) (arr + 4084);

		for (int i = 0; i < totalSlots; i++) {
			int nextSlot = 4080;
			nextSlot -= (sizeof(int) * i);
			//cout << nextRecordPos << "record offset" << endl;
			if ((*(int *) (arr + nextSlot)) > nextRecordPos) {
				//cout << *(int *) (arr + nextSlot) << " should be greater "
					//	<< endl;
				*(int *) (arr + nextSlot) = *(int *) (arr + nextSlot) - 9;

				//cout << "updated address " << *(int *) (arr + nextSlot)
					//	<< " by " << " 9 " << endl;
			}

		}

		*(int *) (arr + 4092) += 9;
		*(int *) (arr + 4088) -= 9;
		memcpy(arr + slotOffset, &nil_value, sizeof(int));

		fileHandle.writePage(rid.pageNum, arr);

		return deleteRecord(fileHandle, recordDescriptor, rid_tomb);
		//////////////////

	}

	memcpy(nullsIndicator, arr + offset, nullfieldindicator);
	offset += nullfieldindicator;
	offset_count += nullfieldindicator;

	for (loop = 0; (unsigned)loop < recordDescriptor.size(); loop++) {
		nullBit = nullsIndicator[0 + (loop / CHAR_BIT)]
				& (1 << (7 - (loop % CHAR_BIT)));
		if (!nullBit) {
			int strPos = 0;
			//cout << " repeat ";
			if (recordDescriptor[loop].type == TypeVarChar) {
				memcpy(&strPos, arr + offset, sizeof(int));
				short strLength = 0;
				memcpy(&strLength, arr + nextRecordPos + strPos, sizeof(short));
				strPos += sizeof(short);
				offset_count += sizeof(int);
				int intStrLength = strLength;
				//cout << "string length " << intStrLength << endl;
				offset_count += sizeof(short);
				offset_count += intStrLength;
				offset += sizeof(int);
				//cout << "count " << offset_count << endl;

			} else if (recordDescriptor[loop].type == TypeReal) {
				offset += sizeof(int);
				offset_count += sizeof(int);
				offset_count += sizeof(float);
				//cout << "count " << offset_count << endl;

			} else if (recordDescriptor[loop].type == TypeInt) {
				offset += sizeof(int);
				offset_count += sizeof(int);
				offset_count += sizeof(int);
				//cout << "count " << offset_count << endl;

			}

		}
	}

	//cout << "data offset : " << offset_count << endl;
	//free(arr);
	int nextLoc = 0;
	memcpy(&nextLoc, arr + 4088, sizeof(int));

	int nextRecordOffset = nextRecordPos + offset_count;
	int bytesMove = nextLoc - nextRecordOffset;
	//cout << " moved " << bytesMove << endl;
	memmove(arr + nextRecordPos, arr + nextRecordOffset, bytesMove);

	int totalSlots = *(int *) (arr + 4084);

	for (int i = 0; i < totalSlots; i++) {
		int nextSlot = 4080;
		nextSlot -= (sizeof(int) * i);
		//cout << nextRecordPos << "record offset" << endl;
		if ((*(int *) (arr + nextSlot)) > nextRecordPos) {
			//cout << *(int *) (arr + nextSlot) << " should be greater " << endl;
			*(int *) (arr + nextSlot) = *(int *) (arr + nextSlot)
					- offset_count;

			//cout << "updated address " << *(int *) (arr + nextSlot) << " by "
				//	<< offset_count << endl;
		}

	}

	*(int *) (arr + 4092) += offset_count;
	*(int *) (arr + 4088) -= offset_count;
	memcpy(arr + slotOffset, &nil_value, sizeof(int));

	fileHandle.writePage(rid.pageNum, arr);
	return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const void *data,
		const RID &rid) {

	bool nullBit = false;
	int pointersNeeded = 0;
	int nullDescBytes = ceil((double) recordDescriptor.size() / CHAR_BIT);

	for (int j = 0; (unsigned)j < recordDescriptor.size(); j++) {
		nullBit = (((char *) data)[0 + (j / CHAR_BIT)])
				& (1 << (7 - (j % CHAR_BIT)));
		if (!nullBit) {
			pointersNeeded += 1;
		}
	}

	int varCharCount = 0;

	int offset = nullDescBytes;
	int ptr = 0;
	char *arr = (char *) malloc(4096);
	char *record = (char *) malloc(sizeof(char));
	memset(record, 0, sizeof(char));
	memcpy(arr, record, sizeof(char));
	memcpy(arr + sizeof(char), data, nullDescBytes);
	ptr += sizeof(char);
	ptr += nullDescBytes;
	int nullCount = 0;
	ptr += pointersNeeded * sizeof(int);
	for (int j = 0; (unsigned)j < recordDescriptor.size(); j++) {

		nullBit = (((char *) data)[0 + (j / CHAR_BIT)])
				& (1 << (7 - (j % CHAR_BIT)));

		if (!nullBit) {

			switch (recordDescriptor[j].type) {

			case TypeVarChar: {
				int g1;
				memcpy(&g1, (char *) data + offset, sizeof(int));
				short shortVal = g1;
				memcpy(arr + ptr, &shortVal, sizeof(short));

				memcpy(
						arr + sizeof(char) + nullDescBytes
								+ (nullCount * sizeof(int)), &ptr, sizeof(int));
				ptr += sizeof(short);
				offset = offset + sizeof(int);

				memcpy(arr + ptr, (char *) data + offset, g1);

				offset = offset + g1;
				ptr += g1;
				varCharCount += 1;
				break;
			}
			case TypeInt: {
				memcpy(
						arr + sizeof(char) + nullDescBytes
								+ (nullCount * sizeof(int)), &ptr, sizeof(int));

				memcpy(arr + ptr, (char *) data + offset, sizeof(int));
				offset += sizeof(int);
				ptr += sizeof(int);
				break;
			}
			case TypeReal: {
				memcpy(
						arr + sizeof(bool) + nullDescBytes
								+ (nullCount * sizeof(int)), &ptr, sizeof(int));

				memcpy(arr + ptr, (char *) data + offset, sizeof(float));
				offset += sizeof(float);
				ptr += sizeof(float);
				break;
			}
			}
			nullCount += 1;
		}

	}

	//cout << " ptr value " << ptr << endl;
//	int totalBytesNeeded = offset + (pointersNeeded * sizeof(int)) + sizeof(bool)
//			- (varCharCount * (sizeof(int) - sizeof(short)));
//	cout << totalBytesNeeded << " needed bytes" << endl;

	int offset_count = 0;
	int nullfieldindicator, loop;
	char *arr1 = (char *) malloc(4096);
	fileHandle.readPage(rid.pageNum, arr1);

	int slotOffset = 4080 - (rid.slotNum * sizeof(int));
	nullBit = false;
	nullfieldindicator = ceil((double) recordDescriptor.size() / CHAR_BIT);
	unsigned char *nullsIndicator = (unsigned char *) malloc(
			nullfieldindicator);

	offset = 0;
	memcpy(&offset, arr1 + slotOffset, sizeof(int));
	if (offset == -1) {
		return -1;
	}

	int nextRecordPos = offset;

	record = (char *) malloc(sizeof(char));
	memcpy(record, arr1 + offset, sizeof(char));
	offset += sizeof(char);
	offset_count += sizeof(char);

	bool checkBit = record[0] & (1 << 7);
	if (checkBit == true) {
		RID rid_tomb;
		int offset_count5 = 0;
		memcpy(&rid_tomb.pageNum, arr1 + offset, sizeof(int));
		memcpy(&rid_tomb.slotNum, arr1 + offset + sizeof(int), sizeof(int));
		int offset5 = 0;

		char *arr5 = (char *) malloc(4096);
		fileHandle.readPage(rid_tomb.pageNum, arr5);

		int slotOffset = 4080 - (rid_tomb.slotNum * sizeof(int));
		int recordPosition;
		memcpy(&recordPosition, arr5 + slotOffset, sizeof(int));
		offset5 = recordPosition;

		char *record5 = (char *) malloc(sizeof(char));
		memcpy(record5, arr5 + offset5, sizeof(char));
		offset5 += sizeof(char);
		offset_count5 += sizeof(char);

		unsigned char *nullsIndicator = (unsigned char *) malloc(
				nullfieldindicator);

		memcpy(nullsIndicator, arr5 + offset5, nullfieldindicator);
		offset5 += nullfieldindicator;
		offset_count5 += nullfieldindicator;

		for (loop = 0; (unsigned)loop < recordDescriptor.size(); loop++) {
			nullBit = nullsIndicator[0 + (loop / CHAR_BIT)]
					& (1 << (7 - (loop % CHAR_BIT)));
			if (!nullBit) {
				int strPos = 0;
				if (recordDescriptor[loop].type == TypeVarChar) {
					memcpy(&strPos, arr5 + offset5, sizeof(int));
					short strLength = 0;
					memcpy(&strLength, arr5 + recordPosition + strPos,
							sizeof(short));
					strPos += sizeof(short);
					offset_count5 += sizeof(int);
					int intStrLength = strLength;
					offset_count5 += sizeof(short);
					offset_count5 += intStrLength;
					offset5 += sizeof(int);

				} else if (recordDescriptor[loop].type == TypeReal) {
					offset5 += sizeof(int);
					offset_count5 += sizeof(int);
					offset_count5 += sizeof(float);

				} else if (recordDescriptor[loop].type == TypeInt) {
					offset5 += sizeof(int);
					offset_count5 += sizeof(int);
					offset_count5 += sizeof(int);

				}

			}
		}

		//cout << offset_count5 << " size of record in other page";
		int freeSpace = *(int *) (arr1 + 4092);

		if (offset_count5 <= freeSpace + 9) {

			int nextLoc = 0;
			memcpy(&nextLoc, arr1 + 4088, sizeof(int));

			int nextRecordOffset = nextRecordPos + 9;
			int bytesMove = nextLoc - nextRecordOffset;
			//cout << " moved " << bytesMove << endl;
			memmove(arr1 + nextRecordPos, arr1 + nextRecordOffset, bytesMove);

			int totalSlots = *(int *) (arr1 + 4084);

			for (int i = 0; i < totalSlots; i++) {
				int nextSlot = 4080;
				nextSlot -= (sizeof(int) * i);
				//cout << nextRecordPos << "record offset" << endl;
				if ((*(int *) (arr1 + nextSlot)) > nextRecordPos) {
					//cout << *(int *) (arr1 + nextSlot) << " should be greater "
							//<< endl;
					*(int *) (arr1 + nextSlot) = *(int *) (arr1 + nextSlot) - 9;
					//cout << "updated address " << *(int *) (arr1 + nextSlot)
					//		<< " by 9" << endl;
				}

			}

			memcpy(arr1 + nextLoc - 9, arr5, offset_count5);
			char *newRecord = (char *)malloc(sizeof(char));
			memset(newRecord, 0, sizeof(char));
			memcpy(arr1 + nextLoc - 9, newRecord, sizeof(char));
			*(int *) (arr1 + 4092) -= (offset_count5 - 9);
			*(int *) (arr1 + 4088) += (offset_count5 - 9);
			int newOffset = nextLoc - 9;
			memcpy(arr1 + slotOffset, &newOffset, sizeof(int));

			fileHandle.writePage(rid.pageNum, arr1);

		} else {
			RID rid1;
			insertRecord(fileHandle, recordDescriptor, data, rid1, 1);

			int pageNum = rid1.pageNum;
			int slotNum = rid1.slotNum;
			memset(record, 0, sizeof(char));
			record[0] |= (1 << 7);
			//cout << " record tombstone here: ";

			int offsetRID = 0;
			memcpy(arr1 + nextRecordPos, record, sizeof(char));
			offsetRID += sizeof(char);
			memcpy(arr1 + nextRecordPos + offsetRID, &pageNum, sizeof(int));
			offsetRID += sizeof(int);
			memcpy(arr1 + nextRecordPos + offsetRID, &slotNum, sizeof(int));
			offsetRID += sizeof(int);

			fileHandle.writePage(rid.pageNum, arr1);

		}
		return deleteRecord(fileHandle, recordDescriptor, rid_tomb);
	}
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	memcpy(nullsIndicator, arr1 + offset, nullfieldindicator);
	offset += nullfieldindicator;
	offset_count += nullfieldindicator;
	for (loop = 0; (unsigned)loop < recordDescriptor.size(); loop++) {
		nullBit = nullsIndicator[0 + (loop / CHAR_BIT)]
				& (1 << (7 - (loop % CHAR_BIT)));
		if (!nullBit) {
			if (recordDescriptor[loop].type == TypeVarChar) {
				int strPos = 0;
				memcpy(&strPos, arr1 + offset, sizeof(int));
				short strLength = 0;
				memcpy(&strLength, arr1 + nextRecordPos + strPos,
						sizeof(short));
				strPos += sizeof(short);
				offset_count += sizeof(int);
				int intStrLength = strLength;
				offset_count += sizeof(short);
				offset_count += intStrLength;
				offset += sizeof(int);

			} else if (recordDescriptor[loop].type == TypeReal) {
				offset += sizeof(int);
				offset_count += sizeof(int);
				offset_count += sizeof(float);

			} else if (recordDescriptor[loop].type == TypeInt) {
				offset += sizeof(int);
				offset_count += sizeof(int);
				offset_count += sizeof(int);

			}

		}
	}

	//cout << "data offset : " << offset_count << endl;

	if (ptr <= offset_count) {
		if (ptr == offset_count) {
			//cout << "goes here : ";

			memcpy(arr1 + nextRecordPos, arr, ptr);
			fileHandle.writePage(rid.pageNum, arr1);
		} else {

			//cout << "goes there : ";
			memcpy(arr1 + nextRecordPos, arr, ptr);
			int diff = offset_count - ptr;

			int nextLoc = 0;
			memcpy(&nextLoc, arr1 + 4088, sizeof(int));

			int nextRecordOffset = nextRecordPos + offset_count;
			int bytesMove = nextLoc - nextRecordOffset;

			memmove(arr1 + nextRecordPos + ptr, arr1 + nextRecordOffset,
					bytesMove);

			int totalSlots = *(int *) (arr1 + 4084);

			for (int i = 0; i < totalSlots; i++) {
				int nextSlot = 4080;
				nextSlot -= (sizeof(int) * i);
				//cout << nextRecordPos << "record offset" << endl;
				if ((*(int *) (arr1 + nextSlot)) > nextRecordPos) {
					//cout << *(int *) (arr1 + nextSlot) << " should be greater "
							//<< endl;
					*(int *) (arr1 + nextSlot) = *(int *) (arr1 + nextSlot)
							- diff;
				}

				//cout << "updated address " << *(int *) (arr1 + nextSlot)
				//		<< " by " << offset_count << endl;
			}
			*(int *) (arr1 + 4092) += diff;
			*(int *) (arr1 + 4088) -= diff;

			fileHandle.writePage(rid.pageNum, arr1);
		}
	} else {
		int freeSpace = *(int *) (arr1 + 4092);
		int diff = ptr - offset_count;
		if (diff <= freeSpace) {

			int nextLoc = 0;
			memcpy(&nextLoc, arr1 + 4088, sizeof(int));

			int nextRecordOffset = nextRecordPos + offset_count;
			int bytesMove = nextLoc - nextRecordOffset;
			//cout << " moved " << bytesMove << endl;
			memmove(arr1 + nextRecordPos, arr1 + nextRecordOffset, bytesMove);

			int totalSlots = *(int *) (arr1 + 4084);

			for (int i = 0; i < totalSlots; i++) {
				int nextSlot = 4080;
				nextSlot -= (sizeof(int) * i);
				//cout << nextRecordPos << "record offset" << endl;
				if ((*(int *) (arr1 + nextSlot)) > nextRecordPos) {
					//cout << *(int *) (arr1 + nextSlot) << " should be greater "
					//		<< endl;
					*(int *) (arr1 + nextSlot) = *(int *) (arr1 + nextSlot)
							- offset_count;
					//cout << "updated address " << *(int *) (arr1 + nextSlot)
						//	<< " by " << offset_count << endl;
				}

			}

			memcpy(arr1 + nextLoc - offset_count, arr, ptr);

			*(int *) (arr1 + 4092) -= diff;
			*(int *) (arr1 + 4088) += diff;
			int newOffset = nextLoc - offset_count;
			memcpy(arr1 + slotOffset, &newOffset, sizeof(int));

			fileHandle.writePage(rid.pageNum, arr1);

		} else {
			RID rid1;
			insertRecord(fileHandle, recordDescriptor, data, rid1, 1);
			int pageNum = rid1.pageNum;
			int slotNum = rid1.slotNum;
			char * record = (char *) malloc(sizeof(char));
			memset(record, 0, sizeof(char));
			record[0] |= (1 << 7);
			//cout << " record tombstone here: ";

			int offsetRID = 0;
			memcpy(arr1 + nextRecordPos, record, sizeof(char));
			offsetRID += sizeof(char);
			memcpy(arr1 + nextRecordPos + offsetRID, &pageNum, sizeof(int));
			offsetRID += sizeof(int);
			memcpy(arr1 + nextRecordPos + offsetRID, &slotNum, sizeof(int));
			offsetRID += sizeof(int);

			int diff = offset_count - offsetRID;

			int nextLoc = 0;
			memcpy(&nextLoc, arr1 + 4088, sizeof(int));

			int nextRecordOffset = nextRecordPos + offset_count;
			int bytesMove = nextLoc - nextRecordOffset;

			memmove(arr1 + nextRecordPos + offsetRID, arr1 + nextRecordOffset,
					bytesMove);

			int totalSlots = *(int *) (arr1 + 4084);

			for (int i = 0; i < totalSlots; i++) {
				int nextSlot = 4080;
				nextSlot -= (sizeof(int) * i);
				//cout << nextRecordPos << "record offset" << endl;
				if ((*(int *) (arr1 + nextSlot)) > nextRecordPos) {
					//cout << *(int *) (arr1 + nextSlot) << " should be greater "
					//		<< endl;
					*(int *) (arr1 + nextSlot) = *(int *) (arr1 + nextSlot)
							- diff;

					//cout << "updated address " << *(int *) (arr1 + nextSlot)
					//		<< " by " << offset_count << endl;
				}

			}
			*(int *) (arr1 + 4092) += diff;
			*(int *) (arr1 + 4088) -= diff;

			fileHandle.writePage(rid.pageNum, arr1);

		}

	}

	return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid,
		const string &attributeName, void *data) {

	int nullfieldindicator;
	char *arr = (char *) malloc(4096);
	fileHandle.readPage(rid.pageNum, arr);

	int slotOffset = 4080 - (rid.slotNum * sizeof(int));
	bool nullBit = false;

	nullfieldindicator = ceil((double) recordDescriptor.size() / CHAR_BIT);

	unsigned char *nullsIndicator = (unsigned char *) malloc(
			nullfieldindicator);

	int offset = 0;

	memcpy(&offset, arr + slotOffset, sizeof(int));
	unsigned char *nullsIndicator1 = (unsigned char *) malloc(sizeof(char));

	if (offset == -1) {
		nullsIndicator1[0] = 255; // 10000000
		memcpy(data, nullsIndicator1, sizeof(char));
		return -1;
	}
	int nextRecordPos = offset;
	char *record = (char *) malloc(sizeof(char));
	memcpy(record, arr + offset, sizeof(char));
	offset += sizeof(char);

	bool checkBit = record[0] & (1 << 7);
	if (checkBit == true) {
		RID rid_tomb;
		memcpy(&rid_tomb.pageNum, arr + offset, sizeof(int));
		memcpy(&rid_tomb.slotNum, arr + offset + sizeof(int), sizeof(int));

		return readAttribute(fileHandle, recordDescriptor, rid_tomb,
				attributeName, data);
	}

	memcpy(nullsIndicator, arr + offset, nullfieldindicator);

	offset += nullfieldindicator;

	int pointers = 0;

	int index = -1;
	for (int k = 0; (unsigned)k < recordDescriptor.size(); k++) {
		nullBit = (((char *) nullsIndicator)[0 + (k / CHAR_BIT)])
				& (1 << (7 - (k % CHAR_BIT)));

		if (!nullBit) {
			if (recordDescriptor[k].name == attributeName) {
				index = k;
				break;
			} else
				pointers += sizeof(int);
		}

	}

	nullBit = nullsIndicator[0 + (index / CHAR_BIT)]
			& (1 << (7 - (index % CHAR_BIT)));
	if (!nullBit) {
		nullsIndicator1[0] = 0; // 00000000
		memcpy(data, nullsIndicator1, sizeof(char));

		if (recordDescriptor[index].type == TypeVarChar) {
			int strPos = 0;
			memcpy(&strPos, arr + offset + pointers, sizeof(int));
			short strLength = 0;
			memcpy(&strLength, arr + nextRecordPos + strPos, sizeof(short));
			strPos += sizeof(short);

			int intStrLength = strLength;
			memcpy((char *) data + sizeof(char), &intStrLength, sizeof(int));
			memcpy((char *) data + sizeof(char) + sizeof(int),
					arr + nextRecordPos + strPos, intStrLength);

		} else if (recordDescriptor[index].type == TypeReal) {
			int strPos = 0;
			memcpy(&strPos, arr + offset + pointers, sizeof(int));
			memcpy((char *) data + sizeof(char), arr + nextRecordPos + strPos,
					sizeof(float));
		} else if (recordDescriptor[index].type == TypeInt) {
			int strPos = 0;
			memcpy(&strPos, arr + offset + pointers, sizeof(int));
			memcpy((char *) data + sizeof(char), arr + nextRecordPos + strPos,
					sizeof(int));
		}

	} else {
		nullsIndicator1[0] = 128; // 10000000
		memcpy(data, nullsIndicator1, sizeof(char));
	}

	return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor,
		const string &conditionAttribute, const CompOp compOp, // comparision type such as "<" and "="
		const void *value, // used in the comparison
		const vector<string> &attributeNames, // a list of projected attributes
		RBFM_ScanIterator &rbfm_ScanIterator) {

	rbfm_ScanIterator.recordDescriptor = recordDescriptor;
	rbfm_ScanIterator.conditionAttribute = conditionAttribute;
	rbfm_ScanIterator.compOp = compOp;
	rbfm_ScanIterator.value = value;
	rbfm_ScanIterator.attributeNames = attributeNames;
	rbfm_ScanIterator.fileHandle = fileHandle;
	rbfm_ScanIterator.test.pageNum = 0;
	rbfm_ScanIterator.test.slotNum = -1;

//	int x = rbfm_ScanIterator.fileHandle.getNumberOfPages();
	return 0;
}
