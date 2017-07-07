#include "qe.h"

BNLJoin::~BNLJoin(){
	free(rightData);
};

RC joinData(void* leftData, void* rightData, void* arr,
		vector<Attribute> leftAttrs, vector<Attribute> rightAttrs){
	int i;
	bool nullBit = false;
	int lSize = leftAttrs.size();
	int rSize = rightAttrs.size();

	int totalSize = lSize + rSize;
	int totalNullFieldIndicator = ceil((double)totalSize/CHAR_BIT);

	int lNullFieldIndicator = ceil((double)lSize/CHAR_BIT);
	int lOffset = lNullFieldIndicator;

	int rNullFieldIndicator = ceil((double)rSize/CHAR_BIT);
	int rOffset = rNullFieldIndicator;

	unsigned char *nullsIndicator = (unsigned char *) malloc(totalNullFieldIndicator);
	memset(nullsIndicator, 0, totalNullFieldIndicator);


	for (i=0; i<lSize; i++){
		nullBit = (((char *) leftData)[(i / CHAR_BIT)])
                				& (1 << (7 - (i % CHAR_BIT)));

		if (!nullBit) {

			if (leftAttrs[i].type == TypeInt){
				lOffset += sizeof(int);
			}
			else if (leftAttrs[i].type == TypeReal){
				lOffset += sizeof(float);
			}
			else {
				int len;
				memcpy(&len, (char *)leftData + lOffset, sizeof(int));
				lOffset += len + sizeof(int);
			}
		}else {
			nullsIndicator[(i / CHAR_BIT)] = nullsIndicator[(i / CHAR_BIT)] | (1 << (7 - (i % CHAR_BIT)));
		}
	}

	for (i=0; i<rSize; i++){
		nullBit = (((char *) rightData)[(i / CHAR_BIT)])
                    				& (1 << (7 - (i % CHAR_BIT)));

		if (!nullBit) {

			if (rightAttrs[i].type == TypeInt){
				rOffset += sizeof(int);
			}
			else if (rightAttrs[i].type == TypeReal){
				rOffset += sizeof(float);
			}
			else {
				int len;
				memcpy(&len, (char *)rightData + rOffset, sizeof(int));
				rOffset += len + sizeof(int);
			}
		}else {
			nullsIndicator[(lSize + i) / CHAR_BIT] = nullsIndicator[(lSize + i) / CHAR_BIT] | (1 << (7 - ((lSize + i) % CHAR_BIT)));
		}
	}

	int joinOffset = 0;
	memcpy((char *)arr, nullsIndicator, totalNullFieldIndicator);
	joinOffset += totalNullFieldIndicator;

	int lCopySize = lOffset - lNullFieldIndicator;
	memcpy((char *)arr + joinOffset, (char *)leftData + lNullFieldIndicator, lCopySize);
	joinOffset += lCopySize;

	int rCopySize = rOffset - rNullFieldIndicator;
	memcpy((char *)arr + joinOffset, (char *)rightData + rNullFieldIndicator, rCopySize);

	free(nullsIndicator);
	return 0;
}

BNLJoin::BNLJoin(Iterator *leftIn,            // Iterator of input R
		TableScan *rightIn,           // TableScan Iterator of input S
		const Condition &condition,   // Join condition
		const unsigned numPages       // # of pages that can be loaded into memory,
		//   i.e., memory block size (decided by the optimizer)
){
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->condition = condition;
	this->numPages = numPages;

	leftIn->getAttributes(leftAttrs);
	rightIn->getAttributes(rightAttrs);

	initializeHT();
};

void clearHashTable(map<int, vector<void *> >& intHT, map<float, vector<void *> >& floatHT, map<string, vector<void *> >& strHT){
	for(map<int, vector<void*> >::iterator it = intHT.begin(); it != intHT.end(); it++){
		vector<void*> records = it->second;
		for (int i = 0; (unsigned)i < records.size(); i++) {
			free(records[i]);
		}
	}
	for(std::map<float, vector<void*> >::iterator it = floatHT.begin(); it != floatHT.end(); it++){
		vector<void*> records = it->second;
		for (int i = 0; (unsigned)i < records.size(); i++) {
			free(records[i]);
		}
	}
	for(std::map<string, vector<void*> >::iterator it = strHT.begin(); it != strHT.end(); it++){
		vector<void*> records = it->second;
		for (int i = 0; (unsigned)i < records.size(); i++) {
			free(records[i]);
		}
	}
	intHT.clear();
	floatHT.clear();
	strHT.clear();
}

RC BNLJoin::initializeHT(){
	bool nullBit = false;
	sizeHT = 0;
	clearHashTable(intHT, floatHT, strHT);

	if (leftDone) return QE_EOF;

	bool IsHTFull = false;

	do{
		void* leftData = malloc(PAGE_SIZE);
		int retVal = leftIn->getNextTuple(leftData);
		if (retVal == -1){
			leftDone = true;
			free(leftData);

			if (intHT.size() > 0 || floatHT.size() > 0 || strHT.size() > 0)
				return 0;
			else return QE_EOF;
		}

		int nullFieldIndicator = ceil((double)leftAttrs.size() / CHAR_BIT);
		int offset = nullFieldIndicator;

		for (int i = 0; (unsigned)i < leftAttrs.size(); i++) {

			nullBit = (((char *) leftData)[(i / CHAR_BIT)])
                                            				& (1 << (7 - (i % CHAR_BIT)));
			if(!nullBit){
				if(leftAttrs[i].type == TypeInt || leftAttrs[i].type == TypeReal){
					offset += sizeof(int);
				}else if(leftAttrs[i].type == TypeVarChar){
					int len;
					memcpy(&len, (char *)leftData + offset, sizeof(int));
					offset += sizeof(int) + len;
				}
			}
		}

		int leftOffset = nullFieldIndicator;
		for (int i = 0; (unsigned)i < leftAttrs.size(); i++) {

			nullBit = (((char *) leftData)[(i / CHAR_BIT)])
                                				& (1 << (7 - (i % CHAR_BIT)));
			if(leftAttrs[i].name == condition.lhsAttr){

				if (!nullBit) {
					if(leftAttrs[i].type == TypeInt){
						int key;
						memcpy(&key, (char *)leftData + leftOffset, sizeof(int));

						map<int, vector<void *> >::iterator it = intHT.find(key);
						if(it == intHT.end()){
							vector<void *> records;
							records.push_back(leftData);
							intHT.insert(pair<int, vector<void *> >(key, records));
						}else{
							vector<void *> records = it->second;
							records.push_back(leftData);
							intHT.insert(pair<int, vector<void *> >(key, records));
						}

						sizeHT += offset;
						if(sizeHT >= PAGE_SIZE * numPages){
							IsHTFull  = true;
						}

					}else if(leftAttrs[i].type == TypeReal){
						float key;
						memcpy(&key, (char *)leftData + leftOffset, sizeof(float));

						map<float, vector<void*> >::iterator it = floatHT.find(key);
						if(it == floatHT.end()){
							vector<void*> records;
							records.push_back(leftData);
							floatHT.insert(pair<float, vector<void *> >(key, records));
						}else{
							vector<void*> records = it->second;
							records.push_back(leftData);
							floatHT.insert(pair<float, vector<void *> >(key, records));
						}

						sizeHT += offset;
						if(sizeHT >= PAGE_SIZE * numPages){
							IsHTFull  = true;
						}
					}else if(leftAttrs[i].type == TypeVarChar){

						int len;
						memcpy(&len, (char *)leftData + leftOffset, sizeof(int));
						string key;
						key.resize(len);
						memcpy(&key[0], (char *)leftData + leftOffset + sizeof(int), len);

						map<string, vector<void*> >::iterator it = strHT.find(key);
						if(it == strHT.end()){
							vector<void*> records;
							records.push_back(leftData);
							strHT.insert(pair<string, vector<void *> >(key, records));
						}else{
							vector<void*> records = it->second;
							records.push_back(leftData);
							strHT.insert(pair<string, vector<void *> >(key, records));
						}

						sizeHT += offset;
						if(sizeHT >= PAGE_SIZE * numPages){
							IsHTFull  = true;
						}
					}
				}
				break;
			}else{
				if(!nullBit){
					if(leftAttrs[i].type == TypeInt || leftAttrs[i].type == TypeReal){
						leftOffset += sizeof(int);
					}else if(leftAttrs[i].type == TypeVarChar){
						int len;
						memcpy(&len, (char *)leftData + leftOffset, sizeof(int));
						leftOffset += sizeof(int) + len;
					}
				}
			}
		}

	}while(IsHTFull);
	return 0;
};

void BNLJoin::getAttributes(vector<Attribute> &attrs) const{
	int i;

	for (i = 0; (unsigned)i < leftAttrs.size(); i++) {
		attrs.push_back(leftAttrs[i]);
	}

	for(i = 0; (unsigned)i < rightAttrs.size(); i++){
		attrs.push_back(rightAttrs[i]);
	}
};

RC BNLJoin::getNextTuple(void *data){
	if(leftMatchRecords.size() > (unsigned)leftMatchOffset){
		void* leftData = leftMatchRecords[leftMatchOffset++];
		joinData(leftData, rightData, data, leftAttrs, rightAttrs);
		return 0;
	}
	bool match = false;
	do{
		void* rightData = malloc(PAGE_SIZE);
		int retVal2 = rightIn->getNextTuple(rightData);
		if(retVal2 == QE_EOF){
			int retVal = initializeHT();
			if(retVal == QE_EOF){
				free(rightData);
				return QE_EOF;
			}else{
				rightIn->setIterator();
			}
		}

		int nullFieldIndicator = ceil((double)rightAttrs.size() / CHAR_BIT);
		int rightOffset = nullFieldIndicator;

		for (int i = 0; (unsigned)i < rightAttrs.size(); i++) {
			bool nullBit = ((char*)rightData)[i / 8] & ( 1 << ( 7 - ( i % 8 )));
			if(rightAttrs[i].name == condition.rhsAttr){
				if(nullBit){
					match = false;
					break;
				}
				if(rightAttrs[i].type == TypeInt){
					int value;
					memcpy(&value, (char *)rightData + rightOffset, sizeof(int));

					map<int, vector<void*> >::iterator it = intHT.find(value);
					if(it != intHT.end()){
						match = true;
						vector<void*> records = it->second;
						if(records.size() > 1){
							leftMatchRecords = records;
							leftMatchOffset = 1;
							memcpy(this->rightData, rightData, PAGE_SIZE);
						}
						void* arr = records[0];
						joinData(arr, rightData, data, leftAttrs, rightAttrs);
						break;
					}else{
						match = false;
						break;
					}
				}else if(rightAttrs[i].type == TypeReal){
					float value;
					memcpy(&value, (char *)rightData + rightOffset, sizeof(float));

					map<float, vector<void*> >::iterator it = floatHT.find(value);
					if(it != floatHT.end()){
						match = true;
						vector<void*> records = it->second;
						if(records.size() > 1){
							leftMatchRecords = records;
							leftMatchOffset = 1;
							memcpy(this->rightData, rightData, PAGE_SIZE);
						}
						void* arr = records[0];
						joinData(arr, rightData, data, leftAttrs, rightAttrs);
						break;
					}else{
						match = false;
						break;
					}
				}else if(rightAttrs[i].type == TypeVarChar){
					int length;
					memcpy(&length, (char *)rightData + rightOffset, sizeof(int));
					string value;
					value.resize(length);
					memcpy(&value[0], (char *)rightData + rightOffset + sizeof(int), length);

					map<string, vector<void*> >::iterator it = strHT.find(value);
					if(it != strHT.end()){
						match = true;
						vector<void*> records = it->second;
						if(records.size() > 1){
							leftMatchRecords = records;
							leftMatchOffset = 1;
							memcpy(this->rightData, rightData, PAGE_SIZE);
						}
						void* arr = records[0];
						joinData(arr, rightData, data, leftAttrs, rightAttrs);
						break;
					}else{
						match = false;
						break;
					}
				}
			}else{
				if(!nullBit){
					if(rightAttrs[i].type == TypeInt || rightAttrs[i].type == TypeReal){
						rightOffset += sizeof(int);
					}else if(rightAttrs[i].type == TypeVarChar){
						int len;
						memcpy(&len, (char *)rightData + rightOffset, sizeof(int));
						rightOffset += sizeof(int) + len;
					}
				}
			}
		}

		free(rightData);
	}while(!match);
	return 0;
};


void INLJoin::setConditionalIterator(CompOp compOp){
	switch (compOp){
	case EQ_OP:
		rightIn->setIterator(conditionKey, conditionKey, true, true);
		break;
	case LT_OP:
		rightIn->setIterator(NULL, conditionKey, true, false);
		break;
	case LE_OP:
		rightIn->setIterator(NULL, conditionKey, true, true);
		break;
	case GT_OP:
		rightIn->setIterator(conditionKey, NULL, false, true);
		break;
	case GE_OP:
		rightIn->setIterator(conditionKey, NULL, true, true);
		break;
	case NO_OP:
		rightIn->setIterator(NULL, NULL, true, true);
		break;
	case NE_OP:
		rightIn->setIterator(NULL, conditionKey, true, false);
		break;
	}
}

INLJoin::INLJoin(Iterator* lIn, IndexScan* rIn, const Condition &cond){
	leftIn = lIn;
	rightIn = rIn;

	leftIn->getAttributes(leftAttrs);
	rightIn->getAttributes(rightAttrs);

	isEOF = false;
	condition = cond;

	leftData = malloc(PAGE_SIZE);
	int retVal = leftIn->getNextTuple(leftData);

	conditionKey = malloc(PAGE_SIZE);
	if(retVal == RM_EOF){
		isEOF = true;
	}
	else{
		isKeyNull = true;
		while(isKeyNull){
			findConditionalKey(leftData, conditionKey, leftAttr, isKeyNull);
			if(isKeyNull)
				leftIn->getNextTuple(leftData);
		}
		setConditionalIterator(condition.op);
		if(condition.op == NE_OP){
			leftJoinNE = true;
		}
	}
}

INLJoin::~INLJoin(){
	free(leftData);
	free(conditionKey);
}

RC INLJoin::findConditionalKey(void* leftData, void* conditionKey, Attribute &leftAttr, bool &isKeyNull){
	int nullBit = false;
	int offset = ceil((double)leftAttrs.size() / CHAR_BIT);

	for(int i = 0; (unsigned)i < leftAttrs.size(); i++){

		nullBit = (((char *) leftData)[(i / CHAR_BIT)])
                                        				& (1 << (7 - (i % CHAR_BIT)));
		if(leftAttrs[i].name == condition.lhsAttr){

			leftAttr.name = leftAttrs[i].name;
			leftAttr.length = leftAttrs[i].length;
			leftAttr.type = leftAttrs[i].type;

			if (!nullBit) {
				if(leftAttrs[i].type == TypeInt){
					int key;
					memcpy(&key, (char*) leftData + offset, sizeof(int));
					memcpy((char*)conditionKey, &key, sizeof(int));
				}
				else if(leftAttrs[i].type == TypeReal){
					float key;
					memcpy(&key, (char*) leftData + offset, sizeof(float));
					memcpy((char*)conditionKey, &key, sizeof(float));
				}
				else if(leftAttrs[i].type == TypeVarChar){
					int len;
					memcpy(&len, (char*) leftData + offset, sizeof(int));
					memcpy((char*)conditionKey, (char*) leftData + offset, sizeof(int) + len);
				}
				isKeyNull = false;
				break;
			} else {
				isKeyNull = true;
				break;
			}

		}
		else{

			if (!nullBit) {
				if(leftAttrs[i].type == TypeInt){
					offset += sizeof(int);
				}
				else if(leftAttrs[i].type == TypeReal){
					offset += sizeof(float);
				}
				else{
					int len;
					memcpy(&len, (char*) leftData + offset, sizeof(int));
					offset += len + sizeof(int);
				}
			}
		}
	}
	return 0;
}

RC INLJoin::getNextTuple(void* data){

	if(isEOF){
		return QE_EOF;
	}

	void* rightData = malloc(PAGE_SIZE);
	bool joinPossible = false;

	while(!isEOF && !joinPossible){
		if(rightIn->getNextTuple(rightData) != RM_EOF){
			joinPossible = true;
			joinData(leftData, rightData, data, leftAttrs, rightAttrs);
		}

		if(!joinPossible){
			if(condition.op == NE_OP){
				if(leftJoinNE == true){
					rightIn->setIterator(conditionKey, NULL, false, true);
				}
				else{
					do{
						RC retVal = leftIn->getNextTuple(leftData);
						if(retVal == RM_EOF){
							isEOF = true;
							break;
						}
						findConditionalKey(leftData, conditionKey, leftAttr, isKeyNull);
					}while(isKeyNull);
					setConditionalIterator(condition.op);
				}
				leftJoinNE = !leftJoinNE;
			}
			else{
				do{
					RC retVal = leftIn->getNextTuple(leftData);
					if(retVal == RM_EOF){
						isEOF = true;
						break;
					}
					findConditionalKey(leftData, conditionKey, leftAttr, isKeyNull);
				}while(isKeyNull && !isEOF);
				if(!isEOF){
					setConditionalIterator(condition.op);
				}
			}
		}
	}

	free(rightData);

	if(isEOF){
		return QE_EOF;
	}

	return 0;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const{
	int i;

	for (i = 0; (unsigned)i < leftAttrs.size(); i++) {
		attrs.push_back(leftAttrs[i]);
	}

	for(i = 0; (unsigned)i < rightAttrs.size(); i++){
		attrs.push_back(rightAttrs[i]);
	}
}

Filter::Filter(Iterator *input,               // Iterator of input R
		const Condition &condition     // Selection condition
)
{
	this->input = input;
	this->condition = condition;

}
bool Filter::checkfilter_condition(void * data,vector<Attribute> &attributes)
{
	bool filter_condition = false;
	//vector<Attribute> attributes;
	//getAttributes(attributes);

	int nullsIndicator = ceil((double)attributes.size() / CHAR_BIT);
	int offset = nullsIndicator;
	if(condition.bRhsIsAttr == false){
		for (int loop = 0; (unsigned)loop < attributes.size(); loop++) {
			bool nullbit = ((char*)data)[loop / 8] & ( 1 << ( 7 - (loop % 8 )));
			if(attributes[loop].name.compare(condition.lhsAttr) == 0){
				if(nullbit){
					if(condition.rhsValue.data == NULL)
						filter_condition = true;
				}
				else if(condition.rhsValue.type == TypeInt){
					int datavalue;
					memcpy(&datavalue, (char *)data + offset, sizeof(int));
					//cout<<"data is "<<datavalue<<endl;
					int filter_value;
					memcpy(&filter_value, (char *)condition.rhsValue.data, sizeof(int));
					switch (condition.op)
					{
					case EQ_OP:
						filter_condition = (datavalue == filter_value) ? 1: 0;
						break;
					case LT_OP:
						filter_condition = (datavalue < filter_value) ? 1: 0;
						break;
					case GT_OP:
						filter_condition = (datavalue > filter_value) ? 1: 0;
						break;
					case LE_OP:
						filter_condition = (datavalue <= filter_value) ? 1: 0;
						break;
					case GE_OP:
						filter_condition = (datavalue >= filter_value) ? 1: 0;
						break;
					case NE_OP:
						filter_condition = (datavalue != filter_value) ? 1: 0;
						break;
					case NO_OP:
						filter_condition = true;
					}
				}else if(condition.rhsValue.type == TypeReal){
					float datavalue;
					memcpy(&datavalue, (char *)data + offset, sizeof(float));
					//cout<<"data is "<<datavalue<<endl;
					float filter_value;
					memcpy(&filter_value, (char *)condition.rhsValue.data, sizeof(float));
					switch (condition.op)
					{
					case EQ_OP:
						filter_condition = (datavalue == filter_value) ? 1: 0;
						break;
					case LT_OP:
						filter_condition = (datavalue < filter_value) ? 1: 0;
						break;
					case GT_OP:
						filter_condition = (datavalue > filter_value) ? 1: 0;
						break;
					case LE_OP:
						filter_condition = (datavalue <= filter_value) ? 1: 0;
						break;
					case GE_OP:
						filter_condition = (datavalue >= filter_value) ? 1: 0;
						break;
					case NE_OP:
						filter_condition = (datavalue != filter_value) ? 1: 0;
						break;
					case NO_OP:
						filter_condition = true;
					}

				}else if(condition.rhsValue.type == TypeVarChar){
					int length;
					memcpy(&length, (char *)data + offset, sizeof(int));
					string datavalue;
					datavalue.resize(length);
					memcpy(&datavalue[0],(char *) data + offset + sizeof(int), length);
					//cout<<"data is "<<datavalue<<endl;
					int length2;
					memcpy(&length2, (char *)condition.rhsValue.data, sizeof(int));
					string filter_value;
					filter_value.resize(length2);
					memcpy(&filter_value[0], (char*) condition.rhsValue.data + sizeof(int), length2);

					switch (condition.op)
					{
					case EQ_OP:
						filter_condition = (datavalue.compare(filter_value) == 0) ? 1: 0;
						break;
					case LT_OP:
						filter_condition = (datavalue.compare(filter_value) < 0) ? 1: 0;
						break;
					case GT_OP:
						filter_condition = (datavalue.compare(filter_value) > 0) ? 1: 0;
						break;
					case LE_OP:
						filter_condition = (datavalue.compare(filter_value) <= 0) ? 1: 0;
						break;
					case GE_OP:
						filter_condition = (datavalue.compare(filter_value) >= 0) ? 1: 0;
						break;
					case NE_OP:
						filter_condition = (datavalue.compare(filter_value) != 0) ? 1: 0;
						break;
					case NO_OP:
						filter_condition = true;
					}
				}
				break;
			}else{

				if(!nullbit)
				{

					if(attributes[loop].type == TypeInt )
						offset += sizeof(int);
					else if(attributes[loop].type == TypeReal )
						offset += sizeof(float);
					else{
						int length;
						memcpy(&length, (char *)data + offset, sizeof(int));
						offset += sizeof(int) + length;
					}
				}
			}
		}
	}
	return filter_condition;

}


RC Filter::getNextTuple(void *data) {

	bool filter_condition = false;
	vector<Attribute> attributes;
	getAttributes(attributes);
	while(filter_condition == false)
	{
		RC rc = input->getNextTuple(data);
		if(rc == RM_EOF)
			return QE_EOF;

		filter_condition = checkfilter_condition(data,attributes);
		//if(filter_condition != false)
		//break;
	}
	return 0;
}

void Filter::getAttributes(vector<Attribute> &attrs) const{
	input->getAttributes(attrs);
}

Project ::Project(Iterator *input,
		const vector<string> &attrNames){
	this->input = input;
	this->attrNames = attrNames;
}
RC Project::getNextTuple(void *data) {
	void* input_data = malloc(PAGE_SIZE);
	RC rc = input->getNextTuple(input_data);
	if(rc == QE_EOF){
		free(input_data);
		return QE_EOF;
	}

	vector<Attribute> attributes;
	input->getAttributes(attributes);
	int inputnullsIndicatorsize = ceil((double)attributes.size() / CHAR_BIT);

	int outputnullsIndicatorsize = ceil((double)attrNames.size() / CHAR_BIT);
	char* outputnullbytearray= (char*)malloc(outputnullsIndicatorsize);
	memset(outputnullbytearray,0,outputnullsIndicatorsize);

	int offset = outputnullsIndicatorsize;
	for (int loop = 0; (unsigned)loop < attrNames.size(); loop++) {


		int offset2 = inputnullsIndicatorsize;
		for (int loop2 = 0; (unsigned)loop2 < attributes.size(); loop2++) {

			bool nullbit = ((char*)input_data)[loop2 / 8] & ( 1 << ( 7 - ( loop2 % 8 )));
			if(attributes[loop2].name.compare(attrNames[loop]) == 0)
			{
				if(nullbit){
					outputnullbytearray[loop2 / 8] = outputnullbytearray[loop2 / 8] | (1 << (7 - (loop2 % 8)));

				}else{
					if(attributes[loop2].type == TypeInt){
						memcpy((char *)data + offset, (char *)input_data + offset2, sizeof(int));
						offset += sizeof(int);
						offset2 += sizeof(int);
					}
					else if  (attributes[loop2].type == TypeReal){
						memcpy((char *)data + offset, (char *)input_data + offset2, sizeof(float));
						offset += sizeof(float);
						offset2 += sizeof(float);

					}
					else{
						int length;
						memcpy(&length, (char *)input_data + offset2, sizeof(int));
						memcpy((char *)data + offset, (char *)input_data + offset2, sizeof(int) + length);
						offset += sizeof(int) + length;
						offset2 += sizeof(int) + length;
					}
				}
				break;
			}else{

				if(!nullbit){
					if(attributes[loop2].type == TypeInt ){
						offset2 += sizeof(int);
					}
					else if	(attributes[loop2].type == TypeReal){
						offset2 += sizeof(float);
					}else{
						int length;
						memcpy(&length, (char *)input_data + offset2, sizeof(int));
						offset2 += sizeof(int) + length;
					}
				}
			}
		}
	}
	memcpy((char *)data, outputnullbytearray, outputnullsIndicatorsize);
	free(input_data);
	return 0;

}

void Project::getAttributes(vector<Attribute> &attrs) const{

	vector<Attribute> input_attributes;
	input->getAttributes(input_attributes);
	for (int loop = 0; (unsigned)loop < attrNames.size(); loop++) {

		for (int loop2 = 0; (unsigned)loop2 < input_attributes.size(); loop2++) {

			if(input_attributes[loop2].name.compare(attrNames[loop]) == 0){
				attrs.push_back(input_attributes[loop2]);
				break;
			}
		}
	}

}
Aggregate::Aggregate(Iterator *input,          // Iterator of input R
		Attribute aggAttr,        // The attribute over which we are computing an aggregate
		AggregateOp op            // Aggregate operation
){
	this->input = input;
	this->aggAttr = aggAttr;
	this->aggregateOP = op;
	input->getAttributes(input_attributes);
}

void Aggregate::populateData(void* data, float value ){
	char* nullsIndicatorByte = (char*)malloc(1);
	memset(nullsIndicatorByte, 0, 1);
	memcpy((char *)data + 1, &value, sizeof(float));

	memcpy(data, nullsIndicatorByte, 1);
}

RC Aggregate::getNextTuple(void *data){

	if(counter == 0)
	{
		int nullsIndicator = ceil((float)input_attributes.size() / CHAR_BIT);
		if(aggAttr.type == TypeVarChar)
			return QE_EOF;
		char *input_data = (char *)malloc(PAGE_SIZE);

		while(input->getNextTuple(input_data) != QE_EOF){

			int offset = nullsIndicator;
			for (int loop = 0; (unsigned)loop < input_attributes.size(); loop++) {

				bool nullbit = ((char*)input_data)[loop / 8] & ( 1 << ( 7 - ( loop % 8 )));

				if(input_attributes[loop].name.compare(aggAttr.name) == 0){
					if(nullbit){
						break;
					}
					if(input_attributes[loop].type == TypeInt){
						int input_data_value;
						memcpy(&input_data_value, input_data + offset, sizeof(int));
						int_vector.push_back(input_data_value);
						break;
					}else if(input_attributes[loop].type == TypeReal){
						float input_data_value;
						memcpy(&input_data_value, input_data + offset, sizeof(float));
						float_vector.push_back(input_data_value);
						break;
					}
				}else{
					if(!nullbit){

						if(input_attributes[loop].type == TypeInt )
							offset += sizeof(int);
						else if(input_attributes[loop].type == TypeReal)
							offset += sizeof(float);

						else{
							int length;
							memcpy(&length, input_data + offset, sizeof(int));
							offset += (sizeof(int) + length);
						}
					}
				}
			}
		}
		counter++;
		free(input_data);
		if(int_vector.size() == 0 && float_vector.size() == 0){
			return QE_EOF;
		}
		//fillAggregateData(int_vector,float_vector,data);
		if(aggregateOP == MIN){
			if(aggAttr.type == TypeInt){
				float minimum = (float)int_vector[0];
				for (int loop = 1; (unsigned)loop < int_vector.size(); loop++) {
					if(int_vector[loop] < minimum){
						minimum = (float)int_vector[loop];
					}
				}

				populateData(data,minimum);
				return 0;
			}else if(aggAttr.type == TypeReal){
				float minimum = float_vector[0];
				for (int loop = 1; (unsigned)loop < float_vector.size(); loop++) {
					if(float_vector[loop] < minimum){
						minimum = float_vector[loop];
					}
				}
				populateData(data,minimum);
				return 0;
			}
		}else if(aggregateOP == MAX){
			if(aggAttr.type == TypeInt){
				float maximum = (float)int_vector[0];
				for (int loop = 1; (unsigned)loop < int_vector.size(); loop++) {
					if(int_vector[loop] > maximum){
						maximum = (float)int_vector[loop];
					}
				}
				populateData(data,maximum);
				return 0;
			}else if(aggAttr.type == TypeReal){
				float maximum = float_vector[0];
				for (int loop = 1; (unsigned)loop < float_vector.size(); loop++) {
					if(float_vector[loop] > maximum){
						maximum = float_vector[loop];
					}
				}
				populateData(data,maximum);
				return 0;
			}
		}else if(aggregateOP == COUNT){
			if(aggAttr.type == TypeInt){
				float count = (float)int_vector.size();
				populateData(data,count);
				return 0;
			}else if(aggAttr.type == TypeReal){
				float count = float_vector.size();
				populateData(data,count);
				return 0;

			}

		}else if(aggregateOP == SUM){
			if(aggAttr.type == TypeInt){
				float sum = 0;
				for (int loop = 0; (unsigned)loop < int_vector.size(); loop++) {
					sum += (float)int_vector[loop];
				}
				populateData(data,sum);
				return 0;
			}else if(aggAttr.type == TypeReal){
				float sum = 0;
				for (int loop = 0; (unsigned)loop < float_vector.size(); loop++) {
					sum += float_vector[loop];
				}
				populateData(data,sum);
				return 0;
			}
		}else if (aggregateOP == AVG){
			if(aggAttr.type == TypeInt){
				float sum = 0;
				for (int loop = 0; (unsigned)loop < int_vector.size(); loop++) {
					sum += (float)int_vector[loop];
				}
				float average = (float)sum / int_vector.size();
				populateData(data,average);
				return 0;
			}else if(aggAttr.type == TypeReal){
				float sum = 0;
				for (int loop = 0; (unsigned)loop < float_vector.size(); loop++) {
					sum += float_vector[loop];
				}
				float average = (float)sum / float_vector.size();
				populateData(data,average);
				return 0;
			}
		}
		return 0;
	}
	else
		return QE_EOF;

}
void Aggregate::getAttributes(vector<Attribute> &attrs) const{
	string temp;
	Attribute attribute = aggAttr;
	attribute.type = TypeReal;
	attribute.length = 4;
	switch (aggregateOP)
	{
	case MIN:
		temp = "MIN";
		break;
	case MAX:
		temp = "MAX";
		break;
	case SUM:
		temp = "SUM";
		break;
	case AVG:
		temp = "AVG";
		break;
	case COUNT:
		temp = "COUNT";
		break;
	}

	attribute.name = temp + "(" + attribute.name + ")";
	attrs.push_back(attribute);
}
