#include "qe.h"
#include <stdlib.h>
using namespace std;

Filter::Filter(Iterator *input, const Condition &condition) {
	unsigned i;
	int offset, length, flag, varcharcmp;
	float lhsAttr, rhsValue;
	input->getAttributes(attrs);
	count = 0;
	iterator = 0;
	void *data = malloc(1000);
	while (input->getNextTuple(data) != QE_EOF) {
		offset = 0;
		flag = 1;
		for (i = 0; i < attrs.size() && flag != 0; i++) {
			if (attrs[i].name == condition.lhsAttr) {
				if (attrs[i].type == TypeVarChar) {
					memcpy(&length, (char*) data + offset, 4);
					varcharcmp = memcmp((char*) condition.rhsValue.data,
							(char*) data + offset, length + 4);
					switch (condition.op) {
					case EQ_OP:
						flag = (varcharcmp == 0);
						break;
					case LT_OP:
						flag = (varcharcmp < 0);
						break;
					case GT_OP:
						flag = (varcharcmp > 0);
						break;
					case LE_OP:
						flag = (varcharcmp <= 0);
						break;
					case GE_OP:
						flag = (varcharcmp >= 0);
						break;
					case NE_OP:
						flag = (varcharcmp != 0);
						break;
					case NO_OP:
						flag = 1;
						break;
					}
					offset += sizeof(int) + length;
				} else {
					memcpy(&rhsValue, (char*) (condition.rhsValue.data), 4);
					memcpy(&lhsAttr, (char*) data + offset, 4);
					switch (condition.op) {
					case EQ_OP:
						flag = (lhsAttr == rhsValue);
						break;
					case LT_OP:
						flag = (lhsAttr < rhsValue);
						break;
					case GT_OP:
						flag = (lhsAttr > rhsValue);
						break;
					case LE_OP:
						flag = (lhsAttr <= rhsValue);
						break;
					case GE_OP:
						flag = (lhsAttr >= rhsValue);
						break;
					case NE_OP:
						flag = (lhsAttr != rhsValue);
						break;
					case NO_OP:
						flag = 1;
						break;
					}
					offset += sizeof(int);
				}
			} else {
				if (attrs[i].type == TypeVarChar) {
					memcpy(&length, (char*) data + offset, 4);
					offset += length + 4;
				} else {
					offset += 4;
				}
			}
		}
		if (flag == 1) {
			void *temp = malloc(offset);
			memcpy((char*) temp, (const char*) data, offset);
			dataselect.push_back(temp);
			len.push_back(offset);
			count++;
		}
	}
	free(data);
}

Filter::~Filter() {
	for (std::vector<void *>::iterator iter = dataselect.begin();
			iter != dataselect.end(); ++iter) {
		free((void *) (*iter));
	}
}

RC Filter::getNextTuple(void *data) {
	if (iterator == count)
		return QE_EOF;
	memcpy(data, this->dataselect[iterator], len[iterator]);
	iterator++;
	return 0;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;
}

Project::Project(Iterator *input, const vector<string> &attrNames) {
	vector<Attribute> attrs;
	unsigned i, j;
	int offset1, flag, offset2, length;
	void *data, *temp;
	input->getAttributes(attrs);
	this->iterator = 0;
	this->count = 0;
	for (i = 0; i < attrs.size(); i++) {
		for (j = 0; j < attrNames.size(); j++) {
			if (attrs[i].name == attrNames[j])
				this->attrs.push_back(attrs[i]);
		}
	}
	data = malloc(100);
	while (input->getNextTuple(data) != QE_EOF) {
		offset1 = 0;
		offset2 = 0;
		temp = malloc(100);
		for (i = 0; i < attrs.size(); i++) {
			flag = 0;
			for (j = 0; j < attrNames.size() && flag == 0; j++) {
				if (attrs[i].name == attrNames[j])
					flag = 1;
			}
			if (flag == 1) {
				if (attrs[i].type == TypeVarChar) {
					memcpy(&length, (char*) data + offset1, 4);
					memcpy((char*) temp + offset2, &length, 4);
					offset1 += 4;
					offset2 += 4;
					memcpy((char*) temp + offset2, (char*) data + offset1,
							length);
					offset1 += length;
					offset2 += length;
				} else {
					memcpy((char*) temp + offset2, (char*) data + offset1, 4);
					offset1 += 4;
					offset2 += 4;
				}
			} else {
				if (attrs[i].type == TypeVarChar) {
					memcpy(&length, (char*) data + offset1, 4);
					offset1 += length + 4;
				} else {
					offset1 += 4;
				}
			}
		}
		dataproject.push_back(temp);
		len.push_back(offset2);
		count++;
	}
	free(data);
}

Project::~Project() {
	for (std::vector<void *>::iterator iter = dataproject.begin();
			iter != dataproject.end(); ++iter) {
		free((void *) (*iter));
	}
}

RC Project::getNextTuple(void *data) {
	if (iterator == count)
		return QE_EOF;
	memcpy((char*) data, (char*) this->dataproject[iterator],
			this->len[iterator]);
	iterator++;
	return 0;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;
}

NLJoin::NLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition,
		const unsigned numPages) {
	vector<Attribute> attrs1;
	unsigned size1, size2, i;
	int length, offset1, jlength1, offset2, jlength2, n, tmp, res;
	AttrType type1;
	float x, y;
	leftIn->getAttributes(attrs);
	size1 = attrs.size();
	rightIn->getAttributes(attrs1);
	size2 = attrs1.size();
	attrs.insert(attrs.end(), attrs1.begin(), attrs1.end());
	iterator = 0;
	count = 0;
	void *data1 = malloc(500);
	void *data2 = malloc(500);
	void *temp1 = malloc(500);
	void *temp2 = malloc(500);
	while (leftIn->getNextTuple(data1) != QE_EOF) {
		offset1 = 0;
		for (i = 0; i < size1; i++) {
			if (attrs[i].name == condition.lhsAttr) {
				type1 = attrs[i].type;
				if (attrs[i].type == TypeVarChar) {
					memcpy(&length, (char*) data1 + offset1, sizeof(int));
					memcpy((char*) temp1, (char*) data1 + offset1,
							sizeof(int) + length);
					jlength1 = length;
				} else {
					memcpy((char*) temp1, (char*) data1 + offset1, sizeof(int));
				}
			}
			if (attrs[i].type == TypeVarChar) {
				memcpy(&length, (char*) data1 + offset1, sizeof(int));
				offset1 += sizeof(int) + length;
			} else {
				offset1 += sizeof(int);
			}
		}
		rightIn->setIterator();
		while (rightIn->getNextTuple(data2) != QE_EOF) {
			offset2 = 0;
			for (i = 0; i < size2; i++) {
				if (attrs1[i].name == condition.rhsAttr) {
					if (attrs1[i].type == TypeVarChar) {
						memcpy(&length, (char*) data2 + offset2, sizeof(int));
						memcpy((char*) temp2, (char*) data2 + offset2,
								sizeof(int) + length);
						jlength2 = length;
					} else {
						memcpy((char*) temp2, (char*) data2 + offset2,
								sizeof(int));
					}
				}
				if (attrs1[i].type == TypeVarChar) {
					memcpy(&length, (char*) data2 + offset2, sizeof(int));
					offset2 += sizeof(int) + length;
				} else {
					offset2 += sizeof(int);
				}
			}
			if (type1 == TypeVarChar) {
				char *s1 = (char*) malloc(jlength1 + 1);
				char *s2 = (char*) malloc(jlength2 + 1);
				memcpy((char*) s1, (char*) temp1 + sizeof(int), jlength1);
				memcpy((char*) s2, (char*) temp2 + sizeof(int), jlength2);
				s1[jlength1] = '\0';
				s2[jlength2] = '\0';
				n = strcmp(s1, s2);
				switch (condition.op) {
				case EQ_OP:
					res = (n == 0);
					break;
				case LT_OP:
					res = (n < 0);
					break;
				case GT_OP:
					res = (n > 0);
					break;
				case LE_OP:
					res = (n <= 0);
					break;
				case GE_OP:
					res = (n >= 0);
					break;
				case NE_OP:
					res = (n != 0);
					break;
				case NO_OP:
					res = 1;
					break;
				}
				free(s1);
				free(s2);
			} else {
				if (type1 == TypeInt) {
					memcpy(&tmp, (char*) temp1, sizeof(int));
					x = (float) tmp;
					memcpy(&tmp, (char*) temp2, sizeof(int));
					y = (float) tmp;
				} else {
					memcpy(&x, (char*) temp1, sizeof(float));
					memcpy(&y, (char*) temp2, sizeof(float));
				}
				switch (condition.op) {
				case EQ_OP:
					res = (x == y);
					break;
				case LT_OP:
					res = (x < y);
					break;
				case GT_OP:
					res = (x > y);
					break;
				case LE_OP:
					res = (x <= y);
					break;
				case GE_OP:
					res = (x >= y);
					break;
				case NE_OP:
					res = (x != y);
					break;
				case NO_OP:
					res = 1;
					break;
				}
			}
			if (res == 1) {
				void *temp = malloc(offset1 + offset2);
				memcpy((char*) temp, (char*) data1, offset1);
				memcpy((char*) temp + offset1, (char*) data2, offset2);
				datanljoin.push_back(temp);
				len.push_back(offset1 + offset2);
				count++;
			}
		}
	}
	free(data1);
	free(data2);
	free(temp1);
	free(temp2);
}

NLJoin::~NLJoin() {
	for (std::vector<void *>::iterator iter = datanljoin.begin();
			iter != datanljoin.end(); ++iter) {
		free((void *) (*iter));
	}
}

RC NLJoin::getNextTuple(void *data) {
	if (iterator == count)
		return QE_EOF;
	memcpy((char*) data, this->datanljoin[iterator], this->len[iterator]);
	iterator++;
	return 0;
}

void NLJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;
}

INLJoin::~INLJoin() {
	for (std::vector<void *>::iterator iter = datainljoin.begin();
			iter != datainljoin.end(); ++iter) {
		free((void *) (*iter));
	}
}

RC INLJoin::getNextTuple(void *data) {
	if (iterator == count)
		return QE_EOF;
	memcpy((char*) data, this->datainljoin[iterator], this->len[iterator]);
	iterator++;
	return 0;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;
}

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn,
		const Condition &condition, const unsigned numPages) {
	vector<Attribute> attrs1;
	unsigned size1, size2, i;
	int length, offset1, jlength1, jlength2, offset2;
	leftIn->getAttributes(attrs);
	size1 = attrs.size();
	rightIn->getAttributes(attrs1);
	size2 = attrs1.size();
	attrs.insert(attrs.end(), attrs1.begin(), attrs1.end());
	iterator = 0;
	count = 0;
	void *data1 = malloc(500);
	void *data2 = malloc(500);
	void *temp1 = malloc(500);
	void *temp2 = malloc(500);
	while (leftIn->getNextTuple(data1) != QE_EOF) {
		offset1 = 0;
		for (i = 0; i < size1; i++) {
			if (attrs[i].name == condition.lhsAttr) {
				if (attrs[i].type == TypeVarChar) {
					memcpy(&length, (char*) data1 + offset1, sizeof(int));
					memcpy((char*) temp1, (char*) data1 + offset1,
							sizeof(int) + length);
					jlength1 = length;
				} else {
					memcpy((char*) temp1, (char*) data1 + offset1, sizeof(int));
				}
			}
			if (attrs[i].type == TypeVarChar) {
				memcpy(&length, (char*) data1 + offset1, sizeof(int));
				offset1 += sizeof(int) + length;
			} else {
				offset1 += sizeof(int);
			}
		}
		switch (condition.op) {
		case EQ_OP:
			rightIn->setIterator(temp1, temp1, true, true);
			break;
		case NE_OP:
			rightIn->setIterator(NULL, NULL, true, true);
			break;
		case NO_OP:
			rightIn->setIterator(NULL, NULL, true, true);
			break;
		case LT_OP:
			rightIn->setIterator(NULL, temp1, false, false);
			break;
		case GT_OP:
			rightIn->setIterator(temp1, NULL, false, false);
			break;
		case LE_OP:
			rightIn->setIterator(NULL, temp1, false, true);
			break;
		case GE_OP:
			rightIn->setIterator(temp1, NULL, true, false);
			break;
		}
		while (rightIn->getNextTuple(data2) != QE_EOF) {
			if (condition.op != NE_OP) {
				offset2 = 0;
				for (i = 0; i < size2; i++) {
					if (attrs[i].type == TypeVarChar) {
						memcpy(&length, (char*) data2 + offset2, sizeof(int));
						offset2 += sizeof(int) + length;
					} else {
						offset2 += sizeof(int);
					}
				}
				void *temp = malloc(offset1 + offset2);
				memcpy((char*) temp, (char*) data1, offset1);
				memcpy((char*) temp + offset1, (char*) data2, offset2);
				datainljoin.push_back(temp);
				len.push_back(offset1 + offset2);
				count++;
			} else {
				AttrType type1;
				offset2 = 0;
				int n, res;
				float x, y;
				for (i = 0; i < size2; i++) {
					if (attrs[i].name == condition.rhsAttr) {
						type1 = attrs[i].type;
						if (attrs[i].type == TypeVarChar) {
							memcpy(&length, (char*) data2 + offset2,
									sizeof(int));
							memcpy((char*) temp2, (char*) data2 + offset2,
									sizeof(int) + length);
							jlength2 = length;
						} else {
							memcpy((char*) temp2, (char*) data2 + offset2,
									sizeof(int));
						}
					}
					if (attrs[i].type == TypeVarChar) {
						memcpy(&length, (char*) data2 + offset2, sizeof(int));
						offset2 += sizeof(int) + length;
					} else {
						offset2 += sizeof(int);
					}
				}
				if (type1 == TypeVarChar) {
					char *s1 = (char*) malloc(jlength1 + 1);
					char *s2 = (char*) malloc(jlength2 + 1);
					memcpy((char*) s1, (char*) temp1 + sizeof(int), jlength1);
					memcpy((char*) s2, (char*) temp2 + sizeof(int), jlength2);
					s1[jlength1] = '\0';
					s2[jlength2] = '\0';
					n = strcmp(s1, s2);
					if (n != 0) {
						res = 1;
					}
					free(s1);
					free(s2);
				} else {
					if (type1 == TypeInt) {
						memcpy(&x, (char*) temp1, sizeof(int));
						x = (float) x;
						memcpy(&y, (char*) temp2, sizeof(int));
						y = (float) y;
					} else {
						memcpy(&x, (char*) temp1, sizeof(float));
						memcpy(&y, (char*) temp2, sizeof(float));
					}
					if (x != y) {
						res = 1;
					}
				}
				if (res == 1) {
					void *temp = malloc(offset1 + offset2);
					memcpy((char*) temp, (char*) data1, offset1);
					memcpy((char*) temp + offset1, (char*) data2, offset2);
					datainljoin.push_back(temp);
					len.push_back(offset1 + offset2);
					count++;
				}
			}
		}
	}
	free(data1);
	free(data2);
	free(temp1);
}
