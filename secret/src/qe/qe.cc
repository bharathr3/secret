#include "qe.h"
#include <stdlib.h>
#include <float.h>
#include <sys/resource.h>
using namespace std;

Filter::Filter(Iterator *input, const Condition &condition) {
	int offset, length, flag, cmp;
	float lhsAttr, rhsValue;
	input->getAttributes(attrs);
	count = 0;
	iterator = 0;
	void *data = malloc(1000);
	while (input->getNextTuple(data) != QE_EOF) {
		offset = 0;
		flag = 1;
		for (unsigned i = 0; i < attrs.size() && flag != 0; i++) {
			if (attrs[i].name == condition.lhsAttr) {
				if (attrs[i].type == TypeVarChar) {
					memcpy(&length, (char*) data + offset, 4);
					cmp = memcmp((char*) condition.rhsValue.data,
							(char*) data + offset, length + 4);
					switch (condition.op) {
					case EQ_OP:
						flag = (cmp == 0);
						break;
					case LT_OP:
						flag = (cmp < 0);
						break;
					case GT_OP:
						flag = (cmp > 0);
						break;
					case LE_OP:
						flag = (cmp <= 0);
						break;
					case GE_OP:
						flag = (cmp >= 0);
						break;
					case NE_OP:
						flag = (cmp != 0);
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
			filter_data.push_back(temp);
			filter_length.push_back(offset);
			count++;
		}
	}
	free(data);
}

Filter::~Filter() {
	for (std::vector<void *>::iterator iter = filter_data.begin();
			iter != filter_data.end(); ++iter) {
		free((void *) (*iter));
	}
}

RC Filter::getNextTuple(void *data) {
	if (iterator == count)
		return QE_EOF;
	memcpy(data, this->filter_data[iterator], filter_length[iterator]);
	iterator++;
	return 0;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;
}

Project::Project(Iterator *input, const vector<string> &attrNames) {
	vector<Attribute> attrs;
	int offset1, offset2, flag, length;
	void *data, *temp;
	input->getAttributes(attrs);
	this->iterator = 0;
	this->count = 0;
	for (unsigned i = 0; i < attrs.size(); i++) {
		for (unsigned j = 0; j < attrNames.size(); j++) {
			if (attrs[i].name == attrNames[j])
				this->attrs.push_back(attrs[i]);
		}
	}
	data = malloc(100);
	while (input->getNextTuple(data) != QE_EOF) {
		offset1 = 0;
		offset2 = 0;
		temp = malloc(100);
		for (unsigned i = 0; i < attrs.size(); i++) {
			flag = 0;
			for (unsigned j = 0; j < attrNames.size() && flag == 0; j++) {
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
		projection_data.push_back(temp);
		projection_length.push_back(offset2);
		count++;
	}
	free(data);
}

Project::~Project() {
	for (std::vector<void *>::iterator iter = projection_data.begin();
			iter != projection_data.end(); ++iter) {
		free((void *) (*iter));
	}
}

RC Project::getNextTuple(void *data) {
	if (iterator == count)
		return QE_EOF;
	memcpy((char*) data, (char*) this->projection_data[iterator],
			this->projection_length[iterator]);
	iterator++;
	return 0;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;
}

NLJoin::NLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition,
		const unsigned numPages) {
	iterator = 0;
	count = 0;
	attrs.clear();
	void *temp1 = malloc(1000);
	void *temp2 = malloc(1000);
	AttrType type;
	int value, length1,length2;
	vector<void *> t_nljoin_data;
	vector<int> t_nljoin_lengths;
	vector<Attribute> r_attr;
	vector<Attribute> l_attr;
	leftIn->getAttributes(l_attr);
	rightIn->getAttributes(r_attr);
	attrs = l_attr;
	attrs.insert(attrs.end(), r_attr.begin(), r_attr.end());
	int flag,offset1,offset2 = 0;
	void *data1 = malloc(1000);
	void *data2 = malloc(1000);
	void *data = malloc(1000);
	while (leftIn->getNextTuple(data) != QE_EOF) {
		offset1 = 0;
		flag = 0;
		for (int i = 0; i < (int) l_attr.size(); i++) {
			if (l_attr[i].name == condition.lhsAttr) {
				type = attrs[i].type;
				if (attrs[i].type == 2) {
					int len;
					memcpy(&len, (char*) data + offset1, sizeof(int));
					memcpy((char *) temp1, (char *) data + offset1, len + 4);
					offset1 += sizeof(int) + len;
					length1 = len;
				} else {
					memcpy((char *) temp1, (char *) data + offset1, 4);
					offset1 += sizeof(int);
				}
			} else {
				if (attrs[i].type == 2) {
					int len;
					memcpy(&len, (char*) data + offset1, sizeof(int));
					offset1 += sizeof(int) + len;
				} else {
					offset1 += sizeof(int);
				}
			}
		}
		rightIn->setIterator();
		while (rightIn->getNextTuple(data1) != QE_EOF) {
			offset2 = 0;
			for (int i = 0; i < (int) r_attr.size(); i++) {
				if (r_attr[i].name == condition.rhsAttr) {
					if (r_attr[i].type == 2) {
						int len;
						memcpy(&len, (char*) data1 + offset2, sizeof(int));
						memcpy((char *) temp2, (char *) data1 + offset2,
								len + 4);
						offset2 = offset2 + sizeof(int) + len;
						length2 = len;
					} else {
						memcpy((char *) temp2, (char *) data1 + offset2, 4);
						offset2 = offset2 + 4;
					}
				} else {
					if (r_attr[i].type == 2) {
						int len;
						memcpy(&len, (char*) data1 + offset2, sizeof(int));
						offset2 += sizeof(int) + len;
					} else {
						offset2 += sizeof(int);
					}
				}
			}
			flag = 0;
			if (type == 2) {
				char *value1 = (char*) malloc(length1 + 1);
				char *value2 = (char*) malloc(length2 + 1);
				memcpy((char*) value1, (char*) temp1 + sizeof(int), length1);
				memcpy((char*) value2, (char*) temp2 + sizeof(int), length2);
				value1[length1] = '\0';
				value2[length2] = '\0';
				value = strcmp(value1, value2);
				switch (condition.op) {
				case 0:
					if (value == 0)
						flag = 1;
					break;
				case 1:
					if (value < 0)
						flag = 1;
					break;
				case 2:
					if (value > 0)
						flag = 1;
					break;
				case 3:
					if (value <= 0)
						flag = 1;
					break;
				case 4:
					if (value >= 0)
						flag = 1;
					break;
				case 5:
					if (value != 0)
						flag = 1;
					break;
				case 6:
					flag = 1;
					break;
				}
				free(value1);
				free(value2);
			} else if (type == 0) {
				int v1, v2;
				memcpy(&v1, (char*) temp1, sizeof(int));
				memcpy(&v2, (char*) temp2, sizeof(int));
				switch (condition.op) {
				case 0:
					if (v1 == v2)
						flag = 1;
					break;
				case 1:
					if (v1 < v2)
						flag = 1;
					break;
				case 2:
					if (v1 > v2)
						flag = 1;
					break;
				case 3:
					if (v1 <= v2)
						flag = 1;
					break;
				case 4:
					if (v1 >= v2)
						flag = 1;
					break;
				case 5:
					if (v1 != v2)
						flag = 1;
					break;
				case 6:
					flag = 1;
					break;
				}

			} else {
				float v1, v2;
				memcpy(&v1, (char*) temp1, sizeof(float));
				memcpy(&v2, (char*) temp2, sizeof(float));
				switch (condition.op) {
				case 0:
					if (v1 == v2)
						flag = 1;
					break;
				case 1:
					if (v1 < v2)
						flag = 1;
					break;
				case 2:
					if (v1 > v2)
						flag = 1;
					break;
				case 3:
					if (v1 <= v2)
						flag = 1;
					break;
				case 4:
					if (v1 >= v2)
						flag = 1;
					break;
				case 5:
					if (v1 != v2)
						flag = 1;
					break;
				case 6:
					flag = 1;
					break;
				}
			}
			if (flag == 1) {
				void *temp = malloc(offset1 + offset2);
				memcpy((char*) temp, (char*) data, offset1);
				memcpy((char*) temp + offset1, (char*) data1, offset2);
				t_nljoin_lengths.push_back(offset1 + offset2);
				t_nljoin_data.push_back(temp);
				count++;
			}
		}
	}
	nljoin_data = t_nljoin_data;
	nljoin_length= t_nljoin_lengths;
	free(data);
	free(data1);
	free(data2);
	free(temp1);
	free(temp2);
}

NLJoin::~NLJoin() {
	for (std::vector<void *>::iterator iter = nljoin_data.begin();
			iter != nljoin_data.end(); ++iter) {
		free((void *) (*iter));
	}
}

RC NLJoin::getNextTuple(void *data) {
	if (iterator == count)
		return QE_EOF;
	memcpy((char*) data, this->nljoin_data[iterator], this->nljoin_length[iterator]);
	iterator++;
	return 0;
}

void NLJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;
}

INLJoin::~INLJoin() {
	for (std::vector<void *>::iterator iter = inljoin_data.begin();
			iter != inljoin_data.end(); ++iter) {
		free((void *) (*iter));
	}
}

RC INLJoin::getNextTuple(void *data) {
	if (iterator == count)
		return QE_EOF;
	memcpy((char*) data, this->inljoin_data[iterator], this->inljoin_length[iterator]);
	iterator++;
	return 0;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;
}

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn,
		const Condition &condition, const unsigned numPages) {
	vector<Attribute> r_attrs;
	unsigned size1, size2, i;
	int length, offset1, offset2, jlength1, jlength2;
	leftIn->getAttributes(attrs);
	size1 = attrs.size();
	rightIn->getAttributes(r_attrs);
	size2 = r_attrs.size();
	attrs.insert(attrs.end(), r_attrs.begin(), r_attrs.end());
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
				inljoin_data.push_back(temp);
				inljoin_length.push_back(offset1 + offset2);
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
					inljoin_data.push_back(temp);
					inljoin_length.push_back(offset1 + offset2);
					count++;
					free(temp);
				}
			}
		}
	}
	free(data1);
	free(data2);
	free(temp1);
	free(temp2);
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op) {
	unsigned i;
	vector<Attribute> attrs;
	input->getAttributes(attrs);
	void *data = malloc(500);
	float min = FLT_MAX, max = 0.0, sum = 0.0, avg = 0.0, ftmp, fcount = 0;
	int itmp, tmp;
	iterator = 0;
	count = 0;
	int offset;
	while (input->getNextTuple(data) != QE_EOF) {
		offset = 0;
		for (i = 0; i < attrs.size(); i++) {
			if (attrs[i].name == aggAttr.name) {
				if (aggAttr.type == TypeInt) {
					memcpy(&itmp, (char*) data + offset, sizeof(int));
					ftmp = (float) (itmp);
				} else if (aggAttr.type == TypeReal) {
					memcpy(&ftmp, (char*) data + offset, sizeof(int));
				}
				min = (min < ftmp) ? min : ftmp;
				max = (max > ftmp) ? max : ftmp;
				sum += ftmp;
				fcount++;
				offset += sizeof(int);
			} else {
				if (attrs[i].type == TypeVarChar) {
					memcpy(&tmp, (char*) data + offset, sizeof(int));
					offset += sizeof(int) + tmp;
				} else
					offset += sizeof(int);
			}
		}
	}
	avg = sum / fcount;
	void *temp = malloc(4);
	switch (op) {
	case MIN:
		memcpy((char*) temp, &min, sizeof(float));
		break;
	case MAX:
		memcpy((char*) temp, &max, sizeof(float));
		break;
	case SUM:
		memcpy((char*) temp, &sum, sizeof(float));
		break;
	case AVG:
		memcpy((char*) temp, &avg, sizeof(float));
		break;
	case COUNT:
		memcpy((char*) temp, &fcount, sizeof(float));
		break;
	}
	aggregate_data.push_back(temp);
	aggregate_length.push_back(4);
	count++;
	string name;
	Attribute temp_attr;
	switch (op) {
	case MIN:
		name = "MIN";
		break;
	case MAX:
		name = "MAX";
		break;
	case SUM:
		name = "SUM";
		break;
	case AVG:
		name = "AVG";
		break;
	case COUNT:
		name = "COUNT";
		break;
	}
	name += "(" + aggAttr.name + ")";
	temp_attr.name = name;
	switch (op) {
	case MIN:
		break;
	case MAX:
		break;
	case SUM:
		temp_attr.type = aggAttr.type;
		break;
	case COUNT:
		temp_attr.type = TypeInt;
		break;
	case AVG:
		temp_attr.type = TypeReal;
		break;
	}
	temp_attr.length = 4;
	this->attrs.push_back(temp_attr);
	free(data);
}

Aggregate::~Aggregate() {
	for (std::vector<void *>::iterator iter = aggregate_data.begin();
			iter != aggregate_data.end(); ++iter) {
		free((void *) (*iter));
	}
}

RC Aggregate::getNextTuple(void *data) {
	if (iterator == count)
		return QE_EOF;
	memcpy((char*) data, (char*) this->aggregate_data[iterator], this->aggregate_length[iterator]);
	iterator++;
	return 0;
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;
}

float min(float a, float b) {
	if (a < b)
		return a;
	return b;
}

float max(float a, float b) {
	if (a > b)
		return a;
	return b;
}

struct running_info {
	float min, max, count, sum, avg;
	running_info() {
		min = max = count = sum = avg = 0.0;
	}
};

template<class T>
void process(vector<T> &gVal, vector<running_info> &gInfo, T gvalue,
		float value) {
	unsigned i;
	int flag = 0;
	for (i = 0; i < gVal.size() && flag == 0; i++) {
		if (gVal[i] == gvalue)
			flag = 1;
	}
	if (flag == 0) {
		gVal.push_back(gvalue);
		running_info r;
		r.min = value;
		r.max = value;
		gInfo.push_back(r);
	} else {
		i--;
	}
	gInfo[i].min = min(gInfo[i].min, value);
	gInfo[i].max = min(gInfo[i].max, value);
	gInfo[i].sum += value;
	gInfo[i].count++;
	gInfo[i].avg = gInfo[i].sum / gInfo[i].count;
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, Attribute gAttr,
		AggregateOp op) {
	int offset, length;
	vector<string> gVal1;
	vector<float> gVal2;
	vector<running_info> gInfo;
	float value;
	string gvalue1;
	float gvalue2;
	vector<Attribute> attrs;
	iterator = 0;
	count = 0;
	input->getAttributes(attrs);
	void *data = malloc(500);
	while (input->getNextTuple(data) != QE_EOF) {
		offset = 0;
		for (unsigned i = 0; i < attrs.size(); i++) {
			if (attrs[i].name == aggAttr.name) {
				if (attrs[i].type == TypeReal) {
					memcpy(&value, (char*) data + offset, sizeof(float));
				} else {
					int x;
					memcpy(&x, (char*) data + offset, sizeof(float));
					value = (int) x;
				}
			}
			if (attrs[i].name == gAttr.name) {
				if (attrs[i].type == TypeVarChar) {
					memcpy(&length, (char*) data + offset, sizeof(int));
					char *str = (char*) malloc(length + 1);
					memcpy((char*) str, (char*) data + offset + sizeof(int),
							length);
					str[length] = '\0';
					gvalue1 = str;
					free(str);
				} else {
					if (attrs[i].type == TypeReal) {
						memcpy(&gvalue2, (char*) data + offset, sizeof(int));
					} else {
						int x;
						memcpy(&x, (char*) data + offset, sizeof(int));
						gvalue2 = (float) x;
					}
				}
			}
			if (attrs[i].type == TypeVarChar) {
				memcpy(&length, (char*) data + offset, sizeof(int));
				offset += sizeof(int) + length;
			} else {
				offset += sizeof(int);
			}
		}
		if (gAttr.type == TypeVarChar)
			process(gVal1, gInfo, gvalue1, value);
		else
			process(gVal2, gInfo, gvalue2, value);

	}
	for (unsigned i = 0; i < gInfo.size(); i++) {
		void *temp = malloc(200);
		offset = 0;
		if (gAttr.type == TypeVarChar) {
			length = gVal1[i].length();
			memcpy((char*) temp + offset, &length, sizeof(int));
			offset += sizeof(int);
			memcpy((char*) temp + offset, gVal1[i].c_str(), length);
			offset += length;
		} else {
			memcpy((char*) temp + offset, &gVal2[i], sizeof(float));
			offset += sizeof(float);
		}
		switch (op) {
		case MIN:
			memcpy((char*) temp + offset, &gInfo[i].min, sizeof(float));
			break;
		case MAX:
			memcpy((char*) temp + offset, &gInfo[i].max, sizeof(float));
			break;
		case AVG:
			memcpy((char*) temp + offset, &gInfo[i].avg, sizeof(float));
			break;
		case SUM:
			memcpy((char*) temp + offset, &gInfo[i].sum, sizeof(float));
			break;
		case COUNT:
			memcpy((char*) temp + offset, &gInfo[i].count, sizeof(float));
			break;
		}
		offset += sizeof(float);
		aggregate_data.push_back(temp);
		aggregate_length.push_back(offset);
		count++;
	}
	this->attrs.push_back(gAttr);
	string name;
	Attribute temp_attr;
	switch (op) {
	case MIN:
		name = "MIN";
		break;
	case MAX:
		name = "MAX";
		break;
	case SUM:
		name = "SUM";
		break;
	case AVG:
		name = "AVG";
		break;
	case COUNT:
		name = "COUNT";
		break;
	}
	name += "(" + aggAttr.name + ")";
	temp_attr.name = name;
	switch (op) {
	case MIN:
		;
		break;
	case MAX:
		;
		break;
	case SUM:
		temp_attr.type = aggAttr.type;
		break;
	case COUNT:
		temp_attr.type = TypeInt;
		break;
	case AVG:
		temp_attr.type = TypeReal;
		break;
	}
	temp_attr.length = 4;
	this->attrs.push_back(temp_attr);
	free(data);
}
