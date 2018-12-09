#pragma once

#include <vector>
#include "mapreduce_spec.h"
#include <cstdio>
#include <iostream>
#include <fstream>

using namespace std;

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct shardDetails {
	string filename;
	int startByte;
	int endByte;
};

struct FileShard {
	int id;
	vector<struct shardDetails> details;
};

inline void printShards(vector<FileShard> &fileShards){

	for(int i=0; i < fileShards.size(); i++){
		cout<<fileShards[i].id<<":";
		// cout << fileShards[i].id << ":" << fileShards[i].filename << " " << fileShards[i].startByte << " " << fileShards[i].endByte << endl; 
		for(int j=0;j <fileShards[i].details.size(); j++) {
			cout<<fileShards[i].details[j].filename<<":" << fileShards[i].details[j].startByte << " " << fileShards[i].details[j].endByte << ";";
		}
		cout<<endl;
	}
	return;
}

/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
	int shardSize = mr_spec.mapKilobytes;
	vector<string> inputFiles = mr_spec.inFiles;
	int counter = 0;
	vector<struct shardDetails> tempDetVec;
	int start = 0;
	int curBytes = 0;
	for(int i=0; i<inputFiles.size(); i++) {
		cout<<"Entrering new file"<<endl;
		bool firstLineFlag = true;
		int fileOffset = 0;
		ifstream fileObj(inputFiles[i]);
		string line;
		if(fileObj.is_open()) {
			while(getline(fileObj, line)) {
				cout<<curBytes<<":"<<line.length()<<":"<<shardSize<<":"<<fileOffset<<endl;
				if(curBytes + line.length() <= shardSize || curBytes == 0) {
					cout<<inputFiles[i]<<"::::"<<curBytes<<"::::"<<fileOffset<<endl;
					if(fileOffset != 0) {
						curBytes++;
						fileOffset++;
					}
					curBytes += line.length();
					fileOffset += line.length();
				} else {
					struct FileShard fs;
					fs.id = counter++;
					if(fileOffset != 0) {
						struct shardDetails det;
						det.filename = inputFiles[i];
						det.startByte = start;
						det.endByte = start+fileOffset-1;
						tempDetVec.push_back(det);
					}
					fs.details = tempDetVec;
					tempDetVec.clear();
					fileShards.push_back(fs);
					if(fileOffset == 0) {
						start = 0;
					} else {
						start += fileOffset+1;
					}
					curBytes = line.length();
					fileOffset = curBytes;
				}
			}
			struct shardDetails tempDet;
			tempDet.filename = inputFiles[i];
			tempDet.startByte = start;
			tempDet.endByte = start+fileOffset-1;
			tempDetVec.push_back(tempDet);
			start = 0;
			// curBytes += line.length();
			cout<<"setting to zero"<<endl;
			fileOffset = 0;


			// struct FileShard fs;
			// fs.id = counter++; 
			// fs.filename = inputFiles[i];
			// fs.startByte = start;
			// fs.endByte = start+curBytes-1;
			// fileShards.push_back(fs);
			fileObj.close();
		}
	}
	if(tempDetVec.size() != 0) {
		struct FileShard fs;
		fs.id = counter++; 
		fs.details = tempDetVec;
		tempDetVec.clear();
		fileShards.push_back(fs);
	}
	// cout << "bowbow" << endl;
	// cout << fileShards.size() << endl;
	printShards(fileShards);
	return true;
}
