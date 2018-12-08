#pragma once

#include <vector>
#include "mapreduce_spec.h"
#include <cstdio>
#include <iostream>
#include <fstream>

using namespace std;

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
	int id;
	string filename;
	int startByte;
	int endByte;
};

inline void printShards(vector<FileShard> &fileShards){

	for(int i=0; i < fileShards.size(); i++){
		cout << fileShards[i].id << " " << fileShards[i].filename << " " << fileShards[i].startByte << " " << fileShards[i].endByte << endl; 
	}
	return;
}

/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
	cout << "FileShard" << endl;
	int shardSize = mr_spec.mapKilobytes;
	vector<string> inputFiles = mr_spec.inFiles;
	// int shardSize = 500;
	// vector<string> inputFiles{"../test/input/testdata_1.txt"};
	int counter = 0;
	for(int i=0; i<inputFiles.size(); i++) {
		ifstream fileObj(inputFiles[i]);
		string line;
		// fileObj.open(inputFiles[i]);
		int start = 0;
		int end = 0;
		int curBytes = 0;
		if(fileObj.is_open()) {
			while(getline(fileObj, line)) {
				if(curBytes + line.length() <= shardSize) {
					curBytes += line.length();
				} else {
					struct FileShard fs;
					fs.id = counter++; 
					fs.filename = inputFiles[i];
					fs.startByte = start;
					fs.endByte = start+curBytes;
					fileShards.push_back(fs);
					start += curBytes+1;
					curBytes = line.length();
				}
			}
			fileObj.close();
		}
	}
	cout << "bowbow" << endl;
	cout << fileShards.size() << endl;
	printShards(fileShards);
	return true;
}
