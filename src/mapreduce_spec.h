#pragma once

#include <string>
#include <fstream>
#include <iostream>
#include <vector>

using namespace std;

/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
	int numWorkers;
	vector<string> workerAddr;
	vector<string> inFiles;
	string outDir;
	int numOutFiles;
	int mapKilobytes;
	string userID;
};

inline vector<string> parseCSV(string input) {
	vector<string> retVal;
	string temp = "";
	for(int i=0; i<input.length(); i++) {
		if(input[i] == ',') {
			retVal.push_back(temp);
			temp = "";
		} else {
			temp += string(1, input[i]);
		}
	}
	retVal.push_back(temp);
	return retVal;
}

/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
	ifstream inFile(config_filename);
	string line;
	if(inFile.is_open()) {
		while(getline(inFile, line)) {
			string key = line.substr(0, line.find("="));
			string val = line.substr(line.find("=")+1, line.length());
			if(key == "n_workers") {
				mr_spec.numWorkers = stoi(val);
			}
			if(key == "worker_ipaddr_ports") {
				vector<string> temp = parseCSV(val);
				mr_spec.workerAddr = temp;
			}
			if(key == "input_files") {
				vector<string> temp = parseCSV(val);
				mr_spec.inFiles = temp;
			}
			if(key == "output_dir") {
				mr_spec.outDir = val;
			}
			if(key == "n_output_files") {
				mr_spec.numOutFiles = stoi(val);
			}
			if(key == "map_kilobytes") {
				mr_spec.mapKilobytes = stoi(val);
			}
			if(key == "user_id") {
				mr_spec.userID = val;
			}
		}
		inFile.close();
	}
	return true;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
	if(mr_spec.numWorkers == 0 || mr_spec.workerAddr.size() == 0 || mr_spec.inFiles.size() == 0 || mr_spec.outDir == "" || mr_spec.numOutFiles == 0 || mr_spec.mapKilobytes == 0 || mr_spec.userID == "") {
		return false;
	}
	return true;
}
