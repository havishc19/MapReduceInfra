#pragma once

#include <string>
#include <iostream>
#include <fstream>
#include <vector>

using namespace std;

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);


        vector<string> _fileNames;
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
    string fileName = "temp/" + key + ".txt";
    ofstream outfile;
	outfile.open(fileName, ios_base::app);
	outfile << val+"\n"; 
	outfile.close();
    _fileNames.push_back(key+".txt");
}


/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

        int _fileNumber; 

        string _outputDir;
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
    string fileName = _outputDir + "/" + to_string(_fileNumber) + ".txt";
    ofstream outfile;
    outfile.open(fileName, ios_base::app);
    outfile << key + "," + val + "\n"; 
    outfile.close();
}
