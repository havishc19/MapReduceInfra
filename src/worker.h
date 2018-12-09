#pragma once

#include <mr_task_factory.h>
#include "mr_tasks.h"
#include <string>
#include <fstream>

using namespace std;

#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>

#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"
#include <grpc++/grpc++.h>

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;


using masterworker::MapperQuery;
using masterworker::Shard;
using masterworker::ReducerQuery;
using masterworker::WorkerReply;
using masterworker::FileLocations;
using masterworker::MasterQuery;
using masterworker::MasterWorker;

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);


void ReplaceStringInPlace(string& subject, string search, string replace) {
    size_t pos = 0;
    while ((pos = subject.find(search, pos)) != std::string::npos) {
         subject.replace(pos, search.length(), replace);
         pos += replace.length();
    }
}

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		string server_address;

		class MapperService final {
		 public:

		  ~MapperService() {
		    server_->Shutdown();
		    cq_->Shutdown();
		  }

		  void Run(string IP_ADDR_PORT) {
		    string server_address(IP_ADDR_PORT);
		    ServerBuilder builder;
		    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
		    builder.RegisterService(&service_);
		    cq_ = builder.AddCompletionQueue();
		    server_ = builder.BuildAndStart();
		    cout << "Server listening on " << server_address << endl;
		    HandleRpcs();
		  }


		 private:
		  class CallData {
		   public:
		    CallData(MasterWorker::AsyncService* service, ServerCompletionQueue* cq)
		        : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
		      Proceed();
		    }
		        
		        
		        void Proceed() {
		      if (status_ == CREATE) {
		        status_ = PROCESS;
		        service_->RequestmapReduceQuery(&ctx_, &request_, &responder_, cq_, cq_,
		                                  this);
		      } else if (status_ == PROCESS) {
	        		// Mapper query
			      	new CallData(service_, cq_);
		      		// cout << "Worker type: " << request_.type() << endl;
					if(request_.type() == 0) {
						// cout << "Mapper Req received!" << endl;
						auto mapper = get_mapper_from_task_factory("cs6210");
		        		// Get shard and details
			        	Shard sh = request_.mapperquery().shard();
			        	string filename = sh.filename();
			        	int startByte = sh.startbyte();
			        	int endByte = sh.endbyte();

			        	//Read filename from startByte to endByte and create input string for mapper
	        			ifstream fileObj(filename);
						string inputLine = "";
						if(fileObj.is_open()) {
							fileObj.seekg(startByte, ios::beg);
							char line[endByte-startByte+2];
							fileObj.read(line, endByte-startByte+1);
							line[endByte-startByte+1] = 0;
							fileObj.close();
							inputLine = line;
							ReplaceStringInPlace(inputLine, "\n", " ");
							cout<<inputLine<<" [INPUT]"<<endl;
							mapper->map(inputLine);
						}
						vector<string> fileNames = mapper->impl_->_fileNames;
						FileLocations *locations;
						locations = reply_.mutable_locations();

						for(int i=0;i<fileNames.size();i++){
							cout<<fileNames[i]<<" :[FILE]"<<endl;
							locations->add_filename(fileNames[i]);
						}
						status_ = FINISH;
						responder_.Finish(reply_, Status::OK, this);
		        	}
		        	else{
		        		// cout << "Redcuer Called" << endl;
		        		auto reducer = get_reducer_from_task_factory("cs6210");
		        		reducer->impl_->_fileNumber = request_.reducerquery().partitionid();

		        		cout<<"Partitionid="<<request_.reducerquery().partitionid()<<endl;
		        		for(const auto fileName : request_.reducerquery().locations().filename()){
		        			vector<string> values;
		        			string line;
		        			ifstream fileObj("temp/" + fileName);
		        			while(getline(fileObj, line)) {
		        				values.push_back(line);
		        			}
		        			fileObj.close();
		        			reducer->reduce(fileName.substr(0,fileName.length()-4), values);
		        		}
		        		FileLocations *locations;
						locations = reply_.mutable_locations();
						locations->add_filename(to_string(request_.reducerquery().partitionid()) + ".txt");
		        		status_ = FINISH;
						responder_.Finish(reply_, Status::OK, this);
		        	}
		      } else {
		        GPR_ASSERT(status_ == FINISH);
		        delete this;
		      }
		    }

		   private:
		    MasterWorker::AsyncService* service_;
		    ServerCompletionQueue* cq_;
		    ServerContext ctx_;
		    MasterQuery request_;
		    WorkerReply reply_;
		    ServerAsyncResponseWriter<WorkerReply> responder_;
		    enum CallStatus { CREATE, PROCESS, FINISH };
		    CallStatus status_;
		  };

		  void HandleRpcs() {
		    new CallData(&service_, cq_.get());
		    void* tag;  
		    bool ok;
		    while (true) {
		      GPR_ASSERT(cq_->Next(&tag, &ok));
		      GPR_ASSERT(ok);
		      static_cast<CallData*>(tag)->Proceed();
		    }
		  }

		  unique_ptr<ServerCompletionQueue> cq_;
		  MasterWorker::AsyncService service_;
		  unique_ptr<Server> server_;
		};

};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {
	server_address = ip_addr_port;
}



/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	/*  Below 5 lines are just examples of how you will call map and reduce
		Remove them once you start writing your own logic */ 
	MapperService service;
	service.Run(server_address);
	return true;
}
