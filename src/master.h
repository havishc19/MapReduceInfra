#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"

#include<vector>
#include<unistd.h>

#include <grpc++/grpc++.h>
#include "masterworker.grpc.pb.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;


using masterworker::MapperQuery;
using masterworker::Shard;
using masterworker::ReducerQuery;
using masterworker::WorkerReply;
using masterworker::FileLocations;
using masterworker::MasterQuery;
using masterworker::MasterWorker;

using namespace std;



/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
	//functions
	void run_mapper();

	MapReduceSpec spec;
	vector<FileShard> shards;
	int cur_shard_index;

	//intermediate file_locations for each mapper
	vector<string> intermediate_fileloc;

	//RPC calls
    std::vector<std::unique_ptr<MasterWorker::Stub>>stubs_;

    CompletionQueue cq;

    struct AsyncClientCall {
      // Container for the data we expect from the server.
      WorkerReply reply;

      // Context for the client. It could be used to convey extra information to
      // the server and/or tweak certain RPC behaviors.
      ClientContext context;

      // Storage for the status of the RPC upon completion.
      Status status;

      std::unique_ptr<ClientAsyncResponseReader<WorkerReply>> response_reader;
    };


};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
	spec = mr_spec;
	shards = file_shards;
	cur_shard_index =  0;
	 for(auto addr : spec.workerAddr){
        stubs_.emplace_back( MasterWorker::NewStub(grpc::CreateChannel(addr, grpc::InsecureChannelCredentials())) ); 
      }
}

void Master::run_mapper(){
	MapperQuery *mapper_query;
	MasterQuery query;
	Shard *shard;
	int w_count = 0;
	for(auto worker: spec.workerAddr) {
		
        cout << worker << endl;
        mapper_query = query.mutable_mapperquery();
        shard = mapper_query->mutable_shard();
		shard->set_filename(shards[cur_shard_index].filename);
		shard->set_id(shards[cur_shard_index].id);
		shard->set_startbyte(shards[cur_shard_index].startByte);
		shard->set_endbyte(shards[cur_shard_index].endByte);


		AsyncClientCall* call = new AsyncClientCall;
  		call->response_reader = stubs_[w_count]->PrepareAsyncmapReduceQuery(&call->context, query, &cq);

		call->response_reader->StartCall();
  		call->response_reader->Finish(&call->reply, &call->status, (void*)call);
		w_count++;
	}

	void* got_tag;
  	bool ok = false;

	WorkerReply reply;

	Status status = Status::OK;

	while (cq.Next(&got_tag, &ok)) {
		AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);

		GPR_ASSERT(ok);
		if (call->status.ok())  {

			for (const auto result : call->reply.locations().filename()) {
		    	std::cout << "file: " << result << std::endl;
			}
		}
		else{
			status = call->status;
			std::cerr << "Hi there" << endl;
		  	break;
		} 

		// Once we're complete, deallocate the call object.
		delete call;
	}
}



/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	run_mapper();
	return true;
}
