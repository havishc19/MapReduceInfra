#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"

#include<vector>
#include<set>
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
	void run_reducer();
	void makeMapperRpcCall(string,int);
	void makeReducerRpcCall(string,int);

	MapReduceSpec spec;
	vector<FileShard> shards;
	int cur_shard_index;
	int cur_output_index;
	int partition_size;

	int busy_workers;

	//intermediate file_locations for each mapper in sorted order
	vector<string> intermediate_fileloc;

	//RPC calls
    std::vector<std::unique_ptr<MasterWorker::Stub>>stubs_;

    CompletionQueue cq;

    struct AsyncClientCall {
      // Container for the data we expect from the server.
      WorkerReply reply;

      string worker_address;
      int worker_id; // for the worker channel

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
	cur_output_index =  0;
	busy_workers = 0;

	 for(auto addr : spec.workerAddr){
        stubs_.emplace_back( MasterWorker::NewStub(grpc::CreateChannel(addr, grpc::InsecureChannelCredentials())) ); 
      }
}

void Master::makeMapperRpcCall(string worker_address, int worker_id){
		MapperQuery *mapper_query;
		MasterQuery query;
		Shard *shard;
        mapper_query = query.mutable_mapperquery();
        shard = mapper_query->mutable_shard();
		shard->set_filename(shards[cur_shard_index].filename);
		shard->set_id(shards[cur_shard_index].id);
		shard->set_startbyte(shards[cur_shard_index].startByte);
		shard->set_endbyte(shards[cur_shard_index].endByte);


		AsyncClientCall* call = new AsyncClientCall;
		call->worker_address = worker_address;
		call->worker_id = worker_id;
		query.set_type(0);
		// cout << "Making RPC Call on worker_id : " << worker_id << " - addr: " << worker_address << endl; 
  		call->response_reader = stubs_[worker_id]->PrepareAsyncmapReduceQuery(&call->context, query, &cq);

		call->response_reader->StartCall();
  		call->response_reader->Finish(&call->reply, &call->status, (void*)call);
  		// cout << "Done RPC" << endl;

}

struct alpha_sort
{
	  bool operator() (const string& lhs, const string& rhs) const {
        string s1, s2;
        for(int i=0; i<lhs.length(); i++)
        	s1 += tolower(lhs[i]);
        for(int i=0; i<rhs.length(); i++)
        	s2 += tolower(rhs[i]);
        return s1 < s2;
    }
};
void Master::run_mapper(){
	
	int w_count = 0;
	for(auto worker: spec.workerAddr) {
        if(cur_shard_index>=shards.size())
        	break;
        cout << worker << endl;
        makeMapperRpcCall(worker, w_count);
		w_count++;
		cur_shard_index++;
		busy_workers++;
	}

	void* got_tag;
  	bool ok = false;

	WorkerReply reply;

	Status status = Status::OK;

	set<string,alpha_sort> unique_filelist;

	while (cq.Next(&got_tag, &ok)) {

		AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);


		busy_workers--;
		GPR_ASSERT(ok);
		if (call->status.ok())  {

			for (const auto result : call->reply.locations().filename()) {
				cout<<result<<" :[MASTER RESULT]"<<endl;
		    	unique_filelist.insert(result);
			}
			cout << "RPC done" << endl;

			//break if all shards are complete.
			if(busy_workers == 0)
				break;

	        cout << call->worker_address << endl;
	        if(cur_shard_index < shards.size()) {
				makeMapperRpcCall(call->worker_address, call->worker_id);
				cur_shard_index++;
				busy_workers++;
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
	for(auto it = unique_filelist.begin(); it != unique_filelist.end(); it++){
		//cout << *it << endl;
		intermediate_fileloc.push_back(*it);
	}
    cout << intermediate_fileloc.size() << endl;
    


}


void Master::makeReducerRpcCall(string worker_address, int worker_id){
		ReducerQuery *reducer_query;
		MasterQuery query;
		FileLocations *locations;

        reducer_query = query.mutable_reducerquery();
        locations = reducer_query->mutable_locations();
        for(int i=0 ; i<partition_size && cur_output_index*partition_size+i < intermediate_fileloc.size(); i++){
        	locations->add_filename(intermediate_fileloc[cur_output_index*partition_size+i]);
        }
        reducer_query->set_partitionid(cur_output_index);


		AsyncClientCall* call = new AsyncClientCall;
		call->worker_address = worker_address;
		call->worker_id = worker_id;
		query.set_type(1);
  		call->response_reader = stubs_[worker_id]->PrepareAsyncmapReduceQuery(&call->context, query, &cq);

		call->response_reader->StartCall();
  		call->response_reader->Finish(&call->reply, &call->status, (void*)call);

}

void Master::run_reducer(){
	partition_size = intermediate_fileloc.size()/spec.numOutFiles;
    cout << "Num Out Files: " << spec.numOutFiles << endl;
    cout << "Partition Size: " << partition_size << endl;
	int w_count = 0;
	for(auto worker: spec.workerAddr) {
        cout << "Reducer: " << worker << endl;
        if(cur_output_index>=partition_size)
        	break;
        makeReducerRpcCall(worker, w_count);
		w_count++;
		cur_output_index++;
	}

	void* got_tag;
  	bool ok = false;

	WorkerReply reply;

	Status status = Status::OK;

	while (cq.Next(&got_tag, &ok)) {
		AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);

		GPR_ASSERT(ok);
		// if (call->status.ok())  {

			// for (const auto result : call->reply.locations().filename()) {
		 //    	std::cout << "file: " << result << std::endl;
			// }

			//break if all shards are complete.
			if(cur_output_index >= spec.numOutFiles)
				break;

			makeReducerRpcCall(call->worker_address, call->worker_id);
			cur_output_index++;
		// }
		// else{
		// 	status = call->status;
		// 	std::cerr << "Hi there" << endl;
		//   	break;
		// } 

		// Once we're complete, deallocate the call object.
		delete call;
	}
}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	run_mapper();
	cout << "Mapper Done" << endl;
	run_reducer();
	return true;
}
