// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include <map>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"

class lock_server {

	struct lock_t {
		int lock;
		pthread_cond_t cv;
	  pthread_mutex_t mp;
	};

 protected:
  int nacquire;
  std::map<lock_protocol::lockid_t, lock_t> locks;
  pthread_mutex_t insert_new_lock_mp;

 public:

 	static const int FREE = 0;
 	static const int LOCKED = 1;

  lock_server();
  ~lock_server() {};
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif 







