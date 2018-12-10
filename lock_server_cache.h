#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"


struct lock_client_id_and_seqnum {
	std::string client_id;
	lock_protocol::seqnum_t seqnum;
};

template<class T>
class thread_safe_queue {
 // method invokation blocks on a mutex, though I think
 // it's ok to use this queue in handlers, which should
 // run to completion without blocking, since mutex is
 // locked for very short time, and no RPCs are called while holding this mutex
private:
	std::list<T> queue;
	pthread_mutex_t m;
	pthread_cond_t cv;
public:
	thread_safe_queue() {
		pthread_mutex_init(&m, NULL);
		pthread_cond_init(&cv, NULL);
	}
	void push_back(T a) {
		pthread_mutex_lock(&m);
		queue.push_back(a);
		pthread_cond_broadcast(&cv);
		pthread_mutex_unlock(&m);
	}
	T pop_front() {
		pthread_mutex_lock(&m);
		T r;
		while(1) {
	    if (!queue.empty()) {
        r = *queue.begin();
	      queue.pop_front();
	      break;
	    }
	    pthread_cond_wait(&cv, &m);
	  }
		pthread_mutex_unlock(&m);
		return r;
	}
};


class lock_server_cache {
 public:
  lock_server_cache();
  void revoker();
  void retryer();
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, std::string client_socket, lock_protocol::seqnum_t seqnum, lock_protocol::lockid_t lid, int &r);
  // lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, std::string client_socket, lock_protocol::seqnum_t seqnum, lock_protocol::lockid_t lid, int &r);

protected:
  int nacquire = 0;

  class lock {
  private:
    static const int FREE = 0;
  	static const int LOCKED = 1;

  public:
    int lock_state;
    // pthread_cond_t cond_var;
    std::string owner; // lock_client_cache::id
    lock_protocol::seqnum_t seqnum; // seqnum of last acquire
    std::list<lock_client_id_and_seqnum> waiting_list;

  public:
    lock() {
      lock_state = FREE;
      // pthread_cond_init(&cond_var, NULL);
    }

    bool is_free() { return (lock_state == FREE); }

    void grant_lock(std::string client_id, lock_protocol::seqnum_t seqnum_of_request) {
    	lock_state = LOCKED;
    	owner = client_id;
    	seqnum = seqnum_of_request;
    }

    void release_lock() {
    	lock_state = FREE;
    }

    void add_to_waiting_list(std::string client_id, lock_protocol::seqnum_t seqnum_of_request) {
    	lock_client_id_and_seqnum t;
    	t.client_id = client_id;
    	t.seqnum = seqnum_of_request;
    	waiting_list.push_back(t);
    }

    void get_from_waiting_list(std::string &client_id, lock_protocol::seqnum_t &seqnum_of_request) {
      lock_client_id_and_seqnum t;
      t = *waiting_list.begin();
      waiting_list.pop_front();
      client_id = t.client_id;
      seqnum_of_request = t.seqnum;
    }

    bool waiting_list_is_empty() {
      return waiting_list.empty();
    }

    std::list<lock_client_id_and_seqnum> get_waiting_list_and_clear_it() {
      std::list<lock_client_id_and_seqnum> t;
      t = waiting_list;
      waiting_list.clear();
      return t;
    }

  };

  struct lock_client_info {
  	int nacquire;
  	rpcc *cl;

  	lock_client_info(std::string client_socket) {
  		nacquire = 0;

  		sockaddr_in dstsock;
      make_sockaddr(client_socket.c_str(), &dstsock);
      cl = new rpcc(dstsock);
      if (cl->bind() < 0) {
        printf("lock_client: call bind\n");
      }
  	}
    lock_client_info() {
      nacquire = 0;
    }


  };
    
protected:
  std::map<lock_protocol::lockid_t, lock> locks;
  std::map<std::string, lock_client_info> lock_clients;
  pthread_mutex_t release_acquire_mutex; // this mutex protects locks, lock_clients and most importantly, nacquire

  thread_safe_queue<lock_protocol::lockid_t> retrier_queue; // push_back to safe_queue wakes up retrier
  thread_safe_queue<lock_protocol::lockid_t> revoker_queue; // push_back to safe_queue wakes up revoker

};

#endif
