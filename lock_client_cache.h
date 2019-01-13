// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "extent_client.h"

// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 6.
class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user() {};
};


class lock_release_user_derived : public lock_release_user {
 public:
  extent_client *ec;
  lock_release_user_derived(extent_client *ec_) {
    ec = ec_;
  }
  virtual void dorelease(lock_protocol::lockid_t lockid) {
      std::cout << "hello" << lockid << std::endl;
    ec->flush(lockid);
  }
  virtual ~lock_release_user_derived() {};
};


// SUGGESTED LOCK CACHING IMPLEMENTATION PLAN:
//
// to work correctly for lab 7,  all the requests on the server run till 
// completion and threads wait on condition variables on the client to
// wait for a lock.  this allows the server to be replicated using the
// replicated state machine approach.
//
// On the client a lock can be in several states:
//  - free: client owns the lock and no thread has it
//  - locked: client owns the lock and a thread has it
//  - acquiring: the client is acquiring ownership
//  - releasing: the client is releasing ownership
//
// in the state acquiring and locked there may be several threads
// waiting for the lock, but the first thread in the list interacts
// with the server and wakes up the threads when its done (released
// the lock).  a thread in the list is identified by its thread id
// (tid).
//
// a thread is in charge of getting a lock: if the server cannot grant
// it the lock, the thread will receive a retry reply.  at some point
// later, the server sends the thread a retry RPC, encouraging the client
// thread to ask for the lock again.
//
// once a thread has acquired a lock, its client obtains ownership of
// the lock. the client can grant the lock to other threads on the client 
// without interacting with the server. 
//
// the server must send the client a revoke request to get the lock back. this
// request tells the client to send the lock back to the
// server when the lock is released or right now if no thread on the
// client is holding the lock.  when receiving a revoke request, the
// client adds it to a list and wakes up a releaser thread, which returns
// the lock the server as soon it is free.
//
// the releasing is done in a separate a thread to avoid
// deadlocks and to ensure that revoke and retry RPCs from the server
// run to completion (i.e., the revoke RPC cannot do the release when
// the lock is free.
//
// a challenge in the implementation is that retry and revoke requests
// can be out of order with the acquire and release requests.  that
// is, a client may receive a revoke request before it has received
// the positive acknowledgement on its acquire request.  similarly, a
// client may receive a retry before it has received a response on its
// initial acquire request.  a flag field is used to record if a retry
// has been received.
//


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


class cached_lock {
public:
  enum xxstatus { NONE, FREE, LOCKED, ACQUIRING, RELEASING };

  int lock_state;
  pthread_cond_t cond_var;
  lock_protocol::seqnum_t seqnum = 0; // seqnum of last acquire

  cached_lock() {
    lock_state = FREE;
    pthread_cond_init(&cond_var, NULL);
  }

};

class lock_client_cache : public lock_client {

private:
  class lock_release_user *lu;
  int rlock_port;
  std::string hostname;

  lock_protocol::seqnum_t seqnum = 0;

  std::map<lock_protocol::lockid_t, cached_lock> cached_locks;
    
  pthread_mutex_t release_acquire_mutex; // this mutex protects locks and seqnum

  thread_safe_queue<lock_protocol::lockid_t> releaser_queue; // push_back to releaser_queue wakes up releaser

public:
  rlock_protocol::status release_to_lock_server(rlock_protocol::seqnum_t seqnum, lock_protocol::lockid_t lid);
private:
  rlock_protocol::status accept_retry_request(rlock_protocol::seqnum_t seqnum, lock_protocol::lockid_t lid, int &r);
  rlock_protocol::status accept_revoke_request(rlock_protocol::seqnum_t seqnum, lock_protocol::lockid_t lid, int &r);
  
public:
  std::string id;
  int n_successes = 0;
  int n_failures = 0;

public:
  static int last_port;
  lock_client_cache(std::string xdst, class lock_release_user *l = 0);
  virtual ~lock_client_cache() {};
  lock_protocol::status acquire(lock_protocol::lockid_t);
  virtual lock_protocol::status release(lock_protocol::lockid_t);
  void releaser();
};
#endif


