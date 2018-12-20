// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <cstdio>

static void *
revokethread(void *x)
{
  lock_server_cache *sc = (lock_server_cache *) x;
  sc->revoker();
  return 0;
}

static void *
retrythread(void *x)
{
  lock_server_cache *sc = (lock_server_cache *) x;
  sc->retryer();
  return 0;
}

lock_server_cache::lock_server_cache()
{
  pthread_mutex_init(&release_acquire_mutex, NULL);
 
  pthread_t th;
  int r = pthread_create(&th, NULL, &revokethread, (void *) this);
  assert (r == 0);
  r = pthread_create(&th, NULL, &retrythread, (void *) this);
  assert (r == 0);
}

void
lock_server_cache::revoker()
{

  // This method should be a continuous loop, that sends revoke
  // messages to lock holders whenever another client wants the
  // same lock
  lock_protocol::lockid_t lid;
  while (1) {
    std::cout << "revoker1\n";
    lid = revoker_queue.pop_front();
    std::cout << "revoker2\n";
    // send revoke RPC
    pthread_mutex_lock(&release_acquire_mutex);
    // std::cout << "revoker3\n";
    rpcc *cl;
    std::cout << "revoke: " << locks[lid].owner << " " << lid << std::endl;

    auto it = lock_clients.find(locks[lid].owner);
    assert (it != lock_clients.end());
    cl = it->second.cl;
    
    rlock_protocol::seqnum_t seqnum;
    seqnum = locks[lid].seqnum;
  
    pthread_mutex_unlock(&release_acquire_mutex);

    int r;
    int ret = cl->call(rlock_protocol::revoke, seqnum, lid, r);
    assert (ret == rlock_protocol::OK);

  }

}


void
lock_server_cache::retryer()
{

  // This method should be a continuous loop, waiting for locks
  // to be released and then sending retry messages to those who
  // are waiting for it.
  lock_protocol::lockid_t lid;
  while (1) {
    lid = retrier_queue.pop_front();

    std::cout << "lock_server_cache::retryer: " << lid << std::endl;

    // TODO: send retry RPC
    pthread_mutex_lock(&release_acquire_mutex);
    n_retries += 1;
    std::cout << "RETRIES SENT " << n_retries << std::endl;
  
    // std::list<lock_client_id_and_seqnum> waiting_list;
    // waiting_list = locks[lid].get_waiting_list_and_clear_it();

    std::cout << "waiting list\n";
    for (auto it = locks[lid].waiting_list.begin(); it != locks[lid].waiting_list.end(); it++) {
      std::cout << "\t" << it->client_id << std::endl;
    }
    // std::cout << "waiting list copied\n";
    // pthread_mutex_unlock(&release_acquire_mutex);
    
    std::string client_id;
    rlock_protocol::seqnum_t seqnum;

    rpcc *cl;

 
    if (!locks[lid].waiting_list.empty()) {
      lock_client_id_and_seqnum t = *(locks[lid].waiting_list.begin());
      locks[lid].waiting_list.pop_front();

      // pthread_mutex_lock(&release_acquire_mutex);
      auto it = lock_clients.find(t.client_id);
      assert (it != lock_clients.end());
      cl = it->second.cl;
      pthread_mutex_unlock(&release_acquire_mutex);
      int r;
      int ret = cl->call(rlock_protocol::retry, seqnum, lid, r);

      std::cout << "retrier call " << ret <<" " <<r<<std::endl;

      std::cout << "retrier: retry sent to " << t.client_id << " " << lid << std::endl;
      assert (ret == rlock_protocol::OK);
    }
    else{
      pthread_mutex_unlock(&release_acquire_mutex);
    }
    
    
    // pthread_mutex_unlock(&release_acquire_mutex);


  }

}


lock_protocol::status
lock_server_cache::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  r = nacquire;
  return lock_protocol::OK;
}


lock_protocol::status
lock_server_cache::acquire(int clt, std::string client_socket, lock_protocol::seqnum_t seqnum, lock_protocol::lockid_t lid, int &r) {

  std::cout << "acquire request (clt " << client_socket << ", seqnum " << seqnum << ", lock id: " << lid << ")\n";
  pthread_mutex_lock(&release_acquire_mutex);

  std::map<lock_protocol::lockid_t, lock>::iterator it;
  it = locks.find(lid);

  // add new lock if it didn't exist before
  if (it == locks.end()) {
    lock new_lock;
    locks[lid] = new_lock;
    it = locks.find(lid);
  }

  // add new lock_client if it didn't exist before
  if (lock_clients.find(client_socket) == lock_clients.end()) {
    lock_client_info new_lock_client_info(client_socket);
    // std::cout << "acquire, inserted: " << client_socket << std::endl;
    // lock_clients[client_socket] = new_lock_client_info;
    lock_clients.insert(std::pair<std::string, lock_client_info>(client_socket, new_lock_client_info));
  }


  if (it->second.is_free()) {
    auto it = lock_clients.find(client_socket);

    it->second.nacquire += 1;
    locks[lid].grant_lock(client_socket, lid);

    r = lock_protocol::OK;
    std::cout << "acquire request successful: " << client_socket << " " << lid << std::endl;

    /// in case someone else is also waiting on this lock right now, ask client to return this lock
    /// that may cause revoke coming to client before acquire lock_protocol::OK
    /// actually not, because we hold release_acquire_mutex
    if (!locks[lid].waiting_list.empty()) {
      revoker_queue.push_back(lid);
    }
    
  }
  else {
    locks[lid].add_to_waiting_list(client_socket, lid);

    revoker_queue.push_back(lid);
    std::cout << "acquire request retry: " << client_socket << " " << lid << std::endl;

    r = lock_protocol::RETRY;

  }
  
  // std::cout << "acquire done (clt " << clt << ", lock id: " << lid << ")\n";
  pthread_mutex_unlock(&release_acquire_mutex);

  return r;
}


lock_protocol::status
lock_server_cache::release(int clt, std::string client_socket, lock_protocol::seqnum_t seqnum, lock_protocol::lockid_t lid, int &r) {
  
  std::cout << "release request (clt " << client_socket << ", lock id: " << lid << ")\n";
  pthread_mutex_lock(&release_acquire_mutex);

  std::map<lock_protocol::lockid_t,lock>::iterator it;
  it = locks.find(lid);

  if ((it != locks.end()) && (!it->second.is_free())) {
    nacquire--;

    std::map<std::string,lock_client_info>::iterator it1;
    it1 = lock_clients.find(client_socket);
    if (it1 != lock_clients.end()) {
      it1->second.nacquire -= 1;
    }

    it->second.release_lock();
    // std::cout << "lock_server_cache::release: pushed to retrier\n";
    
    retrier_queue.push_back(lid);
  }
 
  pthread_mutex_unlock(&release_acquire_mutex);
  
  return lock_protocol::OK;
}
