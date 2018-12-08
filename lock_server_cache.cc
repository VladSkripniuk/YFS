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
  pthread_mutex_init(&locks_map_mutex, NULL);
  pthread_mutex_init(&lock_clients_map_mutex, NULL);
 
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
    lid = revoker_queue.pop_front();

    // TODO: send revoke RPC
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

    // TODO: send retry RPC
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

  if (it == locks.end()) {
    lock new_lock;
    locks[lid] = new_lock;
    it = locks.find(lid);
  }

  if (it->second.is_free()) {
    std::map<std::string, lock_client_info>::iterator it;
    it = lock_clients.find(client_socket);

    if (it == lock_clients.end()) {
      lock_client_info new_lock_client_info(client_socket);

      lock_clients[client_socket] = new_lock_client_info;
      it = lock_clients.find(client_socket);
    }

    it->second.nacquire += 1;
    locks[lid].grant_lock(client_socket, lid);
    
  }
  else {
    locks[lid].add_to_waiting_list(client_socket, lid);

    revoker_queue.push_back(lid);

  }
  
  std::cout << "acquire done (clt " << clt << ", lock id: " << lid << ")\n";
  pthread_mutex_unlock(&release_acquire_mutex);

  return lock_protocol::OK;
}


lock_protocol::status
lock_server_cache::release(int clt, std::string client_socket, lock_protocol::seqnum_t seqnum, lock_protocol::lockid_t lid, int &r) {
  
  std::cout << "release request (clt " << clt << ", lock id: " << lid << ")\n";
  pthread_mutex_lock(&release_acquire_mutex);

  std::map<lock_protocol::lockid_t,lock>::iterator it;
  it = locks.find(lid);

  if ((it != locks.end()) && (it->second.lock_state == LOCKED)) {
    nacquire--;

    std::map<std::string,lock_client_info>::iterator it;
    it = lock_clients.find(client_socket);
    if (it != lock_clients.end()) {
      it->nacquire -= 1;
    }

    it->second.lock_state = FREE;
    
    retrier_queue.push_back(lid);
  }
 
  pthread_mutex_unlock(&release_acquire_mutex);
  
  return lock_protocol::OK;
}
