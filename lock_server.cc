// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <iostream>

lock_server::lock_server():
  nacquire (0)
{
  pthread_mutex_init(&acquire_mutex, NULL);
  pthread_mutex_init(&release_mutex, NULL);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r) {

  std::cout << "acquire request (clt " << clt << ", lock id: " << lid << ")\n";
  pthread_mutex_lock(&acquire_mutex);
  
  std::map<lock_protocol::lockid_t, lock>::iterator it;
  it = locks.find(lid);

  if (it == locks.end()) {

    // Init a new lock
    lock new_lock;
    new_lock.lock_state = FREE;
    pthread_cond_init(&new_lock.cond_var, NULL);

    // Add the new lock
    locks[lid] = new_lock;
    it = locks.find(lid);
  }

  while(1) {
    if (it->second.lock_state == FREE) {
      it->second.lock_state = LOCKED;
      nacquire++;
      break;
    }
    pthread_cond_wait(&(it->second.cond_var), &acquire_mutex);
  }

  std::cout << "acquire done (clt " << clt << ", lock id: " << lid << ")\n";
  pthread_mutex_unlock(&acquire_mutex);

  return lock_protocol::OK;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r) {
  
  std::cout << "release request (clt " << clt << ", lock id: " << lid << ")\n";
  pthread_mutex_lock(&release_mutex);

  std::map<lock_protocol::lockid_t,lock>::iterator it;
  it = locks.find(lid);

  if ((it != locks.end()) && (it->second.lock_state == LOCKED)) {
    nacquire--;
    it->second.lock_state = FREE;
    pthread_cond_broadcast(&(it->second.cond_var));
  }
 
  pthread_mutex_unlock(&release_mutex);

  return lock_protocol::OK;
}

