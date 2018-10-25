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
  pthread_mutex_init(&mp,NULL);
  pthread_cond_init(&cv,NULL);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  pthread_mutex_lock(&mp);
  std::cout << "acquire request from clt " << clt << ", lock id: " << lid << "\n";
  
  std::map<lock_protocol::lockid_t,int>::iterator it;
  it = locks.find(lid);

  while(1) {
    if ((it->second == FREE) || (it == locks.end())) {
      nacquire = nacquire + 1;
      locks[lid] = LOCKED;
      break;
    }
    pthread_cond_wait(&cv, &mp);
  }
   
  pthread_mutex_unlock(&mp);

  return lock_protocol::OK;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  pthread_mutex_lock(&mp);
  std::cout << "0: release request from clt " << clt << ", lock id: " << lid << "\n";
  
  std::map<lock_protocol::lockid_t,int>::iterator it;
  it = locks.find(lid);

  std::cout << "1: release request from clt " << clt << ", lock id: " << lid << "\n";
   
  if ((it != locks.end()) && (it->second == LOCKED)) {
  
    nacquire = nacquire - 1;
    locks[lid] = FREE;
    std::cout << "2: release request from clt " << clt << ", lock id: " << lid << "\n";
    pthread_cond_broadcast(&cv);
    std::cout << "3: release request from clt " << clt << ", lock id: " << lid << "\n";
  }

  std::cout << "4: release request from clt " << clt << ", lock id: " << lid << "\n";
  pthread_mutex_unlock(&mp);
  std::cout << "5: release request from clt " << clt << ", lock id: " << lid << "\n";

  return lock_protocol::OK;
}

