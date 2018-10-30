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
  pthread_mutex_init(&insert_new_lock_mp,NULL);
  // pthread_mutex_init(&mp,NULL);
  // pthread_cond_init(&cv,NULL);
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
	std::cout << "acquire mutex start\n";
  std::cout << "acquire mutex locked\n";
  std::cout << "acquire request from clt " << clt << ", lock id: " << lid << "\n";
  
  std::map<lock_protocol::lockid_t,lock_t>::iterator it;
  
  pthread_mutex_lock(&insert_new_lock_mp);
  nacquire += 1;
  std::cout << "nacquire: " << nacquire << "\n";

  it = locks.find(lid);
	if (it == locks.end()) {
  	lock_t lock;
    lock.lock = FREE;
  	pthread_mutex_init(&(lock.mp), NULL);
		pthread_cond_init(&(lock.cv), NULL);
    
    locks[lid] = lock;
    it = locks.find(lid);
	}

  pthread_mutex_unlock(&insert_new_lock_mp);

  pthread_mutex_lock(&(it->second.mp));

  while(1) {
    if (it->second.lock == FREE) {
      // nacquire = nacquire + 1;

      it->second.lock = LOCKED;
      break;
    }
    std::cout << "asleep\n";
    pthread_cond_wait(&(it->second.cv), &(it->second.mp));
    std::cout << "awake\n";
  }
   
  pthread_mutex_unlock(&(it->second.mp));
  std::cout << "acquire mutex end\n";

  return lock_protocol::OK;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  std::map<lock_protocol::lockid_t,lock_t>::iterator it;
  it = locks.find(lid);
  pthread_mutex_lock(&(it->second.mp));
	// std::cout << "0: release request from clt " << clt << ", lock id: " << lid << "\n";
  
  // std::cout << "1: release request from clt " << clt << ", lock id: " << lid << "\n";
   
  nacquire -= 1;
  std::cout << "released: " << nacquire << "\n";
  if ((it != locks.end()) && (it->second.lock == LOCKED)) {
  
    // nacquire = nacquire - 1;
    it->second.lock = FREE;
    // std::cout << "2: release request from clt " << clt << ", lock id: " << lid << "\n";
    pthread_cond_broadcast(&(it->second.cv));
    // std::cout << "3: release request from clt " << clt << ", lock id: " << lid << "\n";
  }

  // std::cout << "4: release request from clt " << clt << ", lock id: " << lid << "\n";
  pthread_mutex_unlock(&(it->second.mp));
  // std::cout << "5: release request from clt " << clt << ", lock id: " << lid << "\n";

  return lock_protocol::OK;
}

