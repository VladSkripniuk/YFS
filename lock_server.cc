// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
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
	printf("acquire request from clt %d\n", clt);

	std::map<lock_protocol::lockid_t,int>::iterator it;
	it = locks.find(lid);

	if ((it->second == FREE) || (it == locks.end())) {
		nacquire = nacquire + 1;
		locks[lid] = LOCKED;
	}

  r = nacquire;
  lock_protocol::status ret = lock_protocol::OK;
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
	printf("release request from clt %d\n", clt);

	std::map<lock_protocol::lockid_t,int>::iterator it;
	it = locks.find(lid);
	
	if ((it != locks.end()) && (it->second == LOCKED)) {
		nacquire = nacquire - 1;
		locks[lid] = FREE;
	}

  r = nacquire;
  lock_protocol::status ret = lock_protocol::OK;
  return ret;
}

