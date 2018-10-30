// RPC stubs for clients to talk to lock_server

#include "lock_client.h"
#include "rpc.h"
#include <arpa/inet.h>

#include <sstream>
#include <iostream>
#include <stdio.h>

lock_client::lock_client(std::string dst)
{
    printf("lock_client::lock_client\n");
    sockaddr_in dstsock;
    make_sockaddr(dst.c_str(), &dstsock);
    cl = new rpcc(dstsock);
    if (cl->bind() < 0) {
        printf("lock_client: call bind\n");
    }
}

int
lock_client::stat(lock_protocol::lockid_t lid)
{
    std::cout << "stat start\n";
    printf("lock_client::stat\n");
    int r;
    int ret = cl->call(lock_protocol::stat, cl->id(), lid, r);
    assert (ret == lock_protocol::OK);
    std::cout << "stat end\n";
    return r;
}

lock_protocol::status
lock_client::acquire(lock_protocol::lockid_t lid)
{
  // std::cout << "acquire start\n";
  int r;
  int ret = cl->call(lock_protocol::acquire, cl->id(), lid, r);
  assert (ret == lock_protocol::OK);
  // std::cout << "acquire end\n";
  std::cout << "acquire res: " << r << "\n";
  return r;
  // return lock_protocol::RPCERR;
}

lock_protocol::status
lock_client::release(lock_protocol::lockid_t lid)
{
  // std::cout << "release start\n";
  int r;
  int ret = cl->call(lock_protocol::release, cl->id(), lid, r);
  assert (ret == lock_protocol::OK);
  // std::cout << "release end\n";
  return r;
  // return lock_protocol::RPCERR;
}

