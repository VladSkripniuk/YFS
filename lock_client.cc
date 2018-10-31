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
  printf("lock_client::stat\n");
  int r;
  int ret = cl->call(lock_protocol::stat, cl->id(), lid, r);
  assert (ret == lock_protocol::OK);
  return r;
}

lock_protocol::status
lock_client::acquire(lock_protocol::lockid_t lid)
{
  int r;
  int ret = cl->call(lock_protocol::acquire, cl->id(), lid, r);
  std::cout << "acquire res: " << ret << "\n";
  std::cout << lock_protocol::OK << std::endl;
  std::cout << lock_protocol::RETRY << std::endl;
  std::cout << lock_protocol::RPCERR << std::endl;
  std::cout << lock_protocol::NOENT << std::endl;
  std::cout << lock_protocol::IOERR << std::endl;
  assert (ret == lock_protocol::OK);
  return r;
  // return lock_protocol::RPCERR;
}

lock_protocol::status
lock_client::release(lock_protocol::lockid_t lid)
{
  int r;
  int ret = cl->call(lock_protocol::release, cl->id(), lid, r);
  std::cout << "acquire res: " << ret << "\n";
  assert (ret == lock_protocol::OK);
  return r;
  // return lock_protocol::RPCERR;
}

