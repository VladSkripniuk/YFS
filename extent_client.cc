// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

// The calls assume that the caller holds a lock on the extent

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
	make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;

  std::map<extent_protocol::extentid_t, attr_buf_cached>::iterator it;
  pthread_mutex_lock(&buffers_mutex);
  it = buffers.find(eid);
  pthread_mutex_unlock(&buffers_mutex);

  if (it == buffers.end()) {
    attr_buf_cached t;

    ret = cl->call(extent_protocol::getattr, eid, t.attr);
    if (ret != extent_protocol::OK) {
      return ret;
    }

    ret = cl->call(extent_protocol::get, eid, t.buf);
    if (ret != extent_protocol::OK) {
      return ret;
    }

    pthread_mutex_lock(&buffers_mutex);
    buffers[eid] = t;
    it = buffers.find(eid);    
    pthread_mutex_unlock(&buffers_mutex);
  }

  if (it->second.is_deleted) {
    return extent_protocol::NOENT;
  }
  
  pthread_mutex_lock(&buffers_mutex);
  buf = it->second.buf;

  time_t t = time(NULL);
  it->second.attr.atime = t;
  ///// Make sure that if you only read an extent (or its attributes) that you don't flush it back on a release. 
  // it->second.is_dirty = 1;
  pthread_mutex_unlock(&buffers_mutex);

  return extent_protocol::OK;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;

  std::map<extent_protocol::extentid_t, attr_buf_cached>::iterator it;
  pthread_mutex_lock(&buffers_mutex);
  it = buffers.find(eid);
  pthread_mutex_unlock(&buffers_mutex);

  if (it == buffers.end()) {
    attr_buf_cached t;

    ret = cl->call(extent_protocol::getattr, eid, t.attr);
    if (ret != extent_protocol::OK) {
      return ret;
    }

    ret = cl->call(extent_protocol::get, eid, t.buf);
    if (ret != extent_protocol::OK) {
      return ret;
    }

    pthread_mutex_lock(&buffers_mutex);
    buffers[eid] = t;

    it = buffers.find(eid);    
    pthread_mutex_unlock(&buffers_mutex);
  }

  if (it->second.is_deleted) {
    return extent_protocol::NOENT;
  }

  attr = it->second.attr;

  return extent_protocol::OK;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;

  std::map<extent_protocol::extentid_t, attr_buf_cached>::iterator it;
  pthread_mutex_lock(&buffers_mutex);
  it = buffers.find(eid);
  pthread_mutex_unlock(&buffers_mutex);

  if (it == buffers.end()) {
    attr_buf_cached t;

    ret = cl->call(extent_protocol::getattr, eid, t.attr);
    ret = cl->call(extent_protocol::get, eid, t.buf);
    if (ret != extent_protocol::OK) {
      //newly created
      //everything is done in constructor
    }

    pthread_mutex_lock(&buffers_mutex);
    buffers[eid] = t;

    it = buffers.find(eid);
    pthread_mutex_unlock(&buffers_mutex);
  }


  pthread_mutex_lock(&buffers_mutex);
  it->second.buf = buf;
  it->second.attr.size = buf.length();

  time_t t = time(NULL);

  it->second.attr.ctime = t;
  it->second.attr.mtime = t;

  it->second.is_dirty = 1;
  it->second.is_deleted = 0;

  pthread_mutex_unlock(&buffers_mutex);

  return extent_protocol::OK;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;

  std::map<extent_protocol::extentid_t, attr_buf_cached>::iterator it;
  pthread_mutex_lock(&buffers_mutex);
  it = buffers.find(eid);
  pthread_mutex_unlock(&buffers_mutex);

  if (it == buffers.end()) {
    attr_buf_cached t;

    ret = cl->call(extent_protocol::getattr, eid, t.attr);
    if (ret != extent_protocol::OK) {
      return ret;
    }
    ret = cl->call(extent_protocol::get, eid, t.buf);
    if (ret != extent_protocol::OK) {
      return ret;
    }

    pthread_mutex_lock(&buffers_mutex);
    buffers[eid] = t;
    it = buffers.find(eid);
    pthread_mutex_unlock(&buffers_mutex);
  }
  
  pthread_mutex_lock(&buffers_mutex);
  it->second.is_deleted = 1;
  pthread_mutex_unlock(&buffers_mutex);

  return extent_protocol::OK;
}

extent_protocol::status
extent_client::flush(extent_protocol::extentid_t eid)
{
  std::map<extent_protocol::extentid_t, attr_buf_cached>::iterator it;
  pthread_mutex_lock(&buffers_mutex);
  it = buffers.find(eid);
  pthread_mutex_unlock(&buffers_mutex);

  if (it->second.is_deleted) {
    int r;
    int ret;
    ret = cl->call(extent_protocol::remove, eid, r);
  }

  if (it->second.is_dirty) {
    int r;
    int ret;
    ret = cl->call(extent_protocol::put, eid, it->second.buf, r);
  }

  return extent_protocol::OK;
}