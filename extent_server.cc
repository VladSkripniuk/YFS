// the extent server implementation

#include "extent_server.h"
#include "rpc/slock.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctime>

extent_server::extent_server() {
  int t;
  this->put(1, std::string(""), t);
}


int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  ScopedLock scoped_lock(&buffers_mutex);
  std::cout << "put " << "<" <<id << "><" << buf << ">" << std::endl;
  auto it = buffers.find(id);
    
    // Add empty buf
  if (it == buffers.end()) {
    attr_buf new_buffer;
    buffers[id] = new_buffer;
    it = buffers.find(id);
  }

    // Fill in
  it->second.buf = buf;
  it->second.attr.size = buf.length();

  time_t t = time(NULL);

  it->second.attr.ctime = t;
  it->second.attr.mtime = t;

  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
    ScopedLock scoped_lock(&buffers_mutex);
    std::cout << "get " << id << std::endl;
    auto it = buffers.find(id);
    if (it == buffers.end()) {
        std::cout << "get::NOENT " << id << std::endl;
        return extent_protocol::NOENT;
    }
    
    buf = it->second.buf;
    time_t t = time(NULL);
    it->second.attr.atime = t;
    
    std::cout << "get::OK " << id << std::endl;
    return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  ScopedLock scoped_lock(&buffers_mutex);
  auto it = buffers.find(id);
  if (it == buffers.end()) {
    return extent_protocol::NOENT;
  }

  a = it->second.attr;
  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  ScopedLock scoped_lock(&buffers_mutex);
  auto it = buffers.find(id);
  if (it == buffers.end()) {
    return extent_protocol::NOENT;
  }

  buffers.erase(it);

  return extent_protocol::OK;

  // return extent_protocol::IOERR;
}

