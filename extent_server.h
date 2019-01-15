// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include <ctime>
#include "extent_protocol.h"

class extent_server {
 private:

  struct attr_buf {
  	extent_protocol::attr attr;
  	std::string buf;

  	attr_buf() {
  	  buf = "";
  	  attr.size = 0;

  	  time_t t = time(NULL);
  	  
  	  attr.atime = t;
  	  attr.mtime = t;
  	  attr.ctime = t;
  	}
  };

  pthread_mutex_t buffers_mutex;
  std::map<extent_protocol::extentid_t, attr_buf> buffers;

 public:
  extent_server();

  int put(extent_protocol::extentid_t id, std::string, int &);
  int get(extent_protocol::extentid_t id, std::string &);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int remove(extent_protocol::extentid_t id, int &);
};

#endif 







