// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include "extent_protocol.h"
#include "rpc.h"

class extent_client {
 private:
  rpcc *cl;

  struct attr_buf_cached {
  	extent_protocol::attr attr;
  	std::string buf;
  	int is_dirty;
  	int is_deleted;

  	attr_buf_cached() {
  	  buf = "";
  	  attr.size = 0;
  	  is_dirty = 0;
  	  is_deleted = 0;

  	  time_t t = time(NULL);
  	  
  	  attr.atime = t;
  	  attr.mtime = t;
  	  attr.ctime = t;
  	}
  };

  pthread_mutex_t buffers_mutex;
  std::map<extent_protocol::extentid_t, attr_buf_cached> buffers;

 public:
  extent_client(std::string dst);

  extent_protocol::status get(extent_protocol::extentid_t eid, std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(extent_protocol::extentid_t eid);
  extent_protocol::status flush(extent_protocol::extentid_t eid);
};

#endif 

