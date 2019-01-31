#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include "lock_client_cache.h"
#include <vector>
#include <list>
#include <random>
#include <unistd.h>



class yfs_client {
  extent_client *ec;
  lock_client_cache *lc;
public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, FBIG };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    unsigned long long inum;
  };

  // TODO: 
  // Do we really need this? 
  class dir_content
  {
  public:
    friend std::istream &operator>>(std::istream &is, yfs_client::dir_content &obj);
    friend std::ostream &operator<<(std::ostream &os, yfs_client::dir_content &obj);
    std::list<dirent> entries;
  };

  class ScopedRemoteLock {
  public:
    ScopedRemoteLock(lock_client_cache *lc, inum lock_id): lc_(lc), lock_id_(lock_id) {
      std::cout << "acquire " << lock_id_ << std::endl;
      lc_->acquire(lock_id_);
    }
    ~ScopedRemoteLock() {
      std::cout << "release " << lock_id_ << std::endl;
      lc_->release(lock_id_);
    }
  private:
    lock_client_cache *lc_;
    inum lock_id_;
  };

private:
  static std::string filename(inum);
  static inum n2i(std::string);

    /* Seed */
  std::random_device rd;

    /* Random number generator */
  std::default_random_engine generator;

  /* Distribution on which to apply the generator */
  std::uniform_int_distribution<long long unsigned> distribution;



public:

  yfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);
  inum ilookup(inum di, std::string name);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  // TODO 
  inum generate_new_inum(int);
  status create(inum, const char *, int, inum &);
  status readdir(inum, dir_content &);
  status lookup(inum, const char *, inum &);

  status read(inum, size_t*, off_t, char**);
  status write(inum, size_t, off_t, const char*);
  status set_size(inum, size_t);

  status unlink(inum, const char *);

};

#endif 
