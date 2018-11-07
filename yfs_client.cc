// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <random>


yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);

}

yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  int r = OK;


  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

 release:

  return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;


  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

 release:
  return r;
}


// ---------------------------------------

yfs_client::inum
yfs_client::generate_new_inum(int is_dir)
{
  // return inum; // ha ha TODO

  inum inum = distribution(generator);

  if (is_dir) {
    inum = (inum & 0x0FFFFFFF);
  }
  else {
    inum = (inum | 0x80000000); 
  }

  return inum;

}

std::istream &operator>>(std::istream &is, yfs_client::dir_content &obj) {
  entries.clear();
  int number_of_entries;
  is >> number_of_entries;

  std::string name;
  unsigned long long inum;

  for (int i = 0; i < number_of_entries; ++i) {
    is >> name >> inum;
    dirent entry = { name, inum };
    entries.push_back(entry);
  }

  return is;
}

std::ostream &operator<<(std::ostream &os, yfs_client::dir_content &obj) {
  int number_of_entries = entries.size();
  os << number_of_entries;

  for (int i = 0; i < number_of_entries; ++i) {
    os << entries.head()->name << entries.head()->inum;
    entries.pop_front();
  }

  return os;

}

yfs_client::status yfs_client::create(inum parent, const char *name, int is_dir)
{
  std::string parent_dir_content_txt;
  ec->get(parent, parent_dir_content_txt);

  std::istringstream ist(parent_dir_content_txt);
  dir_content parent_dir_content;
  ist >> parent_dir_content;

  inum inum = generate_new_inum(is_dir);
  std::ostringstream ost;

  if (is_dir) {
    dir_content empty_dir_content;
    ost << empty_dir_content;
    ec->put(inum, ost.str());
    ost.str(std::string());
  }
  else {
    ec->put(inum, std::string());
  }

  dirent entry = { std::string(name), inum };
  parent_dir_content.entries.push_back(entry);
  
  ost << parent_dir_content;

  ec->put(parent, ost.str());
  
  return OK;
}



