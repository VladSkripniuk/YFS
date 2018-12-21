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


yfs_client::yfs_client(std::string extent_dst, std::string lock_dst) : generator(rd()), distribution(0,0xFFFFFFFFFFFFFFFF) {
  ec = new extent_client(extent_dst);

  lock_release_user_derived *ptr = new lock_release_user_derived(ec);

  lc = new lock_client_cache(lock_dst, ptr);
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
  std::cout << "yfs_client::getfile " << std::endl;

  ScopedRemoteLock scoped_lock(lc, inum);

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
  std::cout << "yfs_client::getdir " << std::endl;

  ScopedRemoteLock scoped_lock(lc, inum);

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

yfs_client::inum yfs_client::generate_new_inum(int is_dir) {
  // return inum; // ha ha TODO
  // TODO: generate inum not randomly

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
  obj.entries.clear();
  int number_of_entries;
  if (is.rdbuf()->in_avail() == 0) {
    number_of_entries = 0;
  }
  else {
    is >> number_of_entries;
  }

  std::string name;
  unsigned long long inum;

  for (int i = 0; i < number_of_entries; ++i) {
    is >> name >> inum;
    yfs_client::dirent entry = { name, inum };
    obj.entries.push_back(entry);
  }

  return is;
}

std::ostream &operator<<(std::ostream &os, yfs_client::dir_content &obj) {
  int number_of_entries = obj.entries.size();
  os << number_of_entries << " ";

  auto it = obj.entries.begin();
  while (it != obj.entries.end()) {
    os << it->name << " " << it->inum << " ";
    ++it;
  }

  return os;
}

yfs_client::status yfs_client::create(inum parent, const char *name, int is_dir, inum &ino) {
  
  std::cout << "yfs_client::create " << std::endl;

  ScopedRemoteLock scoped_lock(lc, parent);

  // Get info 
  std::string parent_dir_content_txt;
  if(ec->get(parent, parent_dir_content_txt) != OK) {
    std::cout << "yfs_client::create -> error [ec->get(..) != OK]\n";
  }

  // Parse info 
  dir_content parent_dir_content;
  std::istringstream ist(parent_dir_content_txt);
  ist >> parent_dir_content;

  // Remove old file with the same name
  for (auto it=parent_dir_content.entries.begin(); it!=parent_dir_content.entries.end(); it++) {
    if (it->name.compare(name) == 0) {
      parent_dir_content.entries.erase(it);
      break;
    }
  }
  
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
  
  ino = inum;
  
  return OK;
}

yfs_client::status yfs_client::readdir(inum parent_dir, dir_content &parent_dir_content) {

  std::cout << "yfs_client::readdir" << std::endl;

  ScopedRemoteLock scoped_lock(lc, parent_dir);

  // Get info 
  std::string parent_dir_content_txt;
  if(ec->get(parent_dir, parent_dir_content_txt) != OK) {
    std::cout << "yfs_client::readdir -> error: [ec->get(..) != OK]\n";
  }
  
  // Parse info 
  std::istringstream ist(parent_dir_content_txt);
  ist >> parent_dir_content;
  
  return OK;
}


yfs_client::status yfs_client::lookup(inum parent_dir, const char *name, inum &ino) {

  std::cout << "yfs_client::lookup "  << parent_dir <<  " *" << std::string(name) <<"*"  << std::endl;


  ScopedRemoteLock scoped_lock(lc, parent_dir);

  // Get dir content
  dir_content parent_dir_content;
  // if (readdir(parent_dir, parent_dir_content) != OK) {
    // std::cout << "yfs_client::readdir -> error: [readdir(..) != OK]\n";
  // }

  // Get info 
  std::string parent_dir_content_txt;
  ec->get(parent_dir, parent_dir_content_txt);
  
  // Parse info 
  std::istringstream ist(parent_dir_content_txt);
  ist >> parent_dir_content;

  // Find entry
  for (auto it = parent_dir_content.entries.begin(); it != parent_dir_content.entries.end(); ++it) {
    if (it->name == name) {
      ino = it->inum;
      return OK;
    }
  }

  // TODO: it shouldn't be "OK"
  return NOENT;
}


yfs_client::status yfs_client::read(inum ino, size_t *size, off_t off, char **buf) {

  std::cout << "yfs_client::read" << std::endl;

  ScopedRemoteLock scoped_lock(lc, ino);

  std::string file_content;
  if(ec->get(ino, file_content) != OK) {
    std::cout << "yfs_client::create -> error [ec->get(..) != OK]\n";
  }

  if (off >= file_content.length()) {
    *size = 0;
    *buf = NULL;
    return OK;
  }

  if (off + *size > file_content.length()) {
    *size = file_content.length() - off;
  }

  *buf = (char *) malloc(*size);

  memcpy(*buf, file_content.data()+off, *size);

  return OK;
}

yfs_client::status yfs_client::write(inum ino, size_t size, off_t off, const char *buf) {

  std::cout << "yfs_client::write" << std::endl;

  ScopedRemoteLock scoped_lock(lc, ino);

  std::string file_content;
  if(ec->get(ino, file_content) != OK) {
    std::cout << "yfs_client::create -> error [ec->get(..) != OK]\n";
  }

  size_t old_size = file_content.length();
  if (old_size < off + size) {
    file_content.resize(off+size, '\0');
  }

  file_content.replace(off, size, buf, size);

  ec->put(ino, file_content);

  return OK;
}  

yfs_client::status yfs_client::set_size(inum ino, size_t size) {

  std::cout << "yfs_client::set_size" << std::endl;

  ScopedRemoteLock scoped_lock(lc, ino);

  std::string file_content;
  if(ec->get(ino, file_content) != OK) {
    std::cout << "yfs_client::create -> error [ec->get(..) != OK]\n";
  }

  file_content.resize(size, '\0');

  ec->put(ino, file_content);

  return OK;
}

yfs_client::status yfs_client::unlink(inum parent_dir, const char *name_cstr) {

  std::cout << "yfs_client::unlink" << std::endl;

  ScopedRemoteLock scoped_lock(lc, parent_dir);

 // Get dir content
  dir_content parent_dir_content;
  // if (readdir(parent, parent_dir_content) != OK) {
    // std::cout << "yfs_client::readdir -> error: [readdir(..) != OK]\n";
  // }

  // Get info 
  std::string parent_dir_content_txt;
  ec->get(parent_dir, parent_dir_content_txt);
  
  // Parse info 
  std::istringstream ist(parent_dir_content_txt);
  ist >> parent_dir_content;


  std::string name(name_cstr);

  // Find entry
  auto it = parent_dir_content.entries.begin();

  for (it = parent_dir_content.entries.begin(); it != parent_dir_content.entries.end(); ++it) {
    if (it->name == name) {
      ec->remove(it->inum);

      std::ostringstream ost;
      parent_dir_content.entries.erase(it);
      ost << parent_dir_content;

      ec->put(parent_dir, ost.str());

      break;
    }
  }

  if (it == parent_dir_content.entries.end()) {
    return NOENT;
  }

  return OK;
}