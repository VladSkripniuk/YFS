// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

// The calls assume that the caller holds a lock on the extent

extent_client::extent_client(std::string dst) {
    std::cout << "extent_client::extent_client" << std::endl;
    sockaddr_in dstsock;
    make_sockaddr(dst.c_str(), &dstsock);
    cl = new rpcc(dstsock);
    if (cl->bind() != 0) {
        printf("extent_client: bind failed\n");
    }
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf) {
    std::cout << "extent_client::get::start" << std::endl;
    extent_protocol::status ret = extent_protocol::OK;
    
    std::map<extent_protocol::extentid_t, attribute_cached>::iterator it;
    pthread_mutex_lock(&buffers_mutex);
    it = buffers.find(eid);
    pthread_mutex_unlock(&buffers_mutex);
    
    if (it == buffers.end()) {
        attribute_cached temp;
        
        ret = cl->call(extent_protocol::getattr, eid, temp.attr);
        if (ret != extent_protocol::OK) {
            std::cout << "extent_client::get::reterr1" << ret << std::endl;
            return ret;
        }
        
        ret = cl->call(extent_protocol::get, eid, temp.buf);
        std::cout << "extent_client::get::t.buf " << temp.buf << std::endl;
        if (ret != extent_protocol::OK) {
            std::cout << "extent_client::get::reterr2" << ret << std::endl;
            return ret;
        }
        
        pthread_mutex_lock(&buffers_mutex);
        buffers[eid] = temp;
        it = buffers.find(eid);
        pthread_mutex_unlock(&buffers_mutex);
    }
    
    if (it->second.is_deleted) {
        return extent_protocol::NOENT;
    }
    
    pthread_mutex_lock(&buffers_mutex);
    buf = it->second.buf;
    std::cout << "extent_client::get::it->second.buf " << it->second.buf << std::endl;
    time_t t = time(NULL);
    it->second.attr.atime = t;
    ///// Make sure that if you only read an extent
    // (or its attributes) that you don't flush it back on a release.
    // it->second.is_dirty = 1;
    pthread_mutex_unlock(&buffers_mutex);
    
    std::cout << "extent_client::get::end" << std::endl;
    return extent_protocol::OK;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, extent_protocol::attr &attr) {
    std::cout << "extent_client::getattr" << std::endl;
    extent_protocol::status ret = extent_protocol::OK;
    
    std::map<extent_protocol::extentid_t, attribute_cached>::iterator it;
    pthread_mutex_lock(&buffers_mutex);
    it = buffers.find(eid);
    pthread_mutex_unlock(&buffers_mutex);
    
    if (it == buffers.end()) {
        attribute_cached temp;
        
        ret = cl->call(extent_protocol::getattr, eid, temp.attr);
        if (ret != extent_protocol::OK) {
            return ret;
        }
        
        ret = cl->call(extent_protocol::get, eid, temp.buf);
        std::cout << "extent_client::getattr::t.buf " << temp.buf << std::endl;
        if (ret != extent_protocol::OK) {
            return ret;
        }
        
        pthread_mutex_lock(&buffers_mutex);
        buffers[eid] = temp;
        
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
extent_client::put(extent_protocol::extentid_t eid, std::string buf) {
    std::cout << "extent_client::put" << std::endl;
    extent_protocol::status ret = extent_protocol::OK;
    
    std::map<extent_protocol::extentid_t, attribute_cached>::iterator it;
    pthread_mutex_lock(&buffers_mutex);
    it = buffers.find(eid);
    pthread_mutex_unlock(&buffers_mutex);
    
    if (it == buffers.end()) {
        attribute_cached temp;
        
        ret = cl->call(extent_protocol::getattr, eid, temp.attr);
        ret = cl->call(extent_protocol::get, eid, temp.buf);
        std::cout << "extent_client::put::t.buf " << temp.buf << std::endl;
        
        if (ret != extent_protocol::OK) {
            //newly created
            //everything is done in constructor
        }
        
        pthread_mutex_lock(&buffers_mutex);
        buffers[eid] = temp;
        it = buffers.find(eid);
        pthread_mutex_unlock(&buffers_mutex);
    }
    
    pthread_mutex_lock(&buffers_mutex);
    it->second.buf = buf;
    std::cout << "extent_client::put::it->second.buf " << it->second.buf << std::endl;
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
    std::cout << "extent_client::remove" << std::endl;
    extent_protocol::status ret = extent_protocol::OK;
    
    std::map<extent_protocol::extentid_t, attribute_cached>::iterator it;
    pthread_mutex_lock(&buffers_mutex);
    it = buffers.find(eid);
    pthread_mutex_unlock(&buffers_mutex);
    
    if (it == buffers.end()) {
        attribute_cached temp;
        
        ret = cl->call(extent_protocol::getattr, eid, temp.attr);
        if (ret != extent_protocol::OK) {
            return ret;
        }
        ret = cl->call(extent_protocol::get, eid, temp.buf);
        std::cout << "extent_client::remove::t.buf " << temp.buf << "<" << std::endl;
        if (ret != extent_protocol::OK) {
            return ret;
        }
        
        pthread_mutex_lock(&buffers_mutex);
        buffers[eid] = temp;
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
    std::cout << "extent_client::flush" << std::endl;
    std::map<extent_protocol::extentid_t, attribute_cached>::iterator it;
    pthread_mutex_lock(&buffers_mutex);
    it = buffers.find(eid);
    pthread_mutex_unlock(&buffers_mutex);
    
    if (it == buffers.end()) {
        return extent_protocol::OK;
    }
    if (it->second.is_deleted) {
        std::cout << "extent_client::flush::it->second.is_deleted" << std::endl;
        int r;
        int ret;
        ret = cl->call(extent_protocol::remove, eid, r);
    }
    if (it->second.is_dirty) {
        std::cout << "extent_client::flush::it->second.is_dirty <"
            << eid << "> " << it->second.buf << " " << std::endl;
        int r;
        int ret;
        ret = cl->call(extent_protocol::put, eid, it->second.buf, r);
    }
    
    pthread_mutex_lock(&buffers_mutex);
    buffers.erase(it);
    pthread_mutex_unlock(&buffers_mutex);
    
    std::cout << "extent_client::flush::end" << std::endl;
    
    return extent_protocol::OK;
}
