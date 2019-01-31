// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>


static void *
releasethread(void *x)
{
    lock_client_cache *cc = (lock_client_cache *) x;
    cc->releaser();
    return 0;
}

int lock_client_cache::last_port = 0;


lock_client_cache::lock_client_cache(std::string xdst,
                                     class lock_release_user *_lu)
// : lock_client(xdst), lu(_lu), last_seqnum(0)
: rsmcl(new rsm_client(xdst)), lu(_lu), last_seqnum(0)
{
    srand(time(NULL)^last_port);
    rlock_port = ((rand()%32000) | (0x1 << 10));
    const char *hname;
    // assert(gethostname(hname, 100) == 0);
    hname = "127.0.0.1";
    std::ostringstream host;
    host << hname << ":" << rlock_port;
    client_socket = host.str();
    last_port = rlock_port;

    rpcs *rlsrpc = new rpcs(rlock_port);
    /* register RPC handlers with rlsrpc */
    rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::accept_revoke_request);
    rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::accept_retry_request);
    
    pthread_t th;
    int r = pthread_create(&th, NULL, &releasethread, (void *) this);
    assert (r == 0);
}


struct delay_thread_struct {
    lock_client_cache *cc;
    lock_protocol::lockid_t lid;
    //we can also put a sequence number here
};

static void *
delaythread(void *x) {
    //wait here for 0.1 seconds
    usleep(10000 + (rand() % 10000));
    
    delay_thread_struct *ptr = (delay_thread_struct *) x;
    ptr->cc->release_to_lock_server(ptr->lid);
    delete ptr;
    return 0;
}

// This method should be a continuous loop, waiting to be notified of
// freed locks that have been revoked by the server, so that it can
// send a release RPC.
void
lock_client_cache::releaser() {
    while (1) {
        std::pair<lock_protocol::lockid_t, rlock_protocol::seqnum_t> release_request = releaser_queue.pop_front();
        std::cout << "lock_client_cache::releaser;"
                  << " lock id: " << release_request.first
                  << " seq.num: " << release_request.second << std::endl;
        
        auto lock = cached_locks.find(release_request.first);
        if (lock == cached_locks.end()) {
            throw std::runtime_error("There is no lock. ");
        }
        
        if (lock->second.seqnum != release_request.second) {
            throw std::runtime_error("Inconsistent seq.num in releaser. ");
        }
        
        delay_thread_struct *t = new delay_thread_struct();
        t->cc = this;
        t->lid = release_request.first;
        
        pthread_t th;
        int r = pthread_create(&th, NULL, &delaythread, (void *) t);
        assert (r == 0);
        
    }
    
}


lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid) {
    lock_protocol::status ret;
    
    // If this is the first contact with the server,
    // I need to subscribe for async rpc responses
    if (last_seqnum == 0) {
        int r;
        // ret = cl->call(lock_protocol::subscribe, cl->id(), client_socket, r);
        ret = rsmcl->call(lock_protocol::subscribe, client_socket, r);
        if (ret != lock_protocol::OK) {
            return ret;
        }
    }
    
    pthread_mutex_lock(&release_acquire_mutex);
    
    auto lock = cached_locks.find(lid);
    if (lock == cached_locks.end()) {
        cached_lock lock_;
        lock_.lock_state = cached_lock::NONE;
        cached_locks[lid] = lock_;
        lock = cached_locks.find(lid);
    }
    
    while(1) {
        if (lock->second.lock_state == cached_lock::ACQUIRING
            || lock->second.lock_state == cached_lock::LOCKED
            || lock->second.lock_state == cached_lock::RELEASING) {
            pthread_cond_wait(&(lock->second.cond_var), &release_acquire_mutex);
            continue;
        } else if (lock->second.lock_state == cached_lock::NONE) {
            lock->second.lock_state = cached_lock::ACQUIRING;
            while (1) {
                int r;
                lock->second.seqnum = ++last_seqnum;
                
                pthread_mutex_unlock(&release_acquire_mutex);
                // ret = cl->call(lock_protocol::acquire, cl->id(), client_socket, lock->second.seqnum, lid, r);
                ret = rsmcl->call(lock_protocol::acquire, client_socket, lock->second.seqnum, lid, r);
                pthread_mutex_lock(&release_acquire_mutex);
                
                if (r == lock_protocol::OK) {
                    lock->second.lock_state = cached_lock::LOCKED;
                    n_successes++;
                    std::cout << client_socket << " lock_client_cache::acquire: acquired " << lid << std::endl;
                    std::cout << "N_SUCCESSES " << n_successes << std::endl;
                    break;
                } else {
                    n_failures++;
                    std::cout << client_socket << " lock_client_cache::acquire: retry later " << lid << std::endl;
                    std::cout << "N_FAILURES " << n_failures << std::endl;

                    while (1) {
                        auto lockid_and_seqnum = lockid_to_retry_seqnum_map.find(lid);
                        std::cout << "Waiting for the retry request: "
                                  << lockid_and_seqnum->second << " < " << lock->second.seqnum << std::endl;
                        if (lockid_and_seqnum != lockid_to_retry_seqnum_map.end()
                            && lockid_and_seqnum->second == lock->second.seqnum) {
                            break;
                        }
                        pthread_cond_wait(&(lock->second.cond_var), &release_acquire_mutex);
                    }
                }
            }
        } else if (lock->second.lock_state == cached_lock::FREE) {
            lock->second.lock_state = cached_lock::LOCKED;
            std::cout << client_socket << " lock_client_cache::acquire: acquired from cache " << lid << std::endl;
        } else {
            throw "Something is wrong with [it->second.lock_state]!";
        }
        break; 
    }
    pthread_mutex_unlock(&release_acquire_mutex);
    return lock_protocol::OK;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid) {
    
    pthread_mutex_lock(&release_acquire_mutex);
    
    std::cout << client_socket <<  " lock_client_cache::release: released to cache " << lid << std::endl;
    
    cached_locks[lid].lock_state = cached_lock::FREE;
    pthread_cond_broadcast(&cached_locks[lid].cond_var);
    
    pthread_mutex_unlock(&release_acquire_mutex);
    
    return lock_protocol::OK;
}

rlock_protocol::status
lock_client_cache::accept_retry_request(rlock_protocol::seqnum_t seqnum, lock_protocol::lockid_t lid, int &r) {
    
    // without this mutex we signal before acquiring thread goes to sleep
    pthread_mutex_lock(&release_acquire_mutex);
    std::cout << client_socket << " can retry now: lock id: " << lid << ", seq.num: " << seqnum << std::endl;
    lockid_to_retry_seqnum_map[lid] = seqnum;
    
    pthread_cond_broadcast(&cached_locks[lid].cond_var);
    pthread_mutex_unlock(&release_acquire_mutex);
    return rlock_protocol::OK;
}

rlock_protocol::status
lock_client_cache::accept_revoke_request(rlock_protocol::seqnum_t seqnum, lock_protocol::lockid_t lid, int &r) {
    std::cout << "lock_client_cache::accept_revoke_request; lock in: " << lid << ", seq.num: " << seqnum << std::endl;
    releaser_queue.push_back(std::pair<lock_protocol::lockid_t,rlock_protocol::seqnum_t>(lid, seqnum));
    return rlock_protocol::OK;
}

rlock_protocol::status
lock_client_cache::release_to_lock_server(lock_protocol::lockid_t lid) {
    pthread_mutex_lock(&release_acquire_mutex);
    std::cout << client_socket << "lock_client_cache::release_to_lock_server" << std::endl;
    
    auto lock = cached_locks.find(lid);
    if (lock == cached_locks.end()) {
        throw std::runtime_error("Lock client cache tryes to release lock that it doesn't hold. ");
    }
    
    
    while(1) {
        if (lock->second.lock_state == cached_lock::ACQUIRING
            || lock->second.lock_state == cached_lock::LOCKED) {
            pthread_cond_wait(&(lock->second.cond_var), &release_acquire_mutex);
            continue;
        } else if (lock->second.lock_state == cached_lock::FREE) {
            std::cout << client_socket << "lock_client_cache::release_to_lock_server::FREE" << std::endl;
            lock->second.lock_state = cached_lock::RELEASING;
            
            pthread_mutex_unlock(&release_acquire_mutex);
            
            // flush to extent
            std::cout << "lu->dorelease(lid): lu = " << lu << std::endl;
            if (lu) {
              lu->dorelease(lid);
            }
            
            // TODO: useless?
            // usleep(100000);
            
            int r;
            auto ret = rsmcl->call(lock_protocol::release, client_socket, lock->second.seqnum, lid, r);
            if (ret != lock_protocol::OK) {
                throw std::runtime_error("Some problem with RPC in release_to_lock_server. ");
            }
            
            pthread_mutex_lock(&release_acquire_mutex);
            
            std::cout << client_socket << " lock_client_cache::release_to_lock_server: released seqnum "
                    << lock->second.seqnum << " lid " << lid << std::endl;
            lock->second.lock_state = cached_lock::NONE;
            pthread_cond_broadcast(&cached_locks[lid].cond_var);
            
        } else if (lock->second.lock_state == cached_lock::NONE
                   || lock->second.lock_state == cached_lock::RELEASING) {
            
            // TODO: ??
            // do nothing, it was already released, might cause problems if reordering
        } else {
            throw std::runtime_error("Something is wrong with [it->second.lock_state]!");
        }
        break;
    }
    pthread_mutex_unlock(&release_acquire_mutex);
    return lock_protocol::OK;
}
