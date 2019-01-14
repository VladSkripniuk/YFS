// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <cstdio>

static void *
revokethread(void *x) {
    lock_server_cache *sc = (lock_server_cache *) x;
    sc->revoker();
    return 0;
}

static void *
retrythread(void *x) {
    lock_server_cache *sc = (lock_server_cache *) x;
    sc->retryer();
    return 0;
}

lock_server_cache::lock_server_cache() {
    pthread_mutex_init(&release_acquire_mutex, NULL);
    
    pthread_t th;
    int r = pthread_create(&th, NULL, &revokethread, (void *) this);
    assert (r == 0);
    r = pthread_create(&th, NULL, &retrythread, (void *) this);
    assert (r == 0);
}

// This method should be a continuous loop, that sends revoke
// messages to lock holders whenever another client wants the
// same lock
void
lock_server_cache::revoker() {
    while (1) {
        lock_protocol::lockid_t lid = revoke_queue.pop_front();
        pthread_mutex_lock(&release_acquire_mutex);
        
        std::cout << "lock_server_cache::revoker: " << locks[lid].owner << " " << lid << std::endl;
        
        auto client = lock_clients.find(locks[lid].owner);
        if (client == lock_clients.end())
            throw std::runtime_error("There is no client to revoke. ");
        
        rpcc *cl;
        cl = client->second.cl;
        
        rlock_protocol::seqnum_t seqnum = locks[lid].seqnum;
        pthread_mutex_unlock(&release_acquire_mutex);
        
        int r;
        auto ret = cl->call(rlock_protocol::revoke, seqnum, lid, r);
        if (ret != rlock_protocol::OK)
            throw std::runtime_error("[rlock_protocol::revoke] != OK. ");
    }
}

// This method should be a continuous loop, waiting for locks
// to be released and then sending retry messages to those who
// are waiting for it.
void
lock_server_cache::retryer() {
    while (1) {
        lock_protocol::lockid_t lid = retry_queue.pop_front();
        pthread_mutex_lock(&release_acquire_mutex);

        
        n_retries++;
        
        std::cout << "lock_server_cache::retryer: " << locks[lid].owner << " " << lid << std::endl;
        std::cout << "RETRIES SENT " << n_retries << std::endl;
        std::cout << "waiting list\n";
        for (auto it = locks[lid].waiting_list.begin(); it != locks[lid].waiting_list.end(); it++) {
            std::cout << "\t" << it->client_id << std::endl;
        }
    
        if (locks[lid].waiting_list.empty()){
            pthread_mutex_unlock(&release_acquire_mutex);
            continue;
        }
        
        auto client_and_seqnum = *(locks[lid].waiting_list.begin());
        locks[lid].waiting_list.pop_front();
        
        auto lock_client = lock_clients.find(client_and_seqnum.client_id);
        if(lock_client == lock_clients.end())
            throw std::runtime_error("There is no such a client in waiting list. ");
    
        pthread_mutex_unlock(&release_acquire_mutex);
        
        rpcc *cl;
        cl = lock_client->second.cl;
        
        int r;
        auto ret = cl->call(rlock_protocol::retry, client_and_seqnum.seqnum, lid, r);
        if(ret != rlock_protocol::OK) {
            throw std::runtime_error("[rlock_protocol::retry] != OK");
        }
        std::cout << "retrier call " << ret << " " << r << std::endl;
        std::cout << "retrier: retry sent to " << client_and_seqnum.client_id << " " << lid << std::endl;
    }
}

lock_protocol::status
lock_server_cache::stat(int clt, lock_protocol::lockid_t lid, int &r) {
    r = nacquire;
    return lock_protocol::OK;
}

lock_protocol::status
lock_server_cache::acquire(int clt, std::string client_socket, lock_protocol::seqnum_t seqnum, lock_protocol::lockid_t lid, int &r) {
    if (lock_clients.find(client_socket) == lock_clients.end()) {
        throw std::runtime_error("Unsubscribed client tries to acquire lock.");
    }
    
    pthread_mutex_lock(&release_acquire_mutex);
    std::cout << "acquire request (client socket: " << client_socket << ", seqnum: " << seqnum << ", lock id: " << lid << std::endl;
    
    std::map<lock_protocol::lockid_t, lock>::iterator it = locks.find(lid);
    
    // add new lock if it didn't exist before
    if (it == locks.end()) {
        lock new_lock;
        locks[lid] = new_lock;
        it = locks.find(lid);
    }

    
    if (it->second.is_free()) {
        auto lock_client = lock_clients.find(client_socket);
        
        lock_client->second.nacquire += 1;
        locks[lid].grant_lock(client_socket, seqnum);
        
        r = lock_protocol::OK;
        std::cout << "acquire request successful: " << client_socket << ", seq.num: " << seqnum << ", lock id: " << lid << std::endl;
        
        /// in case someone else is also waiting on this lock right now, ask client to return this lock
        /// that may cause revoke coming to client before acquire lock_protocol::OK
        /// actually not, because we hold release_acquire_mutex
        if (!locks[lid].waiting_list.empty()) {
            revoke_queue.push_back(lid);
        }
        
    } else {
        locks[lid].add_to_waiting_list(client_socket, seqnum);
        
        revoke_queue.push_back(lid);
        std::cout << "acquire request retry: " << client_socket << " " << lid << std::endl;
        
        r = lock_protocol::RETRY;
        
    }
    
    // std::cout << "acquire done (clt " << clt << ", lock id: " << lid << ")\n";
    pthread_mutex_unlock(&release_acquire_mutex);
    
    return r;
}

lock_protocol::status
lock_server_cache::release(int clt, std::string client_socket, lock_protocol::seqnum_t seqnum, lock_protocol::lockid_t lid, int &r) {

    pthread_mutex_lock(&release_acquire_mutex);
    std::cout << "release request (client socket: " << client_socket << ", seqnum: "<< seqnum << ", lock id: " << lid << ")\n";
    
    std::map<lock_protocol::lockid_t,lock>::iterator it = locks.find(lid);
    
    if (it == locks.end()) {
        throw std::runtime_error("Someone tries to release a lock that nobody holds. ");
    }
    
    if (!it->second.is_free()) {
        nacquire--;
        
        std::map<std::string, lock_client>::iterator it1;
        it1 = lock_clients.find(client_socket);
        if (it1 != lock_clients.end()) {
            it1->second.nacquire -= 1;
        }
        it->second.release_lock();
        retry_queue.push_back(lid);
    }
    
    pthread_mutex_unlock(&release_acquire_mutex);
    
    return lock_protocol::OK;
}

// TODO: delete useless params
lock_protocol::status
lock_server_cache::subscribe(int clt, std::string client_socket, int &r) {
    pthread_mutex_lock(&release_acquire_mutex);
    std::cout << "subscribe request (clt: " << client_socket << ")\n";
    
    if (lock_clients.find(client_socket) != lock_clients.end()) {
        throw std::runtime_error("Client tries to subscribe twice.");
    }
    
    // Add a new lock_client
    lock_client new_lock_client(client_socket);
    lock_clients.insert(std::pair<std::string, lock_client>(client_socket, new_lock_client));
    
    pthread_mutex_unlock(&release_acquire_mutex);
    return lock_protocol::OK;
}
