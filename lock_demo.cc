//
// Lock demo
//

#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"
#include <arpa/inet.h>
#include <vector>
#include <stdlib.h>
#include <stdio.h>

std::string dst;
lock_client *lc1;
lock_client *lc2;
lock_client *lc3;

lock_protocol::lockid_t a = 1;
lock_protocol::lockid_t b = 2;
lock_protocol::lockid_t c = 3;

void *
do_smth_lc1(void *lid) 
{
  int id = * (int *) lid;
  
  std::cout << "client lc1 acquire : " << id << "\n";
  lc1->acquire(id);
  std::cout << "client lc1 acquire done\n"; 

  sleep(5);

  std::cout << "client lc1 release : " << id << "\n";
  lc1->release(id);
  std::cout << "client lc1 release done\n"; 

  return 0;
}

void *
do_smth_lc2(void *lid) 
{
  int id = * (int *) lid;
  
  std::cout << "client lc2 acquire : " << id << "\n";
  lc2->acquire(id);
  std::cout << "client lc2 acquire done\n"; 

  sleep(5);

  std::cout << "client lc2 release : " << id << "\n";
  lc2->release(id);
  std::cout << "client lc2 release done\n"; 

  return 0;
}

void *
do_smth_lc3(void *lid) 
{
  int id = * (int *) lid;
  
  std::cout << "client lc3 acquire : " << id << "\n";
  lc3->acquire(id);
  std::cout << "client lc3 acquire done\n"; 

  sleep(5);

  std::cout << "client lc3 release : " << id << "\n";
  lc3->release(id);
  std::cout << "client lc3 release done\n"; 

  return 0;
}

int
main(int argc, char *argv[])
{
  if(argc != 2){
    fprintf(stderr, "Usage: %s [host:]port\n", argv[0]);
    exit(1);
  }

   pthread_t th1;
   pthread_t th2;
   pthread_t th3;

  dst = argv[1];
  lc1 = new lock_client(dst);
  lc2 = new lock_client(dst);
  lc3 = new lock_client(dst);
 
  pthread_create(&th1, NULL, do_smth_lc1, (void *) new int (1));
  pthread_create(&th2, NULL, do_smth_lc2, (void *) new int (1));
  pthread_create(&th3, NULL, do_smth_lc3, (void *) new int (2));

  pthread_join(th1, NULL);
  pthread_join(th2, NULL);
  pthread_join(th3, NULL);

}
