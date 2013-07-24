/* paratest.c - memory-mapped database tester/toy */
/*
 * Copyright 2011 Howard Chu, Symas Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */
#define _XOPEN_SOURCE 500		/* srandom(), random() */
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include "lmdb.h"

#include <fcntl.h>
MDB_env *env;

typedef struct stepper {
   unsigned long int start;
   unsigned long int nb;
   unsigned long int step; //or modulo for delete ops
} stepper;

void *step1BatchAdd(void *x)
{
    int i = 0, j = 0, rc;
    MDB_dbi dbi;
    MDB_val key, data;
    MDB_txn *txn;
    MDB_stat mst;
    int count;
    int *values;
    char kval[32];
    char sval[64];

    stepper *s = (stepper *) x;

    rc = mdb_txn_begin(env, NULL, 0, &txn);
    rc = mdb_open(txn, NULL, 0, &dbi);

    srandom(time(NULL));

    count = s->nb; //(random()%23841) + 64;
    values = (int *)malloc(count*sizeof(int));

    for(i = 0;i<count;i++) {
      values[i] = 1 + i + s->start; //random()%25024;
    }
   
    key.mv_data = kval;
    data.mv_data = sval;

    printf("[%lu] Adding %d values\n", pthread_self(), count);
    for (i=0;i<count;i++) {
       sprintf(kval, "%lu", values[i]);
       key.mv_size = strlen(kval) + 1;
       sprintf(sval, "%03x %d foo bar", values[i], values[i]);
       data.mv_size = strlen(sval) + 1;
       //printf("[%lu] Add key %s\n", pthread_self(), kval);
       rc = mdb_put(txn, dbi, &key, &data, MDB_NOOVERWRITE);
       if (rc) {
         j++;
       }
    }
    if (j) printf("[%lu] %d duplicates skipped\n", pthread_self(), j);
    rc = mdb_txn_commit(txn);
    rc = mdb_env_stat(env, &mst);
    free(values);
    return 0;
}

void *step1UnbatchedAdd(void *x)
{
    int i = 0, j = 0, rc;
    MDB_dbi dbi;
    MDB_val key, data;
    MDB_txn *txn;
    MDB_stat mst;
    int count;
    int *values;
    char kval[32];
    char sval[64];

    stepper *s = (stepper *) x;
    //printf("STEPPPPPPPPPPPPPPPPP = %ld\n", s->step);
    srandom(time(NULL));

    count = s->nb; //(random()%23841) + 64;
    values = (int *)malloc(count*sizeof(int));

    for(i = 0;i<count;i++) {
      values[i] = 1 + s->start + i; //random()%25024;
    }
   
    key.mv_data = kval;
    data.mv_data = sval;

    printf("[%lu] Adding %d values\n", pthread_self(), count);
    for (i=0;i<count;i++) {  
      txn=NULL;
      rc = mdb_txn_begin(env, NULL, 0, &txn);
      rc = mdb_open(txn, NULL, 0, &dbi);

       sprintf(kval, "%lu", values[i]);
       key.mv_size = strlen(kval) + 1;
       sprintf(sval, "%03x %d foo bar", values[i], values[i]);
       data.mv_size = strlen(sval) + 1;
       //printf("[%lu] Add key %s\n", pthread_self(), kval);
       rc = mdb_put(txn, dbi, &key, &data, MDB_NOOVERWRITE);
       if (rc) {
         j++;
         mdb_txn_abort(txn);
       } else {
         rc = mdb_txn_commit(txn);
       }
       mdb_close(env, dbi);
    }
    if (j) printf("[%lu] %d duplicates skipped\n", pthread_self(), j);
    rc = mdb_env_stat(env, &mst);
    free(values);
    return 0;
}

void *step1BatchDelete(void *x)
{
/*
start : delete values start to start+650
nb    : nt used
step  : we will modulo's step-1
*/
    unsigned long int i = 0; int j = 0, rc;
    MDB_dbi dbi;
    MDB_val key, data;
    MDB_txn *txn;
    MDB_stat mst;
    int count;

    char kval[32];
    char sval[64];
    stepper *s = (stepper *) x;

    key.mv_data = kval;
    data.mv_data = sval;

    j=0;
    for (i= 0; i < 650UL; ++i) {
      if(  ((i + s->start) % ((unsigned long) s->step - 2UL)) != 0) continue;
      if(  ((i + s->start) % 1000UL)    == 0) continue;

      j++;
      txn=NULL;
      rc = mdb_txn_begin(env, NULL, 0, &txn);
      rc = mdb_open(txn, NULL, 0, &dbi);
      sprintf(kval, "%lu", s->start + i);
      key.mv_size = strlen(kval) + 1;

      printf("%luDel[%lu + 650UL] %s\n", i, s->start, key.mv_data);

      rc = mdb_del(txn, dbi, &key, NULL);
      if (rc) {
        j--;
        mdb_txn_abort(txn);
      } else {
        rc = mdb_txn_commit(txn);
      }

      mdb_close(env, dbi);
    }

    printf("[%lu] Deleted %d values\n", pthread_self(), j);

    rc = mdb_env_stat(env, &mst);
    return 0;
}

void *step1BatchDeleteWithCursor(void *x)
{
/*
 start : where to set cursor
 nb    : modulo's of where to step (i.e. 1000)
 step  : modulo's of where to delete
*/
    int rc;
    MDB_dbi dbi;
    MDB_val key, data;
    MDB_txn *txn;
    MDB_cursor *cur2;
    stepper *s = (stepper *) x;
    //return 0;

    char ikey[32];
    key.mv_data = ikey;
    sprintf(ikey, "%lu", s->start);
    key.mv_size = strlen(ikey) + 1;

    printf("Deleting with cursor\n");
    rc = mdb_txn_begin(env, NULL, 0, &txn);
    rc = mdb_open(txn, NULL, 0, &dbi);
    rc = mdb_cursor_open(txn, dbi, &cur2);

    rc = mdb_cursor_get(cur2, &key, &data, MDB_SET_KEY);
    if (rc!=0) goto clean;
    while ((rc = mdb_cursor_get(cur2, &key, &data, MDB_NEXT)) == 0)
    {
        if(atol(key.mv_data)==s->nb) break;
        if((atol(key.mv_data) % s->step) == 0) {
          if((atol(key.mv_data) % 1000) != 0) {
             printf("DelC[%lu,%lu] %s\n", s->start, s->nb, data.mv_data);
             rc = mdb_del(txn, dbi, &key, NULL);
          }
        }
    }
clean:
    mdb_cursor_close(cur2);
    rc = mdb_txn_commit(txn);
    return 0;
}

/*
  1 = lock(1)
  2 = lock(2)
  3 = unlock(2),lock(1) 
  4 = unlock(1),lock(2)
  5 = unlock(2)
  6 = unlock(1)
*/
/*
p1
1   pnum
4   pnum%2 + 3
3    
4
3

p2
2   pnum
3   pnum%2 + 3
4
3
4
*/
int LockUnlock(int which, 
               struct flock *fl1, struct flock *fl2,
               int *fd1, int *fd2) {
    if (which==1||which==3||which==5) {
        if (which == 3 || which == 5) {
            fl2->l_type = F_UNLCK;
            if (fcntl(*fd2, F_SETLK, fl2) == -1) {
               printf("fcntl: error unlocking 2\n");
               exit(1);
            }
        }

        if (which == 1 || which == 3) {
            fl1->l_type = F_WRLCK;
            if (fcntl(*fd1, F_SETLKW, fl1) == -1) {
               printf("fcntl: error locking 1\n");
               exit(1);
            }
        }
    } else {
        if (which == 4 || which == 6) {
            fl1->l_type = F_UNLCK;
            if (fcntl(*fd1, F_SETLK, fl1) == -1) {
               printf("fcntl: error unlocking 1\n");
               exit(1);
            }
        }

        if (which == 2 || which == 4) {
            fl2->l_type = F_WRLCK;
            if (fcntl(*fd2, F_SETLKW, fl2) == -1) {
               printf("fcntl: error locking 2\n");
               exit(1);
            }
        }
    }
    return 1;
}

int doparallel(int pnum)
{
    struct flock fl1 = {F_WRLCK, SEEK_SET, 0, 0, 0};
    int fd1;
    if ((fd1 = open("/tmp/plock1.me", O_RDWR)) == -1) {
        printf("error opening lock 1\n");
        exit(1);
    }

    struct flock fl2 = {F_WRLCK, SEEK_SET, 0, 0, 0};
    int fd2;
    if ((fd2 = open("/tmp/plock2.me", O_RDWR)) == -1) {
        printf("error opening lock 2\n");
        exit(1);
    }

    int rc;
    rc = mdb_env_create(&env);
    rc = mdb_env_set_mapsize(env, 10485760);
    rc = mdb_env_open(env, "/tmp/paradb", 0/*MDB_FIXEDMAP |MDB_NOSYNC*/, 0664);

    pthread_t ta[17*2];
    pthread_t tc[14*2];
    pthread_t td[31];

    unsigned long j = 0;
    stepper sa[17*2];
    stepper scd[14*2+31];

    for(j=0; j<17; j++) {
       sa[j].start = j*2000;
       sa[j].nb =  2000;
       sa[j].step = 1;
    }
    for(j=0; j<17; j++) {
       sa[17+j].start = (17+0)*2000 + j*1000;
       sa[17+j].nb =  1000;
       sa[17+j].step = 1;
    }
    for(j=0;j<14*2+31; j++) {
       scd[j].start = 0;
       scd[j].nb =  17*20+17*20; //(j+14)*20;
       if((j+14)*20 > 17*20+17*10) {
          scd[j].nb = 17*20+17*10 - scd[j].start ;
       }
       scd[j].step = 13; //5, 7, 11
       if(j%6==0) scd[j].step = 5; //5, 7, 11
       if(j%8==0) scd[j].step = 7; //5, 7, 11
       if(j%10==0) scd[j].step = 11; //5, 7, 11
    }

    int i = 0;
    //rc = step1BatchAdd();
    LockUnlock(pnum, &fl1, &fl2, &fd1, &fd2);
    for(i=0;i<17;i++)
    {
       pthread_create(ta+i, NULL, step1BatchAdd, (void *)&sa[i]);
    }
    for(i=0;i<17;i++)
    {
       if (pthread_join(ta[i], NULL)) printf("Problem joining add %d\n", i);
    }

    //rc = step1BatchDeleteWithCursor();
    LockUnlock((pnum%2)+3, &fl1, &fl2, &fd1, &fd2);
    for(i=0;i<14;i++)
    {
       pthread_create(tc+i, NULL, step1BatchDeleteWithCursor, (void *)&scd[i]);
    }
    for(i=0;i<14;i++)
    {
       if (pthread_join(tc[i], NULL)) printf("Problem joining delc %d\n", i);
    }

    //rc = step1BatchDelete();
    LockUnlock(pnum+2, &fl1, &fl2, &fd1, &fd2);
    for(i=0;i<31;i++)
    {
       pthread_create(td+i, NULL, step1BatchDelete, (void *)&scd[2*14+i]);
    }
    for(i=0;i<31;i++)
    {
       if (pthread_join(td[i], NULL)) printf("Problem joining del %d\n", i);
    }


    //rc = step1UnbatchedAdd();
    LockUnlock((pnum%2)+3, &fl1, &fl2, &fd1, &fd2);
    for(i=17;i<17*2;i++)
    {
       pthread_create(ta+i, NULL, step1UnbatchedAdd, (void *)&sa[i]);
    }
    for(i=17;i<17*2;i++)
    {
       if (pthread_join(ta[i], NULL)) printf("Problem joining add %d\n", i);
    }

    //rc = step1BatchDeleteWithCursor();
    LockUnlock(pnum+2, &fl1, &fl2, &fd1, &fd2);
    for(i=14;i<14*2;i++)
    {
       pthread_create(tc+i, NULL, step1BatchDeleteWithCursor, (void *)&scd[i]);
    }
    for(i=14;i<14*2;i++)
    {
       if (pthread_join(tc[i], NULL)) printf("Problem joining delc %d\n", i);
    }

    //LockUnlock(pnum+4, &fl1, &fl2, &fd1, &fd2); IF LAST WAS (pnum%2)+3
    LockUnlock((pnum%2)+5, &fl1, &fl2, &fd1, &fd2); //IF LAST WAS pnum+2

    //joining


    mdb_env_close(env);
    close(fd1);
    close(fd2);
    return 0;
}

int doserial(int pnum)
{
    struct flock fl1 = {F_WRLCK, SEEK_SET, 0, 0, 0};
    int fd1;
    if ((fd1 = open("/tmp/plock1.me", O_RDWR)) == -1) {
        printf("error opening lock 1\n");
        exit(1);
    }

    struct flock fl2 = {F_WRLCK, SEEK_SET, 0, 0, 0};
    int fd2;
    if ((fd2 = open("/tmp/plock2.me", O_RDWR)) == -1) {
        printf("error opening lock 2\n");
        exit(1);
    }

    int rc;
    rc = mdb_env_create(&env);
    rc = mdb_env_set_mapsize(env, 10485760);
    rc = mdb_env_open(env, "/tmp/paradb", 0/*MDB_FIXEDMAP |MDB_NOSYNC*/, 0664);

    unsigned long j = 0;
    stepper sa[17*2];
    stepper scd[14*2+31 + 2] = {
{10000UL,11000UL,6},
{11000UL,12000UL,7},
{12000UL,13000UL,8},
{13000UL,14000UL,9},
{14000UL,15000UL,10},
{15000UL,16000UL,11},
{16000UL,17000UL,12},
{17000UL,18000UL,13},
{18000UL,19000UL,14},
{19000UL,20000UL,6},
{20000UL,21000UL,7},
{21000UL,22000UL,8},
{22000UL,23000UL,9},
{23000UL,24000UL,10},
{24000UL,25000UL,11},
{25000UL,26000UL,12},
{26000UL,27000UL,13},
{27000UL,28000UL,14},
{28000UL,29000UL,6},
{29000UL,30000UL,7},
{30000UL,31000UL,8},
{31000UL,32000UL,9},
{32000UL,33000UL,10},
{33000UL,34000UL,11},
{34000UL,35000UL,12},
{35000UL,36000UL,13},
{36000UL,37000UL,14},
{37000UL,38000UL,6},
{38000UL,39000UL,7},
{39000UL,40000UL,8},
{40000UL,41000UL,9},
{41000UL,42000UL,10},
{42000UL,43000UL,11},
{43000UL,44000UL,12},
{44000UL,45000UL,13},
{45000UL,46000UL,14},
{46000UL,47000UL,6},
{47000UL,48000UL,7},
{48000UL,49000UL,8},
{49000UL,50000UL,9},
{50000UL,51000UL,10},
{51000UL,6000UL,11},
{6000UL,7000UL,12},
{7000UL,8000UL,13},
{8000UL,9000UL,14},

{33000UL,34000UL,6},
{34000UL,35000UL,7},
{35000UL,36000UL,8},
{36000UL,37000UL,9},
{37000UL,38000UL,10},
{38000UL,39000UL,11},
{39000UL,40000UL,12},
{40000UL,41000UL,13},
{41000UL,42000UL,10},

{14000UL,15000UL,10},
{15000UL,16000UL,11},
{16000UL,17000UL,12},
{17000UL,18000UL,13},
{18000UL,19000UL,14},
{19000UL,20000UL,6},
{20000UL,21000UL,7}};


    for(j=0; j<17; j++) {
       sa[j].start = j*2000;
       sa[j].nb =  2000;
       sa[j].step = 1;
    }
    for(j=0; j<17; j++) {
       sa[17+j].start = (17+0)*2000 + j*1000;
       sa[17+j].nb =  1000;
       sa[17+j].step = 1;
    }
#if 0
    for(j=0;j<14*2+31; j++) {
       scd[j].start = 0;
       scd[j].nb =  17*20+17*20; //(j+14)*20;
       if((j+14)*20 > 17*20+17*10) {
          scd[j].nb = 17*20+17*10 - scd[j].start ;
       }
       scd[j].step = 13; //5, 7, 11
       if(j%6==0) scd[j].step = 5; //5, 7, 11
       if(j%8==0) scd[j].step = 7; //5, 7, 11
       if(j%10==0) scd[j].step = 11; //5, 7, 11
    }
#endif
    int i = 0;

    LockUnlock(pnum, &fl1, &fl2, &fd1, &fd2);
    for(i=0;i<17;i++)
    rc = step1BatchAdd( (void *)&sa[i]);

    LockUnlock((pnum%2)+3, &fl1, &fl2, &fd1, &fd2);
    for(i=0;i<14;i++)
    rc = step1BatchDeleteWithCursor((void *)&scd[i]);

    LockUnlock(pnum+2, &fl1, &fl2, &fd1, &fd2);
    for(i=0;i<31;i++)
    rc = step1BatchDelete( (void *)&scd[2*i/*2*14+i*/]);

    LockUnlock((pnum%2)+3, &fl1, &fl2, &fd1, &fd2);
    for(i=17;i<17*2;i++)
    rc = step1UnbatchedAdd( (void *)&sa[i]);

    LockUnlock(pnum+2, &fl1, &fl2, &fd1, &fd2);
    for(i=14;i<14*2;i++)
    rc = step1BatchDeleteWithCursor( (void *)&scd[i]);

    //LockUnlock(pnum+4, &fl1, &fl2, &fd1, &fd2); IF LAST WAS (pnum%2)+3
    LockUnlock((pnum%2)+5, &fl1, &fl2, &fd1, &fd2); //IF LAST WAS pnum+2

    mdb_env_close(env);
    close(fd1);
    close(fd2);
    return 0;
}

int main(int argc, char * argv[])
{
    int pnum;
    if (argc > 1)
    {
        pnum = atoi(argv[1]);
        if (pnum!=2 && pnum!=1) {
            printf("Usage : process [1|2]\n");
            exit(1);
        }
    } else {
        printf("Usage : process [1|2]\n");
        exit(1);
    }
    doserial(pnum);
    return 0;
}

