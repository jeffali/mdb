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
#include "lmdb.h"

#include <fcntl.h>
MDB_env *env;

void *step1BatchAdd(void *x)
{
    int i = 0, j = 0, rc;
    MDB_dbi dbi;
    MDB_val key, data;
    MDB_txn *txn;
    MDB_stat mst;
    int count;
    int *values;
    char sval[32];

    rc = mdb_txn_begin(env, NULL, 0, &txn);
    rc = mdb_open(txn, NULL, 0, &dbi);

    srandom(time(NULL));

    count = (random()%23841) + 64;
    values = (int *)malloc(count*sizeof(int));

    for(i = 0;i<count;i++) {
      values[i] = random()%25024;
    }
   
    key.mv_size = sizeof(int);
    key.mv_data = sval;
    data.mv_size = sizeof(sval);
    data.mv_data = sval;

    printf("Adding %d values\n", count);
    for (i=0;i<count;i++) {  
       sprintf(sval, "%03x %d foo bar", values[i], values[i]);
       rc = mdb_put(txn, dbi, &key, &data, MDB_NOOVERWRITE);
       if (rc) {
         j++;
         data.mv_size = sizeof(sval);
         data.mv_data = sval;
       }
    }
    if (j) printf("%d duplicates skipped\n", j);
    rc = mdb_txn_commit(txn);
    rc = mdb_env_stat(env, &mst);
    free(values);
    return 0;
}

void *step1BatchDelete(void *x)
{
    int i = 0, j = 0, rc;
    MDB_dbi dbi;
    MDB_val key, data;
    MDB_txn *txn;
    MDB_stat mst;
    int count;
    int *values;
    char sval[32];


    srandom(time(NULL) - 166626);

    count = (random()%9384) + 64;
    values = (int *)malloc(count*sizeof(int));

    for(i = 0;i<count;i++) {
      values[i] = random()%9024;
    }
   
    key.mv_size = sizeof(int);
    key.mv_data = sval;
    data.mv_size = sizeof(sval);
    data.mv_data = sval;

    j=0;
    key.mv_data = sval;
    for (i= count - 1; i > -1; i-= (random()%9000)) {  
      j++;
      txn=NULL;
      rc = mdb_txn_begin(env, NULL, 0, &txn);
      rc = mdb_open(txn, NULL, 0, &dbi);
      sprintf(sval, "%03x ", values[i]);
      rc = mdb_del(txn, dbi, &key, NULL);
      if (rc) {
        j--;
        mdb_txn_abort(txn);
      } else {
        rc = mdb_txn_commit(txn);
      }
      mdb_close(env, dbi);
    }
    free(values);
    printf("Deleted %d values\n", j);

    rc = mdb_env_stat(env, &mst);
    return 0;
}

void *step1BatchDeleteWithCursor(void *x)
{
    int i = 0, rc;
    MDB_dbi dbi;
    MDB_val key, data;
    MDB_txn *txn;
    MDB_cursor *cur2;

    printf("Deleting with cursor\n");
    rc = mdb_txn_begin(env, NULL, 0, &txn);
    rc = mdb_open(txn, NULL, 0, &dbi);
    rc = mdb_cursor_open(txn, dbi, &cur2);

    for (i=0; i<50; i++) {
      rc = mdb_cursor_get(cur2, &key, &data, MDB_NEXT);
      printf("key: %p %.*s, data: %p %.*s\n",
        key.mv_data,  (int) key.mv_size,  (char *) key.mv_data,
        data.mv_data, (int) data.mv_size, (char *) data.mv_data);
      rc = mdb_del(txn, dbi, &key, NULL);
    }

    printf("Restarting cursor in txn\n");
    rc = mdb_cursor_get(cur2, &key, &data, MDB_FIRST);
    printf("key: %p %.*s, data: %p %.*s\n",
      key.mv_data,  (int) key.mv_size,  (char *) key.mv_data,
      data.mv_data, (int) data.mv_size, (char *) data.mv_data);
    for (i=0; i<32; i++) {
      rc = mdb_cursor_get(cur2, &key, &data, MDB_NEXT);
      printf("key: %p %.*s, data: %p %.*s\n",
        key.mv_data,  (int) key.mv_size,  (char *) key.mv_data,
        data.mv_data, (int) data.mv_size, (char *) data.mv_data);
    }
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
if(which==1||which==3||which==5){
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

    int nbthreads = 10;

    pthread_t ta[17*2];
    pthread_t tc[14*2];
    pthread_t td[31];
    int i = 0;

    //rc = step1BatchAdd();
    LockUnlock(pnum, &fl1, &fl2, &fd1, &fd2);
    for(i=0;i<17;i++)
    {
       pthread_create(ta+i, NULL, step1BatchAdd, NULL);
    }

    //rc = step1BatchDeleteWithCursor();
    LockUnlock((pnum%2)+3, &fl1, &fl2, &fd1, &fd2);
    for(i=0;i<14;i++)
    {
       pthread_create(tc+i, NULL, step1BatchDeleteWithCursor, NULL);
    }

    //rc = step1BatchDelete();
    LockUnlock(pnum+2, &fl1, &fl2, &fd1, &fd2);
    for(i=0;i<31;i++)
    {
       pthread_create(td+i, NULL, step1BatchDelete, NULL);
    }

    //rc = step1BatchAdd();
    LockUnlock((pnum%2)+3, &fl1, &fl2, &fd1, &fd2);
    for(i=17;i<17*2;i++)
    {
       pthread_create(ta+i, NULL, step1BatchAdd, NULL);
    }
    //rc = step1BatchDeleteWithCursor();
    LockUnlock(pnum+2, &fl1, &fl2, &fd1, &fd2);
    for(i=14;i<14*2;i++)
    {
       pthread_create(tc+i, NULL, step1BatchDeleteWithCursor, NULL);
    }
    //LockUnlock(pnum+4, &fl1, &fl2, &fd1, &fd2); IF LAST WAS (pnum%2)+3
    LockUnlock((pnum%2)+5, &fl1, &fl2, &fd1, &fd2); //IF LAST WAS pnum+2

    //joining
    for(i=0;i<17*2;i++)
    {
       if (pthread_join(ta[i], NULL)) printf("Problem joining add %d\n", i);
    }

    for(i=0;i<31;i++)
    {
       if (pthread_join(td[i], NULL)) printf("Problem joining del %d\n", i);
    }

    for(i=0;i<14*2;i++)
    {
       if (pthread_join(tc[i], NULL)) printf("Problem joining delc %d\n", i);
    }

    mdb_env_close(env);
    close(fd1);
    close(fd2);
    return 0;
}
