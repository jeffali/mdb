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

MDB_env *env;

int step1BatchAdd()
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

    count = (random()%384) + 64;
    values = (int *)malloc(count*sizeof(int));

    for(i = 0;i<count;i++) {
      values[i] = random()%1024;
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

int step1BatchDelete()
{
    int i = 0, j = 0, rc;
    MDB_dbi dbi;
    MDB_val key, data;
    MDB_txn *txn;
    MDB_stat mst;
    int count;
    int *values;
    char sval[32];

    srandom(time(NULL));

    count = (random()%384) + 64;
    values = (int *)malloc(count*sizeof(int));

    for(i = 0;i<count;i++) {
      values[i] = random()%1024;
    }
   
    key.mv_size = sizeof(int);
    key.mv_data = sval;
    data.mv_size = sizeof(sval);
    data.mv_data = sval;

    j=0;
    key.mv_data = sval;
    for (i= count - 1; i > -1; i-= (random()%5)) {  
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

int main(int argc,char * argv[])
{
    int rc;
    rc = mdb_env_create(&env);
    rc = mdb_env_set_mapsize(env, 10485760);
    rc = mdb_env_open(env, "/tmp/paradb", 0/*MDB_FIXEDMAP |MDB_NOSYNC*/, 0664);

    rc = step1BatchAdd();
    rc = step1BatchDelete();

    mdb_env_close(env);
    return 0;
}
