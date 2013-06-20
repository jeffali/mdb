#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "mdb.h"

#define TWENTY 260
int createp_20(int count, char *dbname)
{
    int i = 0, rc;
    MDB_env *env;
    MDB_db *db;
    MDB_val key, data;
    MDB_txn *txn;
    MDB_stat *mst;
    char sval[32];

    srandom(time(NULL));
    int values[TWENTY] =
{
9, 13, 7, 8, 3, 4, 5, 18, 10, 20,
 29, 33, 27, 28, 23, 24, 25, 38, 30, 40,
1, 2, 11, 6, 15, 16, 17, 14, 12, 19,
 21, 22, 31, 26, 35, 36, 37, 34, 32, 39,

 49, 33, 47, 48, 43, 44, 45, 38, 30, 60,
 41, 42, 31, 46, 35, 36, 37, 34, 32, 39,

 69, 73, 67, 68, 63, 64, 65, 78, 70, 80,
 61, 62, 71, 66, 75, 76, 77, 74, 72, 79,
 89, 93, 87, 88, 83, 84, 85, 98, 90, 100,
 81, 82, 91, 86, 95, 96, 97, 94, 92, 99,
 109, 113, 107, 108, 103, 104, 105, 118, 110, 120,
 101, 102, 111, 106, 115, 116, 117, 114, 112, 119,
 129, 133, 127, 128, 123, 124, 125, 138, 130, 140,
 121, 122, 131, 126, 135, 136, 137, 134, 132, 139,
 149, 153, 147, 148, 143, 144, 145, 158, 150, 160,
 141, 142, 151, 146, 155, 156, 157, 154, 152, 159,
 169, 173, 167, 168, 163, 164, 165, 178, 170, 180,
 161, 162, 171, 166, 175, 176, 177, 174, 172, 179,
 189, 193, 187, 188, 183, 184, 185, 198, 190, 200,
 181, 182, 191, 186, 195, 196, 197, 194, 192, 199,
 209, 213, 207, 208, 203, 204, 205, 218, 210, 220,
 201, 202, 211, 206, 215, 216, 217, 214, 212, 219,
 229, 233, 227, 228, 223, 224, 225, 238, 230, 240,
 221, 222, 231, 226, 235, 236, 237, 234, 232, 239,
 249, 253, 247, 248, 243, 244, 245, 258, 250, 260,
 241, 242, 251, 246, 255, 256, 257, 254, 252, 259,
};

#if 0
    count = random()%512;        
    values = (int *)malloc(count*sizeof(int));
    
    for(i = 0;i<count;i++) {
        values[i] = random()%1024;
    }
#endif
    
    rc = mdbenv_create(&env, 10485760);
    rc = mdbenv_open(env, dbname, MDB_FIXEDMAP, 0664);

    key.mv_size = sizeof(int);
    key.mv_data = sval;
    data.mv_size = sizeof(sval);
    data.mv_data = sval;
    
    for (i=0;i<count;i++) {    
        rc = mdb_txn_begin(env, 0, &txn);
        rc = mdb_open(env, txn, NULL, 0, &db);
        sprintf(sval, "X%03xY%dfghijkZ", values[i], values[i]);
        mdb_put(db, txn, &key, &data, 0);
        rc = mdb_txn_commit(txn);
    }        

#if 0
rc = mdbenv_stat(env, &mst);

for (i= count - 1; i > -1; i-= (random()%5)) {    
    rc = mdb_txn_begin(env, 0, &txn);
    sprintf(sval, "%03x ", values[i]);
    rc = mdb_del(db, txn, &key, NULL);
    rc = mdb_txn_commit(txn);
}
    free(values);
#endif
    rc = mdbenv_stat(env, &mst);
    mdb_close(db);
    mdbenv_close(env);

    return 0;
}

int main(int argc,char * argv[])
{
//    createp_20(1,"/tmp/ptestdb.001");
//    createp_20(2,"/tmp/ptestdb.002");
//    createp_20(3,"/tmp/ptestdb.003");
//    createp_20(4,"/tmp/ptestdb.004");
//    createp_20(20,"/tmp/ptestdb.020");
//    createp_20(40,"/tmp/ptestdb.040");
//    createp_20(100,"/tmp/ptestdb.100");
//    createp_20(200,"/tmp/ptestdb.200");
//    createp_20(255,"/tmp/ptestdb.255");
    createp_20(260,"/tmp/ptestdb.260");
    return 0;
}

