#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "lmdb.h"

int generic_read(char *path, int pv, int px)
{
    int rc;
    MDB_env *env;

    MDB_dbi dbi;
    MDB_val key, data;
    MDB_txn *txn;
    MDB_cursor *cur2;

    rc = mdb_env_create(&env);
    rc = mdb_env_set_mapsize(env, 10485760);
    rc = mdb_env_open(env, path, 0/*MDB_FIXEDMAP |MDB_NOSYNC*/, 0664);

    rc = mdb_txn_begin(env, NULL, 0, &txn);
    rc = mdb_open(txn, NULL, 0, &dbi);
    rc = mdb_cursor_open(txn, dbi, &cur2);

    int ii = 0;
    while ((rc = mdb_cursor_get(cur2, &key, &data, MDB_NEXT)) == 0) {
#if 0
        printf("key: %p %.*s, data: %p %.*s\n",
                key.mv_data,  (int) key.mv_size,  (char *) key.mv_data,
                data.mv_data, (int) data.mv_size, (char *) data.mv_data);
        printf("*************************** %d *************\n",ii);
#endif
        printf("%s\n", key.mv_data);
        ii++;
    }
    mdb_cursor_close(cur2);
    rc = mdb_txn_commit(txn);
    return 0;
}
static void usage(void) {
  printf("list [-pv|-px] path\n");
}

int main(int argc,char * argv[])
{
  char *path = NULL;
  int pv = 0;
  int px = 0;
  for(int i = 1; i < argc; i++){
    if(!path && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-pv")){
        pv = 1;
      } else if(!strcmp(argv[i], "-px")){
        px = 1;
      } else {
        usage(); return 1;
      }
    } else if(!path){
      path = argv[i];
    } else {
      usage(); return 1;
    }
  }
  if(!path) {usage(); return 1;}
  generic_read(path, pv, px);
  return 0;
}
