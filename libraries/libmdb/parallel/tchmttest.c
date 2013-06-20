#include <tcutil.h>
#include <tchdb.h>
#include "myconf.h"

#define RECBUFSIZ      48                // buffer for records

typedef struct {                         // type of structure for read thread
  TCHDB *hdb;
  int rnum;
  bool wb;
  bool rnd;
  int id;
} TARGREAD;

/* global variables */
const char *g_progname;                  // program name
unsigned int g_randseed;                 // random seed
int g_dbgfd;                             // debugging output


/* function prototypes */
int main(int argc, char **argv);
static void usage(void);
static void iprintf(const char *format, ...);
static void iputchar(int c);
static void eprint(TCHDB *hdb, int line, const char *func);
static void mprint(TCHDB *hdb);
static void sysprint(void);
static int myrand(int range);
static int myrandnd(int range);
static bool iterfunc(const void *kbuf, int ksiz, const void *vbuf, int vsiz, void *op);
static int runread(int argc, char **argv);
static int procread(const char *path, int tnum, int rcnum, int xmsiz, int dfunit, int omode,
                    bool wb, bool rnd);
static void *threadread(void *targ);


/* main routine */
int main(int argc, char **argv){
  g_progname = argv[0];
  const char *ebuf = getenv("TCRNDSEED");
  g_randseed = ebuf ? tcatoix(ebuf) : tctime() * 1000;
  srand(g_randseed);
  ebuf = getenv("TCDBGFD");
  g_dbgfd = ebuf ? tcatoix(ebuf) : UINT16_MAX;
  if(argc < 2) usage();
  int rv = 0;
  if(!strcmp(argv[1], "read")){
    rv = runread(argc, argv);
  } else {
    usage();
  }
  if(rv != 0){
    printf("FAILED: TCRNDSEED=%u PID=%d", g_randseed, (int)getpid());
    for(int i = 0; i < argc; i++){
      printf(" %s", argv[i]);
    }
    printf("\n\n");
  }
  return rv;
}


/* print the usage and exit */
static void usage(void){
  fprintf(stderr, "%s: test cases of the hash database API of Tokyo Cabinet\n", g_progname);
  fprintf(stderr, "\n");
  fprintf(stderr, "usage:\n");
  fprintf(stderr, "  %s write [-tl] [-td|-tb|-tt|-tx] [-rc num] [-xm num] [-df num]"
          " [-nl|-nb] [-as] [-rnd] path tnum rnum [bnum [apow [fpow]]]\n", g_progname);
  fprintf(stderr, "  %s read [-rc num] [-xm num] [-df num] [-nl|-nb] [-wb] [-rnd] path tnum\n",
          g_progname);
  fprintf(stderr, "  %s remove [-rc num] [-xm num] [-df num] [-nl|-nb] [-rnd] path tnum\n",
          g_progname);
  fprintf(stderr, "  %s wicked [-tl] [-td|-tb|-tt|-tx] [-nl|-nb] [-nc]"
          " path tnum rnum\n", g_progname);
  fprintf(stderr, "  %s typical [-tl] [-td|-tb|-tt|-tx] [-rc num] [-xm num] [-df num]"
          " [-nl|-nb] [-nc] [-rr num] path tnum rnum [bnum [apow [fpow]]]\n", g_progname);
  fprintf(stderr, "  %s race [-tl] [-td|-tb|-tt|-tx] [-xm num] [-df num] [-nl|-nb]"
          " path tnum rnum [bnum [apow [fpow]]]\n", g_progname);
  fprintf(stderr, "\n");
  exit(1);
}


/* print formatted information string and flush the buffer */
static void iprintf(const char *format, ...){
  va_list ap;
  va_start(ap, format);
  vprintf(format, ap);
  fflush(stdout);
  va_end(ap);
}


/* print a character and flush the buffer */
static void iputchar(int c){
  putchar(c);
  fflush(stdout);
}


/* print error message of hash database */
static void eprint(TCHDB *hdb, int line, const char *func){
  const char *path = tchdbpath(hdb);
  int ecode = tchdbecode(hdb);
  fprintf(stderr, "%s: %s: %d: %s: error: %d: %s\n",
          g_progname, path ? path : "-", line, func, ecode, tchdberrmsg(ecode));
}


/* print members of hash database */
static void mprint(TCHDB *hdb){
  if(hdb->cnt_writerec < 0) return;
  iprintf("bucket number: %lld\n", (long long)tchdbbnum(hdb));
  iprintf("used bucket number: %lld\n", (long long)tchdbbnumused(hdb));
  iprintf("cnt_writerec: %lld\n", (long long)hdb->cnt_writerec);
  iprintf("cnt_reuserec: %lld\n", (long long)hdb->cnt_reuserec);
  iprintf("cnt_moverec: %lld\n", (long long)hdb->cnt_moverec);
  iprintf("cnt_readrec: %lld\n", (long long)hdb->cnt_readrec);
  iprintf("cnt_searchfbp: %lld\n", (long long)hdb->cnt_searchfbp);
  iprintf("cnt_insertfbp: %lld\n", (long long)hdb->cnt_insertfbp);
  iprintf("cnt_splicefbp: %lld\n", (long long)hdb->cnt_splicefbp);
  iprintf("cnt_dividefbp: %lld\n", (long long)hdb->cnt_dividefbp);
  iprintf("cnt_mergefbp: %lld\n", (long long)hdb->cnt_mergefbp);
  iprintf("cnt_reducefbp: %lld\n", (long long)hdb->cnt_reducefbp);
  iprintf("cnt_appenddrp: %lld\n", (long long)hdb->cnt_appenddrp);
  iprintf("cnt_deferdrp: %lld\n", (long long)hdb->cnt_deferdrp);
  iprintf("cnt_flushdrp: %lld\n", (long long)hdb->cnt_flushdrp);
  iprintf("cnt_adjrecc: %lld\n", (long long)hdb->cnt_adjrecc);
  iprintf("cnt_defrag: %lld\n", (long long)hdb->cnt_defrag);
  iprintf("cnt_shiftrec: %lld\n", (long long)hdb->cnt_shiftrec);
  iprintf("cnt_trunc: %lld\n", (long long)hdb->cnt_trunc);
}


/* print system information */
static void sysprint(void){
  TCMAP *info = tcsysinfo();
  if(info){
    tcmapiterinit(info);
    const char *kbuf;
    while((kbuf = tcmapiternext2(info)) != NULL){
      iprintf("sys_%s: %s\n", kbuf, tcmapiterval2(kbuf));
    }
    tcmapdel(info);
  }
}


/* get a random number */
static int myrand(int range){
  if(range < 2) return 0;
  int high = (unsigned int)rand() >> 4;
  int low = range * (rand() / (RAND_MAX + 1.0));
  low &= (unsigned int)INT_MAX >> 4;
  return (high + low) % range;
}


/* get a random number based on normal distribution */
static int myrandnd(int range){
  int num = (int)tcdrandnd(range >> 1, range / 10);
  return (num < 0 || num >= range) ? 0 : num;
}


/* iterator function */
static bool iterfunc(const void *kbuf, int ksiz, const void *vbuf, int vsiz, void *op){
  unsigned int sum = 0;
  while(--ksiz >= 0){
    sum += ((char *)kbuf)[ksiz];
  }
  while(--vsiz >= 0){
    sum += ((char *)vbuf)[vsiz];
  }
  return myrand(100 + (sum & 0xff)) > 0;
}


/* parse arguments of read command */
static int runread(int argc, char **argv){
  char *path = NULL;
  char *tstr = NULL;
  int rcnum = 0;
  int xmsiz = -1;
  int dfunit = 0;
  int omode = 0;
  bool wb = false;
  bool rnd = false;
  for(int i = 2; i < argc; i++){
    if(!path && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-rc")){
        if(++i >= argc) usage();
        rcnum = tcatoix(argv[i]);
      } else if(!strcmp(argv[i], "-xm")){
        if(++i >= argc) usage();
        xmsiz = tcatoix(argv[i]);
      } else if(!strcmp(argv[i], "-df")){
        if(++i >= argc) usage();
        dfunit = tcatoix(argv[i]);
      } else if(!strcmp(argv[i], "-nl")){
        omode |= HDBONOLCK;
      } else if(!strcmp(argv[i], "-nb")){
        omode |= HDBOLCKNB;
      } else if(!strcmp(argv[i], "-wb")){
        wb = true;
      } else if(!strcmp(argv[i], "-rnd")){
        rnd = true;
      } else {
        usage();
      }
    } else if(!path){
      path = argv[i];
    } else if(!tstr){
      tstr = argv[i];
    } else {
      usage();
    }
  }
  if(!path || !tstr) usage();
  int tnum = tcatoix(tstr);
  if(tnum < 1) usage();
  int rv = procread(path, tnum, rcnum, xmsiz, dfunit, omode, wb, rnd);
  return rv;
}

/* perform read command */
static int procread(const char *path, int tnum, int rcnum, int xmsiz, int dfunit, int omode,
                    bool wb, bool rnd){
  iprintf("<Reading Test>\n  seed=%u  path=%s  tnum=%d  rcnum=%d  xmsiz=%d  dfunit=%d  omode=%d"
          "  wb=%d  rnd=%d\n\n", g_randseed, path, tnum, rcnum, xmsiz, dfunit, omode, wb, rnd);
  bool err = false;
  double stime = tctime();
  TCHDB *hdb = tchdbnew();
  if(g_dbgfd >= 0) tchdbsetdbgfd(hdb, g_dbgfd);
  if(!tchdbsetmutex(hdb)){
    eprint(hdb, __LINE__, "tchdbsetmutex");
    err = true;
  }
  if(!tchdbsetcodecfunc(hdb, _tc_recencode, NULL, _tc_recdecode, NULL)){
    eprint(hdb, __LINE__, "tchdbsetcodecfunc");
    err = true;
  }
  if(!tchdbsetcache(hdb, rcnum)){
    eprint(hdb, __LINE__, "tchdbsetcache");
    err = true;
  }
  if(xmsiz >= 0 && !tchdbsetxmsiz(hdb, xmsiz)){
    eprint(hdb, __LINE__, "tchdbsetxmsiz");
    err = true;
  }
  if(dfunit >= 0 && !tchdbsetdfunit(hdb, dfunit)){
    eprint(hdb, __LINE__, "tchdbsetdfunit");
    err = true;
  }
  if(!tchdbopen(hdb, path, HDBOREADER | omode)){
    eprint(hdb, __LINE__, "tchdbopen");
    err = true;
  }
  int rnum = tchdbrnum(hdb) / tnum;
  TARGREAD targs[tnum];
  pthread_t threads[tnum];
  if(tnum == 1){
    targs[0].hdb = hdb;
    targs[0].rnum = rnum;
    targs[0].wb = wb;
    targs[0].rnd = rnd;
    targs[0].id = 0;
    if(threadread(targs) != NULL) err = true;
  } else {
    for(int i = 0; i < tnum; i++){
      targs[i].hdb = hdb;
      targs[i].rnum = rnum;
      targs[i].wb = wb;
      targs[i].rnd = rnd;
      targs[i].id = i;
      if(pthread_create(threads + i, NULL, threadread, targs + i) != 0){
        eprint(hdb, __LINE__, "pthread_create");
        targs[i].id = -1;
        err = true;
      }
    }
    for(int i = 0; i < tnum; i++){
      if(targs[i].id == -1) continue;
      void *rv;
      if(pthread_join(threads[i], &rv) != 0){
        eprint(hdb, __LINE__, "pthread_join");
        err = true;
      } else if(rv){
        err = true;
      }
    }
  }
  iprintf("record number: %llu\n", (unsigned long long)tchdbrnum(hdb));
  iprintf("size: %llu\n", (unsigned long long)tchdbfsiz(hdb));
  mprint(hdb);
  sysprint();
  if(!tchdbclose(hdb)){
    eprint(hdb, __LINE__, "tchdbclose");
    err = true;
  }
  tchdbdel(hdb);
  iprintf("time: %.3f\n", tctime() - stime);
  iprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}



/* thread the read function */
static void *threadread(void *targ){
  TCHDB *hdb = ((TARGREAD *)targ)->hdb;
  int rnum = ((TARGREAD *)targ)->rnum;
  bool wb = ((TARGREAD *)targ)->wb;
  bool rnd = ((TARGREAD *)targ)->rnd;
  int id = ((TARGREAD *)targ)->id;
  bool err = false;
  int base = id * rnum;
  for(int i = 1; i <= rnum && !err; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%08d", base + (rnd ? myrandnd(i) : i));
    int vsiz;
    if(wb){
      char vbuf[RECBUFSIZ];
      int vsiz = tchdbget3(hdb, kbuf, ksiz, vbuf, RECBUFSIZ);
      if(vsiz < 0 && (!rnd || tchdbecode(hdb) != TCENOREC)){
        eprint(hdb, __LINE__, "tchdbget3");
        err = true;
      }
    } else {
      char *vbuf = tchdbget(hdb, kbuf, ksiz, &vsiz);
      if(!vbuf && (!rnd || tchdbecode(hdb) != TCENOREC)){
        eprint(hdb, __LINE__, "tchdbget");
        err = true;
      }
      tcfree(vbuf);
    }
    if(id == 0 && rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  return err ? "error" : NULL;
}

// END OF FILE
