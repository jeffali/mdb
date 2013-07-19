#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>

int main(int argc,char * argv[])
{

    struct flock fl = {F_WRLCK, SEEK_SET, 0, 0, 0};
    int fd;
    if ((fd = open("/tmp/plock.me", O_RDWR)) == -1) {
        printf("error opening lock\n");
        exit(1);
    }


    if (fcntl(fd, F_SETLKW, &fl) == -1) {
       printf("fcntl: error locking\n");
       exit(1);
    }

    printf("Press <RETURN> to release lock: ");
    getchar();

    fl.l_type = F_UNLCK;
    if (fcntl(fd, F_SETLK, &fl) == -1) {
       printf("fcntl: error unlocking\n");
       exit(1);
    }
    printf("Unlocked\n");
    close(fd);

    return 0;
}
