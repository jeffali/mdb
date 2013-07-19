#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>

int main(int argc,char * argv[])
{
    struct flock fl1 = {F_WRLCK, SEEK_SET, 0, 0, 0};
    struct flock fl2 = {F_WRLCK, SEEK_SET, 0, 0, 0};
    int fd1;
    int fd2;
    if ((fd1 = open("/tmp/plock1.me", O_RDWR)) == -1) {
        printf("error opening lock 1\n");
        exit(1);
    }
    if ((fd2 = open("/tmp/plock2.me", O_RDWR)) == -1) {
        printf("error opening lock 2\n");
        exit(1);
    }


    if (fcntl(fd1, F_SETLKW, &fl1) == -1) {
       printf("fcntl: error locking 1\n");
       exit(1);
    }
    if (fcntl(fd2, F_SETLKW, &fl2) == -1) {
       printf("fcntl: error locking 2\n");
       exit(1);
    }

    printf("Press <RETURN> to release locks: ");
    getchar();

    fl1.l_type = F_UNLCK;
    if (fcntl(fd1, F_SETLK, &fl1) == -1) {
       printf("fcntl: error unlocking 1\n");
       exit(1);
    }
    fl2.l_type = F_UNLCK;
    if (fcntl(fd2, F_SETLK, &fl2) == -1) {
       printf("fcntl: error unlocking 2\n");
       exit(1);
    }

    printf("Unlocked 1 & 2\n");
    close(fd1);
    close(fd2);

    return 0;
}
