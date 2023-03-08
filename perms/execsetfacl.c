#include<stdio.h>
#include<stddef.h>
#include<stdlib.h>
#include<unistd.h>
int main(int argc, char *argv[])
{
    int ret;
    ret = execv("/usr/bin/setfacl", argv);
    printf("ret %d", ret);
    exit(ret);
}

