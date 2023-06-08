#include<stdio.h>
#include<stddef.h>
#include<stdlib.h>
#include<unistd.h>
int main(int argc, char *argv[])
{
    int ret;
    ret = execv("/usr/bin/getfacl", argv);
    exit(ret);
}
