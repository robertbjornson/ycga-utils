#include<stdio.h>
#include<stddef.h>
#include<stdlib.h>
#include<unistd.h>
#include<string.h>
#include<getopt.h>

/* This program is intended to allow certain users to run setfacl as
root on files or directories only under certain locations. 

written by robert.bjornson@yale.edu
*/

// only these users may run the program
const int uids[] = {10017, 11851, 12107, 10973}; // rdb9, ccc7, jk2269, gw92

// Only locations under these prefixes are allowed.  Make sure to use the true path.
const char * pref[] =
  {
   "/gpfs/ycga/sequencers", // illumina
   "/gpfs/gibbs/pi/ycga",   // pacbio and 10x
   "/gpfs/ycga/project/lsprog/rdb9/repos/ycga-utils", //testing
  };      

int sanity_check_args(int argc, char *argv[])
{

  int c;
  while ((c = getopt (argc, argv, "dbm")) != -1) ;

  if (argc-optind != 2) {
    printf("Error:Only single path supported\n");
    exit(1);
  }
}

int main(int argc, char *argv[])
{
    int ret;
    int tst;
    char *pth, *truepth;
    int ok;
    int me;

    sanity_check_args(argc, argv);
    int numprefs = sizeof(pref)/sizeof(pref[0]);
    int numuids = sizeof(uids)/sizeof(uids[0]);
    
    pth=argv[argc-1];
    truepth=realpath(pth, 0);
    if (truepth==NULL) {
      printf("Error: can't read path %s\n", pth);
      exit(1);
    }

    // Test uid
    ok=0;
    me=getuid();
    for (int i=0; i<numuids; i++) {
      if (me == uids[i]) ok=1;
    }
    if (ok==0) {
      printf("Error: Uid %d not allowed.\n", me);
      exit(1);
    }

    // test path
    ok=0;
    for (int i=0; i<numprefs; i++) {
      tst=strncmp(pref[i], truepth, strlen(pref[i]));
      if (tst == 0) ok=1;
    }
    if (ok==0) {
      printf("Error: Path %s not allowed.\n", truepth);
      exit(1);
    }

    // OK!
    ret = execv("/usr/bin/setfacl", argv);
    if (ret!=0)
	printf("Error: problem invoking setfacl");
    exit(ret);
}

