#include<stdio.h>
#include<stdlib.h>
#include<errno.h>
#include<string.h>
#include "pds.h"
#include "bst.h"

int main(int argc, char *argv[])
{
    pds_create_schema (argv[2]);
    return PDS_SUCCESS;
}