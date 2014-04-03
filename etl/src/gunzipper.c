#include <stdio.h>
#include <stdlib.h>
#include <zlib.h> /* compile and link using -lz option */

static char *prog;
static char *infile;
static char *instr;
static char *inrterm;
static gzFile file;

static void exiterror(char *errmsg)
{
	fprintf(stderr,"%s: %s %s\n", prog, errmsg, infile);
	perror(prog);
	if (file) gzclose(file);
	exit(4);
}

int main (int argc, char *argv[])
{
char inbuff[16384];
int charcnt, i;
int termfound = 1;

prog = argv[0];

if (argc != 4)
{
	fprintf(stderr, "%s: Error invalid number of parameters, passed %d, "
		"expecting 3, \nUsage: %s infile append_str rec_term\n", prog, argc-1, prog);
	return 4;
}

infile = argv[1];
instr = argv[2];
inrterm = argv[3];

if (*inrterm=='\\' && *(inrterm+1)=='n')
	*inrterm='\n';

if ((file = gzopen(argv[1], "r")) == NULL)
	exiterror("Error opening file");

/*  This code examines each character to locate the record terminator.  This approach deliberately avoids string operations, which would otherwise be used to optimize performance via strchr/memcpy.  This is required to handle any type of incoming file, inlcluding binary */

while ((charcnt = gzread(file, inbuff, sizeof(inbuff))) > 0)
{
	for(i=0; i<charcnt; i++)
	{
		if (termfound)
		{
			if (fputs(instr,stdout) == EOF)
				exiterror("Error writing output");
			termfound=0;
		}

		if (putchar(inbuff[i]) < 0)
			exiterror("Error writing output");

		if (inbuff[i] == *inrterm)
			termfound=1;
	}

}

if (charcnt < 0)
	exiterror("Error reading output");

gzclose(file);

return 0;
}
