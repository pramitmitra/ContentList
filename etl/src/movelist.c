#include <stdio.h>

main(int argc, char *argv[])
{
	char iline[2048];
	char nline[2048];
	int cc;

	while ((cc=scanf("%s%s",iline,nline))==2)
	{
		if (rename(iline,nline))
		{
			fprintf(stderr,"%s: error renaming %s to %s\n", argv[0], iline, nline);
			perror(argv[0]);
			return 2;
		}
	}

	if (ferror(stdin))
	{
		fprintf(stderr,"%s: error reading input\n", argv[0]);
		perror(argv[0]);
		return 4;
	}

	if (cc != EOF)
	{
		fprintf(stderr,"%s: incomplete conversion (need even number of arguments)\n", argv[0]);
		return 4;
	} 

	
	return 0;
}
