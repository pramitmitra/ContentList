#include <stdio.h>
#include <string.h>
#include <time.h>

/*
extern clock_t clock(void);
extern double  difftime(time_t, time_t);
extern time_t  mktime(struct tm *);
extern time_t  time(time_t *);
extern char *asctime(const struct tm *);
extern char *ctime (const time_t *);
struct tm   *gmtime(const time_t *);
struct tm   *localtime(const time_t *);
extern size_t  strftime(char *, size_t, const char *, const struct tm *);
*/

main(int argc, char *argv[])
{
	char dateTime[20];
	time_t timeInSeconds = -1;
	struct tm myTm;
	int yyyy,mins,minsInSeconds;
	int argsRqrd = 2;

	/* check parameters */
	if (argc != argsRqrd+1 || strlen(argv[1]) != 19)
	{
		fprintf(stderr, "Incorrect Usage: %d parameters required (\"yyyy-mm-dd hh:mm:ss\" add_mins)\n", argsRqrd);
		return 1;
	}

	strcpy(dateTime,argv[1]);

	/* set used fields */
	myTm.tm_mday = atoi(dateTime+8);
	myTm.tm_mon = atoi(dateTime+5)-1;
	yyyy=atoi(dateTime);
	myTm.tm_year = yyyy-1900;
	myTm.tm_sec=atoi(dateTime+17);
	myTm.tm_min=atoi(dateTime+14);
	myTm.tm_hour=atoi(dateTime+11);

	/* set unused fields */
	myTm.tm_wday = -1;
	myTm.tm_yday = -1;
	myTm.tm_isdst = -1;

	/* convert struct tm to time_t */
	timeInSeconds = mktime(&myTm);

	/* set minute increment */
	mins = atoi(argv[2]);

	/* convert minutes increment to time_t offset */
	minsInSeconds = mins*60;

	/* increment time_t value by number of minutes */
	timeInSeconds += minsInSeconds;

	/* print result - replaced cftime with strftime for redhat port on 20120801 - koaks */
	strftime(dateTime, sizeof(dateTime), "%Y-%m-%d %T", localtime(&timeInSeconds) );
	puts(dateTime);

	return 0;
};
