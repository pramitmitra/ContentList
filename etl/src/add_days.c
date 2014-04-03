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
	char date[9];
	time_t timeInSeconds = -1;
	struct tm myTm;
	int yyyy,days,daysInSeconds;
	int argsRqrd = 2;

	/* check parameters */
	if (argc != argsRqrd+1 || strlen(argv[1]) != 8)
	{
		fprintf(stderr, "Incorrect Usage: %d parameters required (yyyymmdd incdays)\n", argsRqrd);
		return 1;
	}

	strcpy(date,argv[1]);

	/* set day of month */
	myTm.tm_mday = atoi(date+6);
	date[6]=0;

	/* set month */
	myTm.tm_mon = atoi(date+4)-1;
	date[4]=0;

	/* set year (offset from 1900) */
	yyyy=atoi(date);
	myTm.tm_year = yyyy-1900;

	/* set remaining fields */
	myTm.tm_sec=0;
	myTm.tm_min=0;
	myTm.tm_hour=12;
	myTm.tm_wday = -1;
	myTm.tm_yday = -1;
	myTm.tm_isdst = -1;

	/* set day increment */
	days = atoi(argv[2]);

	/* convert day increment to time_t offset */
	daysInSeconds = days*60*60*24;

	/* convert struct tm to time_t */
	timeInSeconds = mktime(&myTm);

	/* increment time_t value by number of days */
	timeInSeconds += daysInSeconds;

	/* print result - replaced cftime with strftime on 20120801 */
	strftime(date, sizeof(date), "%Y%m%d", localtime(&timeInSeconds) );
	puts(date);

	return 0;
};
