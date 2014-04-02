#include <stdio.h>
#include <string.h>
#include <time.h>

static int daysinmonth[2][13] = { { 0,31,28,31,30,31,30,31,31,30,31,30,31 },
                                  { 0,31,29,31,30,31,30,31,31,30,31,30,31 }  }; 

main(int argc, char *argv[])
{
	char date[9];
	struct tm myTm;
	int yyyy,months,years,leap;
	int argsRqrd = 2;

	/* check parameters */
	if (argc != argsRqrd+1 || strlen(argv[1]) != 8)
	{
		fprintf(stderr, "Incorrect Usage: %d parameters required (yyyymmdd incmonths)\n", argsRqrd);
		return 1;
	}

	strcpy(date,argv[1]);

	/* set day of month */
	myTm.tm_mday = atoi(date+6);
	date[6]=0;

	/* set month */
	myTm.tm_mon = atoi(date+4);
	date[4]=0;

	/* set year */
	yyyy=atoi(date);
	myTm.tm_year = yyyy;

	/* set remaining fields */
	myTm.tm_sec=0;
	myTm.tm_min=0;
	myTm.tm_hour=0;
	myTm.tm_wday = -1;
	myTm.tm_yday = -1;
	myTm.tm_isdst = -1;

	/* set month increment */
	months = atoi(argv[2]);
	years = months/12;
	months = months%12;

	/* set year */
	myTm.tm_year += years;

	/* set month, and adjust year as necessary */
	if ( myTm.tm_mon + months < 1 )
	{
		myTm.tm_year = myTm.tm_year - 1;
		myTm.tm_mon = myTm.tm_mon + months + 12;
	}
	else if ( myTm.tm_mon + months > 12 )
	{
		myTm.tm_year = myTm.tm_year + 1;
		myTm.tm_mon = myTm.tm_mon + months - 12;
	}
	else 
		myTm.tm_mon= myTm.tm_mon + months;

	leap = myTm.tm_year%4 ==0 && myTm.tm_year%100 !=0 ||  myTm.tm_year%400 == 0; 


	if ( myTm.tm_mday > daysinmonth[leap][myTm.tm_mon]  )
	{
		myTm.tm_mday = daysinmonth[leap][myTm.tm_mon] ;
	}

	/* print result */
	sprintf(date, "%02d%02d%02d", myTm.tm_year, myTm.tm_mon, myTm.tm_mday );
	puts(date);

	return 0;
};
