#!/bin/ksh -eu

NEW_DATE=$($DW_EXE/add_days $(date '+%Y%m%d') -1)
  MONTH_DAY=${NEW_DATE#????}
  print ${NEW_DATE%????}-${MONTH_DAY%??}-${NEW_DATE#??????} "00:00:00"
exit
