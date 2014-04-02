df -k $DW_IN/extract
ls -d $DW_IN/extract/* | while read mydir
do
  echo "removing files older than 30 days from directory: $mydir"
  find $mydir -mtime +30 -exec rm -f {} \;
done
df -k $DW_IN/extract
