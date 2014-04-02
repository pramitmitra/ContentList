#!/usr/bin/ksh -eu

if [[ $# != 2 ]]
then
        echo "Usage:  $0 <src_server> <code_list_file>"
        exit 4
fi
_src=$1
_clf=$2

while read  _dir _lcldir _file
do
    lcldir=$(eval echo $_lcldir)
  cd $lcldir
  scp bwenner@${_src}:${_dir}/$_file .
done < $_clf

