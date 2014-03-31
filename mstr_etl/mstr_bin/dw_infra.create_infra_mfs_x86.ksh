#!/bin/ksh -eux
#------------------------------------------------------------------------------------------------
# Filename:     dw_infra.create_infra_mfs_x86.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#
#------------------------------------------------------------------------------------------------

ARGC=$#
if [ $ARGC -lt 1 -o $ARGC -gt 2 ]
then
	print "usage: ${0##*/} <etl env> [owner id]"
	exit 1
fi

ETL_ENV=$1
HOST_ALIAS=${2:-}
CHOWN_ID=${3:-}

export AB_HOME=/usr/local/abinitio
DATA_DIR=/dw/etl
BASE_DIR=/dw/etl/home
HOME_DIR=$BASE_DIR/$ETL_ENV
MFS_DIR=$DATA_DIR/mfs_home/$ETL_ENV
RUN_DIR=`dirname $0`

function createtargetdirs {
	_mfsver=$1

        $AB_HOME/bin/m_mkdir //$HOST_ALIAS/$MFS_DIR/$_mfsver/in
        $AB_HOME/bin/m_mkdir //$HOST_ALIAS/$MFS_DIR/$_mfsver/in/extract
        $AB_HOME/bin/m_mkdir //$HOST_ALIAS/$MFS_DIR/$_mfsver/tmp
        $AB_HOME/bin/m_mkdir //$HOST_ALIAS/$MFS_DIR/$_mfsver/tmp/extract

        while read TARGET
        do
            if [[ $TARGET != primary && $TARGET != secondary ]]
            then  
                ln -s $MFS_DIR/$_mfsver/in/extract $MFS_DIR/$_mfsver/in/$TARGET
                $AB_HOME/bin/m_mkdir //$HOST_ALIAS/$MFS_DIR/$_mfsver/tmp/$TARGET
            elif [ $TARGET == primary ]
            then
                ln -s $MFS_DIR/$_mfsver/in/td1 $MFS_DIR/$_mfsver/in/$TARGET
                ln -s $MFS_DIR/$_mfsver/tmp/td1 $MFS_DIR/$_mfsver/tmp/$TARGET
            elif [ $TARGET == secondary ] 
            then
                ln -s $MFS_DIR/$_mfsver/in/td2 $MFS_DIR/$_mfsver/in/$TARGET       
                ln -s $MFS_DIR/$_mfsver/tmp/td2 $MFS_DIR/$_mfsver/tmp/$TARGET
            fi
        done < $RUN_DIR/create_infra_env_x86.targets.lis
}

# Remove any mfs directories that exist. We will rebuild from scratch.
rm -fR $MFS_DIR/fs02
rm -fR $MFS_DIR/fs04
rm -fR $MFS_DIR/fs08
rm -fR $MFS_DIR/fs12
rm -fR $MFS_DIR/fs16
rm -fR $MFS_DIR/fs20
rm -fR $MFS_DIR/fs24

rm -fR /dw/etl/mfs01/$ETL_ENV/part01/
rm -fR /dw/etl/mfs02/$ETL_ENV/part02/
rm -fR /dw/etl/mfs03/$ETL_ENV/part03/
rm -fR /dw/etl/mfs04/$ETL_ENV/part04/
rm -fR /dw/etl/mfs01/$ETL_ENV/part05/
rm -fR /dw/etl/mfs02/$ETL_ENV/part06/
rm -fR /dw/etl/mfs03/$ETL_ENV/part07/
rm -fR /dw/etl/mfs04/$ETL_ENV/part08/
rm -fR /dw/etl/mfs01/$ETL_ENV/part09/
rm -fR /dw/etl/mfs02/$ETL_ENV/part10/
rm -fR /dw/etl/mfs03/$ETL_ENV/part11/
rm -fR /dw/etl/mfs04/$ETL_ENV/part12/
rm -fR /dw/etl/mfs01/$ETL_ENV/part13/
rm -fR /dw/etl/mfs02/$ETL_ENV/part14/
rm -fR /dw/etl/mfs03/$ETL_ENV/part15/
rm -fR /dw/etl/mfs04/$ETL_ENV/part16/
rm -fR /dw/etl/mfs01/$ETL_ENV/part17/
rm -fR /dw/etl/mfs02/$ETL_ENV/part18/
rm -fR /dw/etl/mfs03/$ETL_ENV/part19/
rm -fR /dw/etl/mfs04/$ETL_ENV/part20/
rm -fR /dw/etl/mfs01/$ETL_ENV/part21/
rm -fR /dw/etl/mfs02/$ETL_ENV/part22/
rm -fR /dw/etl/mfs03/$ETL_ENV/part23/
rm -fR /dw/etl/mfs04/$ETL_ENV/part24/

mkdir -p /dw/etl/mfs01/$ETL_ENV/part01
mkdir -p /dw/etl/mfs02/$ETL_ENV/part02
mkdir -p /dw/etl/mfs03/$ETL_ENV/part03
mkdir -p /dw/etl/mfs04/$ETL_ENV/part04
mkdir -p /dw/etl/mfs01/$ETL_ENV/part05
mkdir -p /dw/etl/mfs02/$ETL_ENV/part06
mkdir -p /dw/etl/mfs03/$ETL_ENV/part07
mkdir -p /dw/etl/mfs04/$ETL_ENV/part08
mkdir -p /dw/etl/mfs01/$ETL_ENV/part09
mkdir -p /dw/etl/mfs02/$ETL_ENV/part10
mkdir -p /dw/etl/mfs03/$ETL_ENV/part11
mkdir -p /dw/etl/mfs04/$ETL_ENV/part12
mkdir -p /dw/etl/mfs01/$ETL_ENV/part13
mkdir -p /dw/etl/mfs02/$ETL_ENV/part14
mkdir -p /dw/etl/mfs03/$ETL_ENV/part15
mkdir -p /dw/etl/mfs04/$ETL_ENV/part16
mkdir -p /dw/etl/mfs01/$ETL_ENV/part17
mkdir -p /dw/etl/mfs02/$ETL_ENV/part18
mkdir -p /dw/etl/mfs03/$ETL_ENV/part19
mkdir -p /dw/etl/mfs04/$ETL_ENV/part20
mkdir -p /dw/etl/mfs01/$ETL_ENV/part21
mkdir -p /dw/etl/mfs02/$ETL_ENV/part22
mkdir -p /dw/etl/mfs03/$ETL_ENV/part23
mkdir -p /dw/etl/mfs04/$ETL_ENV/part24

$AB_HOME/bin/m_mkfs //$HOST_ALIAS/$MFS_DIR/fs02 \
	//$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part01/fs02 \
    	//$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part02/fs02

createtargetdirs fs02


$AB_HOME/bin/m_mkfs //$HOST_ALIAS/$MFS_DIR/fs04 \
    //$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part01/fs04 \
    //$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part02/fs04 \
    //$HOST_ALIAS/$DATA_DIR/mfs03/$ETL_ENV/part03/fs04 \
    //$HOST_ALIAS/$DATA_DIR/mfs04/$ETL_ENV/part04/fs04

createtargetdirs fs04

$AB_HOME/bin/m_mkfs //$HOST_ALIAS/$MFS_DIR/fs08 \
    //$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part01/fs08 \
    //$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part02/fs08 \
    //$HOST_ALIAS/$DATA_DIR/mfs03/$ETL_ENV/part03/fs08 \
    //$HOST_ALIAS/$DATA_DIR/mfs04/$ETL_ENV/part04/fs08 \
    //$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part05/fs08 \
    //$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part06/fs08 \
    //$HOST_ALIAS/$DATA_DIR/mfs03/$ETL_ENV/part07/fs08 \
    //$HOST_ALIAS/$DATA_DIR/mfs04/$ETL_ENV/part08/fs08

createtargetdirs fs08

$AB_HOME/bin/m_mkfs //$HOST_ALIAS/$MFS_DIR/fs12 \
    //$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part01/fs12 \
    //$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part02/fs12 \
    //$HOST_ALIAS/$DATA_DIR/mfs03/$ETL_ENV/part03/fs12 \
    //$HOST_ALIAS/$DATA_DIR/mfs04/$ETL_ENV/part04/fs12 \
    //$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part05/fs12 \
    //$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part06/fs12 \
    //$HOST_ALIAS/$DATA_DIR/mfs03/$ETL_ENV/part07/fs12 \
    //$HOST_ALIAS/$DATA_DIR/mfs04/$ETL_ENV/part08/fs12 \
    //$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part09/fs12 \
    //$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part10/fs12 \
    //$HOST_ALIAS/$DATA_DIR/mfs03/$ETL_ENV/part11/fs12 \
    //$HOST_ALIAS/$DATA_DIR/mfs04/$ETL_ENV/part12/fs12 \
    //$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part13/fs12 \
    //$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part14/fs12 \
    //$HOST_ALIAS/$DATA_DIR/mfs03/$ETL_ENV/part15/fs12 \
    //$HOST_ALIAS/$DATA_DIR/mfs04/$ETL_ENV/part16/fs12

createtargetdirs fs12

$AB_HOME/bin/m_mkfs //$HOST_ALIAS/$MFS_DIR/fs16 \
    //$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part01/fs16 \
    //$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part02/fs16 \
    //$HOST_ALIAS/$DATA_DIR/mfs03/$ETL_ENV/part03/fs16 \
    //$HOST_ALIAS/$DATA_DIR/mfs04/$ETL_ENV/part04/fs16 \
    //$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part05/fs16 \
    //$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part06/fs16 \
    //$HOST_ALIAS/$DATA_DIR/mfs03/$ETL_ENV/part07/fs16 \
    //$HOST_ALIAS/$DATA_DIR/mfs04/$ETL_ENV/part08/fs16 \
    //$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part09/fs16 \
    //$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part10/fs16 \
    //$HOST_ALIAS/$DATA_DIR/mfs03/$ETL_ENV/part11/fs16 \
    //$HOST_ALIAS/$DATA_DIR/mfs04/$ETL_ENV/part12/fs16 \
    //$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part13/fs16 \
    //$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part14/fs16 \
    //$HOST_ALIAS/$DATA_DIR/mfs03/$ETL_ENV/part15/fs16 \
    //$HOST_ALIAS/$DATA_DIR/mfs04/$ETL_ENV/part16/fs16

createtargetdirs fs16

$AB_HOME/bin/m_mkfs //$HOST_ALIAS/$MFS_DIR/fs20 \
	//$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part01/fs20 \
    	//$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part02/fs20 \
    	//$HOST_ALIAS/$DATA_DIR/mfs03/$ETL_ENV/part03/fs20 \
    	//$HOST_ALIAS/$DATA_DIR/mfs04/$ETL_ENV/part04/fs20 \
    	//$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part05/fs20 \
    	//$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part06/fs20 \
    	//$HOST_ALIAS/$DATA_DIR/mfs03/$ETL_ENV/part07/fs20 \
    	//$HOST_ALIAS/$DATA_DIR/mfs04/$ETL_ENV/part08/fs20 \
    	//$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part09/fs20 \
    	//$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part10/fs20 \
    	//$HOST_ALIAS/$DATA_DIR/mfs03/$ETL_ENV/part11/fs20 \
    	//$HOST_ALIAS/$DATA_DIR/mfs04/$ETL_ENV/part12/fs20 \
    	//$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part13/fs20 \
   	 	//$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part14/fs20 \
    	//$HOST_ALIAS/$DATA_DIR/mfs03/$ETL_ENV/part15/fs20 \
    	//$HOST_ALIAS/$DATA_DIR/mfs04/$ETL_ENV/part16/fs20 \
    	//$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part17/fs20 \
    	//$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part18/fs20 \
    	//$HOST_ALIAS/$DATA_DIR/mfs03/$ETL_ENV/part19/fs20 \
	    //$HOST_ALIAS/$DATA_DIR/mfs04/$ETL_ENV/part20/fs20

createtargetdirs fs20	
	
$AB_HOME/bin/m_mkfs //$HOST_ALIAS/$MFS_DIR/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part01/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part02/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs03/$ETL_ENV/part03/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs04/$ETL_ENV/part04/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part05/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part06/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs03/$ETL_ENV/part07/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs04/$ETL_ENV/part08/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part09/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part10/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs03/$ETL_ENV/part11/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs04/$ETL_ENV/part12/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part13/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part14/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs03/$ETL_ENV/part15/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs04/$ETL_ENV/part16/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part17/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part18/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs03/$ETL_ENV/part19/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs04/$ETL_ENV/part20/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs01/$ETL_ENV/part21/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs02/$ETL_ENV/part22/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs03/$ETL_ENV/part23/fs24 \
	    //$HOST_ALIAS/$DATA_DIR/mfs04/$ETL_ENV/part24/fs24
	
createtargetdirs fs24

if [ $CHOWN_ID ]
then
	chown -fR $CHOWN_ID /dw/etl/mfs01/$ETL_ENV/
	chown -fR $CHOWN_ID /dw/etl/mfs02/$ETL_ENV/
	chown -fR $CHOWN_ID /dw/etl/mfs03/$ETL_ENV/
	chown -fR $CHOWN_ID /dw/etl/mfs04/$ETL_ENV/
fi

exit 0
