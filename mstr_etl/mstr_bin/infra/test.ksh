#!/usr/bin/ksh -eu
# Title:        dw_infra.hadr_extract_cleanup.ksh
# File Name:    dw_infra.hadr_extract_cleanup.ksh
# Description:  Clean up Files on non-production nodes
# Developer:    Brian Wenner
# Created on:
# Location:     $DW_EXE
#
# Execution:    $DW_EXE/shell_handler.ksh dw_infra.cleanup td1 $DW_MASTER_BIN/dw_infra.hadr_extract_cleanup.ksh
#
# Parameters:   none
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# Brian Wenner     11/03/2010      Initial Creation
#------------------------------------------------------------------------------------------------


echo "####################################################################################"
echo "#"
echo "# Beginning cleanup process for data files  `date`"
echo "#"
echo "####################################################################################"
echo ""
. /dw/etl/mstr_cfg/etlenv.setup

