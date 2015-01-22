#!/bin/awk -f

BEGIN { FS ="," }
{print $1","$2","$3 ;}

