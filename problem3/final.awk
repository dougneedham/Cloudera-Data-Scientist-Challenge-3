#!/bin/awk -f

BEGIN { FS ="," }
{print $1","$VERTEX","$2;}

