#!/bin/bash

PWD=`pwd`
CMD=$0
PREPATH=${CMD%/*}
EXTERNAL=${PWD%/*}
CONFIGURE=$PREPATH/configure
ARGS="CC=\"$CC\" CXX=\"$CXX\" --prefix=$EXTERNAL --disable-shared --enable-unicode-properties"

while [ -n "$(echo $1)" ]; do
	case $1 in
		--enable-64bit ) ;;
	esac
	shift
done

#echo "$CONFIGURE $ARGS"
if [ -f config.status ]; then
	TMP=""
else
	eval $CONFIGURE $ARGS
	eval make
	eval make install
fi
