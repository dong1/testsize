
#
# Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution. 
#
#   This program is free software; you can redistribute it and/or modify 
#   it under the terms of the GNU General Public License as published by 
#   the Free Software Foundation; either version 2 of the License, or 
#   (at your option) any later version. 
#
#  This program is distributed in the hope that it will be useful, 
#  but WITHOUT ANY WARRANTY; without even the implied warranty of 
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the 
#  GNU General Public License for more details. 
#
#  You should have received a copy of the GNU General Public License 
#  along with this program; if not, write to the Free Software 
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#


ko_KR.utf8dir = @CUBRID_LOCALEDIR@/ko_KR.utf8
ko_KR.utf8_DATA = cubrid.msg cubrid.cat csql.msg csql.cat utils.msg utils.cat \
	esql.msg esql.cat syntax.txt

GENCAT = $(top_builddir)/util/gencat

SUFFIXES = .msg.cat

.msg.cat:
	$(GENCAT) $@ $<
