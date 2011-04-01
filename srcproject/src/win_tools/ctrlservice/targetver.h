/*
 * Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution. 
 *
 *   This program is free software; you can redistribute it and/or modify 
 *   it under the terms of the GNU General Public License as published by 
 *   the Free Software Foundation; version 2 of the License. 
 *
 *  This program is distributed in the hope that it will be useful, 
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of 
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the 
 *  GNU General Public License for more details. 
 *
 *  You should have received a copy of the GNU General Public License 
 *  along with this program; if not, write to the Free Software 
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
 *
 */

#pragma once

// ���� ��ũ�δ� �ʿ��� �ּ� �÷����� �����մϴ�. �ʿ��� �ּ� �÷�����
// ���� ���α׷��� �����ϴ� �� �ʿ��� ����� ���Ե� ���� ���� ������ Windows, Internet Explorer
// ���Դϴ�. �� ��ũ�δ� ������ ���� �̻��� �÷��� �������� ��� ������ ��� ����� Ȱ��ȭ�ؾ�
// �۵��մϴ�.

// �Ʒ� ������ �÷����� �켱�ϴ� �÷����� ������� �ϴ� ��� ���� ���Ǹ� �����Ͻʽÿ�.
// �ٸ� �÷����� ���Ǵ� �ش� ���� �ֽ� ������ MSDN�� �����Ͻʽÿ�.
#ifndef _WIN32_WINNT            // �ʿ��� �ּ� �÷����� Windows Vista�� �����մϴ�.
#define _WIN32_WINNT 0x0600     // �ٸ� ������ Windows�� �µ��� ������ ������ ������ �ֽʽÿ�.
#endif

