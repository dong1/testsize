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


#include "stdafx.h"
#include <stdio.h>
#include <wtypes.h>
#include <winnt.h>
#include <winsvc.h>
#include <winuser.h>
#include <windows.h>

#include <string.h>
#include <time.h>
#include <stdarg.h>
#include <io.h>
#include <Tlhelp32.h>
#include <sys/stat.h>

static int
proc_execute (const char *file, char *args[], bool wait_child,
	      bool close_output, int *out_pid);

void WriteLog( char* p_logfile, char* p_format, ... );
void GetCurDateTime( char* p_buf, char* p_form );
void SendMessage_Tray(int status);

void vKingCHStart(DWORD argc, LPTSTR* argv);
void vHandler(DWORD opcode);
void vSetStatus(DWORD dwState, DWORD dwAccept = SERVICE_ACCEPT_STOP|SERVICE_ACCEPT_PAUSE_CONTINUE);
void SetCUBRIDEnvVar();
SERVICE_STATUS_HANDLE g_hXSS; //���� ȯ�� �۷ι� �ڵ�
DWORD	g_XSS;    //������ ���� ���¸� �����ϴ� ����
BOOL    g_bPause; //���񽺰� �����ΰ� �ƴѰ�
HANDLE	g_hExitEvent; //���񽺸� ���� ��ų�� �̺�Ʈ�� ����Ͽ� �����带 �����Ѵ�
BOOL g_isRunning = false;
#define		WM_SERVICE_STOP		WM_USER+1
#define		WM_SERVICE_START	WM_USER+2

#define		SERVICE_STATUS_STOP  0
#define 	SERVICE_STATUS_START 1

char sLogFile[256] = "CUBRIDService.log";

int checkCmautoProcess(int pid)
{
    HANDLE        hModuleSnap = NULL;
    MODULEENTRY32 me32        = {0};
    hModuleSnap = CreateToolhelp32Snapshot(TH32CS_SNAPMODULE, pid);

	if (hModuleSnap == (HANDLE)-1)
        return -1;

    me32.dwSize = sizeof(MODULEENTRY32);

    if(Module32First(hModuleSnap, &me32)) {
       do {
          if(_stricmp((const char*)me32.szModule, "cub_auto.exe") == 0) {
             CloseHandle (hModuleSnap); 
             return 1;
          }
       } while(Module32Next(hModuleSnap, &me32));
    }
    CloseHandle (hModuleSnap);
    return 0;
}

#if defined (ENABLE_UNUSED_FUNCTION)
BOOL checkCmauto()
{
	char *envCMHome;
	char pidFile[1024];
	int  cmautoPid;
	FILE *fpCmautoPid;

	envCMHome = getenv("CUBRID");
	if (!envCMHome) return FALSE;

	sprintf(pidFile, "%s\\logs\\cub_auto.pid", envCMHome);

	for (int i=0 ; i<20 ; i++) {
		if (_access(pidFile, 0 /* F_OK */) == -1) {
			Sleep(500);
		}
		else {
			fpCmautoPid = fopen(pidFile, "r");
			if (fpCmautoPid) {
				fscanf(fpCmautoPid, "%d", &cmautoPid);
				fclose(fpCmautoPid);

				if (checkCmautoProcess(cmautoPid) != 0) {
					return TRUE;
				}
			}
			Sleep(500);
		}
	}

	return FALSE;
}
#endif /* ENABLE_UNUSED_FUNCTION */

int main(int argc, char* argv[])
{
	SetCUBRIDEnvVar();

	SERVICE_TABLE_ENTRY stbl[] = 
	{
		{"CUBRIDService", (LPSERVICE_MAIN_FUNCTION)vKingCHStart },
		{NULL, NULL}
	};

	if(!StartServiceCtrlDispatcher(stbl))
	{
		WriteLog(sLogFile,"StartServiceCtrlDispatcher : error (%d)\n", GetLastError() );
		return 0;
	}

	return 1;
}

void vKingCHStart(DWORD argc, LPTSTR* argv)
{
	char * args[5];
	char command[100];

	g_hXSS = RegisterServiceCtrlHandlerA("CUBRIDService",
		(LPHANDLER_FUNCTION)vHandler);

	if(g_hXSS ==0)
	{
		WriteLog(sLogFile,"RegisterServiceCtrlHandlerA : error (%d)\n", GetLastError() );
		return ;
	}

	vSetStatus(SERVICE_START_PENDING);
	g_bPause = FALSE;

	g_hExitEvent = CreateEventA(NULL, TRUE, FALSE, "XServiceExitEvent");

	SendMessage_Tray(SERVICE_STATUS_START);

	sprintf(command,"%s\\bin\\cubrid.exe", getenv("CUBRID"));

	args[0] = command;
	args[1] = "service";
	args[2] = "start";
	args[3] = "--for-windows-service";
	args[4] = NULL;

	proc_execute (command, args, true, true, NULL);

	vSetStatus(SERVICE_RUNNING);

	g_isRunning = true;

	while (1)
    {
		Sleep(2000);

		if ( !g_isRunning )
        {
            break;
        }
    }

	vSetStatus(SERVICE_STOPPED);
}

void vSetStatus(DWORD dwState, DWORD dwAccept)
{
	SERVICE_STATUS ss;

	ss.dwServiceType				= SERVICE_WIN32_OWN_PROCESS;
	ss.dwCurrentState				= dwState;
	ss.dwControlsAccepted			= dwAccept;
	ss.dwWin32ExitCode				= 0;
	ss.dwServiceSpecificExitCode	= 0;
	ss.dwCheckPoint					= 0;
	ss.dwWaitHint					= 0;

	//���� ���� ����
	g_XSS = dwState;
	SetServiceStatus(g_hXSS, &ss);
}	

void vHandler(DWORD opcode)
{
	if(opcode == g_XSS)
	{
		return;
	}

	switch(opcode)
	{
	case SERVICE_CONTROL_PAUSE:
		vSetStatus(SERVICE_PAUSE_PENDING,0);
		g_bPause = TRUE;
		vSetStatus(SERVICE_PAUSED);
		break;
	case SERVICE_CONTROL_CONTINUE:
		vSetStatus(SERVICE_CONTINUE_PENDING, 0);
		g_bPause = FALSE;
		vSetStatus(SERVICE_RUNNING);
		break;
	case SERVICE_CONTROL_STOP:
		{
			char * args[5];
			char command[100];

			SendMessage_Tray(SERVICE_STATUS_STOP);
			vSetStatus(SERVICE_STOP_PENDING, 0);

			sprintf(command,"%s\\bin\\cubrid.exe", getenv("CUBRID"));

			args[0] = command;
			args[1] = "service";
			args[2] = "stop";
			args[3] = "--for-windows-service";
			args[4] = NULL;

			proc_execute (command, args, true, true, NULL);

			g_isRunning = false;
			
			//�����带 �������̸� �����
			SetEvent(g_hExitEvent);
			vSetStatus(SERVICE_STOPPED);
		}
		break;
	case SERVICE_CONTROL_INTERROGATE:
	default:
		vSetStatus(g_XSS);
		break;
	}
}

void WriteLog( char* p_logfile, char* p_format, ... ) 
{
	va_list str;
	char    old_logfile[256];
	char	cur_time[25];
	FILE*   logfile_fd;
	struct  _stat stat_buf;

#define _MAX_LOGFILE_SIZE_	102400
	
	if (p_logfile != NULL)
	{
		if ((_stat(p_logfile, &stat_buf) == 0) &&
			(stat_buf.st_size >= _MAX_LOGFILE_SIZE_) )
		{
			strcpy_s(old_logfile, p_logfile );
			strcat_s(old_logfile, ".bak" );

			remove(old_logfile);

			if (rename( p_logfile, old_logfile ) != 0)
			{
				fprintf(stderr,"WriteLog:rename error\n");
				return;
			}
		}

		fopen_s(&logfile_fd, p_logfile, "a+");
		
		if ( logfile_fd == NULL )
		{
			fprintf(stderr,"WriteLog:Can't open logfile [%s][%d]\n",
				p_logfile, errno );
			return;
		}
	}
	else
	{
		logfile_fd = stderr;
	}

#ifndef __DEBUG
	GetCurDateTime( cur_time,"%Y%m%d %H:%M:%S" );
	fprintf( logfile_fd, "[%s] ", cur_time );
#endif

	va_start( str, p_format );
	vfprintf( logfile_fd, p_format, str );
	va_end( str );

	if( p_logfile != NULL )
		fclose( logfile_fd );
}

void GetCurDateTime( char* p_buf, char* p_form )
{
	time_t c_time;
	struct tm* l_time = NULL;


	time( &c_time );

	l_time = localtime( &c_time );

	strftime( p_buf, 24 , p_form, l_time );
	
}

void SetCUBRIDEnvVar()
{
#define BUF_LENGTH 1024

	DWORD dwBufLength = BUF_LENGTH;
	TCHAR sEnvCUBRID[BUF_LENGTH];
	TCHAR sEnvCUBRID_CAS[BUF_LENGTH];
	TCHAR sEnvCUBRID_MANAGER[BUF_LENGTH];
	TCHAR sEnvCUBRID_DATABASES[BUF_LENGTH];
	TCHAR sEnvCUBRID_LANG[BUF_LENGTH];
	TCHAR sEnvCUBRID_MODE[BUF_LENGTH];
	TCHAR sEnvPath[BUF_LENGTH];

	char szKey[BUF_LENGTH] = "SYSTEM\\CurrentControlSet\\Control\\Session Manager\\Environment";
	char EnvString[BUF_LENGTH];
	HKEY hKey;
	LONG nResult;

	nResult = RegOpenKeyExA(HKEY_LOCAL_MACHINE, szKey, 0, KEY_QUERY_VALUE, &hKey);
	if (nResult != ERROR_SUCCESS) return;

#ifdef _DEBUG
	FILE *debugfd = fopen("C:\\CUBRIDService.log", "w+");
#endif

	dwBufLength = BUF_LENGTH;
	nResult = RegQueryValueEx(hKey, TEXT("CUBRID"), NULL, NULL, (LPBYTE)sEnvCUBRID, &dwBufLength);
	if (nResult == ERROR_SUCCESS) {
		// set CUBRID Environment variable.
		strcpy(EnvString, "CUBRID=");
		strcat(EnvString, (const char*)sEnvCUBRID);
		_putenv(EnvString);

#ifdef _DEBUG
		if (debugfd) {
			fprintf(debugfd, "$CUBRID = %s\n", getenv("CUBRID"));
		}
#endif
	}

	dwBufLength = BUF_LENGTH;
	nResult = RegQueryValueEx(hKey, TEXT("CUBRID_DATABASES"), NULL, NULL, (LPBYTE)sEnvCUBRID_DATABASES, &dwBufLength);
	if (nResult == ERROR_SUCCESS) {
		// set CUBRID Environment variable.
		strcpy(EnvString, "CUBRID_DATABASES=");
		strcat(EnvString, sEnvCUBRID_DATABASES);
		_putenv(EnvString);
#ifdef _DEBUG
		if (debugfd) {
			fprintf(debugfd, "$CUBRID_DATABASES = %s\n", getenv("CUBRID_DATABASES"));
		}
#endif
	}

	dwBufLength = BUF_LENGTH;
	nResult = RegQueryValueEx(hKey, TEXT("CUBRID_MODE"), NULL, NULL, (LPBYTE)sEnvCUBRID_MODE, &dwBufLength);
	if (nResult == ERROR_SUCCESS) {
		// set CUBRID Environment variable.
		strcpy(EnvString, "CUBRID_MODE=");
		strcat(EnvString, sEnvCUBRID_MODE);
		_putenv(EnvString);
#ifdef _DEBUG
		if (debugfd) {
			fprintf(debugfd, "$CUBRID_MODE = %s\n", getenv("CUBRID_MODE"));
		}
#endif
	}

	dwBufLength = BUF_LENGTH;
	nResult = RegQueryValueEx(hKey, TEXT("CUBRID_LANG"), NULL, NULL, (LPBYTE)sEnvCUBRID_LANG, &dwBufLength);
	if (nResult == ERROR_SUCCESS) {
		// set CUBRID Environment variable.
		strcpy(EnvString, "CUBRID_LANG=");
		strcat(EnvString, sEnvCUBRID_LANG);
		_putenv(EnvString);
#ifdef _DEBUG
		if (debugfd) {
			fprintf(debugfd, "$CUBRID_LANG = %s\n", getenv("CUBRID_LANG"));
		}
#endif
	}

	dwBufLength = BUF_LENGTH;
	nResult = RegQueryValueEx(hKey, TEXT("Path"), NULL, NULL, (LPBYTE)sEnvPath, &dwBufLength);
	if (nResult == ERROR_SUCCESS) {
		// set CUBRID Environment variable.
		strcpy(EnvString, "Path=");
		strcat(EnvString, sEnvPath);
		_putenv(EnvString);
#ifdef _DEBUG
		if (debugfd) {
			fprintf(debugfd, "Path = %s\n", getenv("Path"));
		}
#endif
	}

#ifdef _DEBUG
	if (debugfd) fclose(debugfd);
#endif

	RegCloseKey(hKey);
}

void SendMessage_Tray(int status)
{
	HWND hTrayWnd;
	hTrayWnd = FindWindowA("cubrid_tray", "cubrid_tray");

	if (hTrayWnd == NULL){
		return;
	}

	if (status == SERVICE_STATUS_STOP) {
		PostMessage(hTrayWnd, WM_SERVICE_STOP, NULL, NULL);
	}
	else if (status == SERVICE_STATUS_START) {
		PostMessage(hTrayWnd, WM_SERVICE_START, NULL, NULL);
	}
}

static int
proc_execute (const char *file, char *args[], bool wait_child,
	      bool close_output, int *out_pid)
{
  STARTUPINFO si;
  PROCESS_INFORMATION pi;
  int i, cmd_arg_len;
  char cmd_arg[1024];
  int ret_code = 0;
  bool inherited_handle = TRUE;

  if (out_pid)
    {
      *out_pid = 0;
    }

  for (i = 0, cmd_arg_len = 0; args[i]; i++)
    {
      cmd_arg_len += sprintf (cmd_arg + cmd_arg_len, "\"%s\" ", args[i]);
    }

  GetStartupInfo (&si);
  if (close_output)
    {
      si.dwFlags = si.dwFlags | STARTF_USESTDHANDLES | STARTF_USESHOWWINDOW;
      si.hStdOutput = NULL;
      si.hStdError = NULL;
      inherited_handle = FALSE;
	  si.wShowWindow = SW_HIDE;
    }

  if (!CreateProcess (file, cmd_arg, NULL, NULL, inherited_handle,
		      0, NULL, NULL, &si, &pi))
    {
      return -1;
    }

  if (wait_child)
    {
      DWORD status = 0;
      WaitForSingleObject (pi.hProcess, INFINITE);
      GetExitCodeProcess (pi.hProcess, &status);
      ret_code = status;
    }
  else
    {
      if (out_pid)
	{
	  *out_pid = pi.dwProcessId;
	}
    }

  CloseHandle (pi.hProcess);
  CloseHandle (pi.hThread);
  return ret_code;
}
