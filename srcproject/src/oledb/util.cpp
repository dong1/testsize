/*
 * Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution. 
 *
 * Redistribution and use in source and binary forms, with or without modification, 
 * are permitted provided that the following conditions are met: 
 *
 * - Redistributions of source code must retain the above copyright notice, 
 *   this list of conditions and the following disclaimer. 
 *
 * - Redistributions in binary form must reproduce the above copyright notice, 
 *   this list of conditions and the following disclaimer in the documentation 
 *   and/or other materials provided with the distribution. 
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors 
 *   may be used to endorse or promote products derived from this software without 
 *   specific prior written permission. 
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND 
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, 
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, 
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY 
 * OF SUCH DAMAGE. 
 *
 */

#include "stdafx.h"
#include "util.h"
#include "type.h"
#include "DataSource.h"
#include <atltime.h>

void show_error(LPSTR msg, int code, T_CCI_ERROR *error)
{
	ATLTRACE(atlTraceDBProvider, 2, "Error : %s [code : %d]", msg, code);
    if (code == CCI_ER_DBMS)
	{
		ATLTRACE(atlTraceDBProvider, 2, "DBMS Error : %s [code : %d]",
			(LPSTR)error->err_msg, error->err_code);
    }
}

namespace Util {

//DBROWCOUNT FindBookmark(const CAtlArray<DBROWCOUNT> &rgBookmarks, DBROWCOUNT iRowset)
//{
//	for(DBROWCOUNT i=0;i<(DBROWCOUNT)rgBookmarks.GetCount();i++)
//	{
//		if(rgBookmarks[i]==iRowset)
//			return i;
//	}
//	return 0;
//
//	// TODO: �ϸ�ũ�� ������������ ���ĵǾ� ���� ������ ������ �߻��� �� ����. see Rowset.h
//	//DBROWCOUNT bm = iRowset+2;
//	//DBROWCOUNT cnt = (DBROWCOUNT)rgBookmarks.GetCount();
//	//while(bm<cnt && rgBookmarks[bm]!=iRowset)
//	//{
//	//	if(rgBookmarks[bm]<iRowset)
//	//		bm++;
//	//	else
//	//		bm--;
//	//}
//	//if(bm>=cnt) return 0;
//	//return bm;
//}

HRESULT Connect(IDBProperties *pDBProps, int *phConn)
{
	ATLASSERT(phConn!=NULL);
	*phConn = 0;

	int cas_port;
	{
		CDBPropIDSet set(DBPROPSET_UNIPROVIDER_DBINIT);
		set.AddPropertyID(DBPROP_UNIPROVIDER_UNICAS_PORT);

		ULONG cPropSet = 0;
		CComHeapPtr<DBPROPSET> rgPropSet;
		HRESULT hr = pDBProps->GetProperties(1, &set, &cPropSet, &rgPropSet);
		if(FAILED(hr)) return hr;

		cas_port = V_I4(&rgPropSet->rgProperties[0].vValue);

		VariantClear(&(rgPropSet->rgProperties[0].vValue));
		CoTaskMemFree(rgPropSet->rgProperties);
	}

	char lo[512], ds[512], id[512], pw[4096];
	{
		// Properties to read
		CDBPropIDSet set(DBPROPSET_DBINIT);
		set.AddPropertyID(DBPROP_INIT_LOCATION);
		set.AddPropertyID(DBPROP_INIT_DATASOURCE);
		set.AddPropertyID(DBPROP_AUTH_USERID);
		set.AddPropertyID(DBPROP_AUTH_PASSWORD);
		
		// Read properties
		ULONG cPropSet = 0;
		CComHeapPtr<DBPROPSET> rgPropSet;
		HRESULT hr = pDBProps->GetProperties(1, &set, &cPropSet, &rgPropSet);
		if(FAILED(hr)) return hr;

		ATLASSERT(cPropSet==1);
			
		// ���� property���� ����ϱ� ���� ���·� ��ȯ�ϰ�, �޸𸮸� ��ȯ�Ѵ�.

		// WideCharToMultiByte�� ���� �ѹ��� �޸� ���縸���� ���� �� �ִ�.
		// �ٸ� CodePage�� ������ ��� ���� �𸣰ڴ�.
		strncpy(lo, CW2A(V_BSTR(&rgPropSet->rgProperties[0].vValue)), 511); lo[511] = 0;
		strncpy(ds, CW2A(V_BSTR(&rgPropSet->rgProperties[1].vValue)), 511); ds[511] = 0;
		strncpy(id, CW2A(V_BSTR(&rgPropSet->rgProperties[2].vValue)), 511); id[511] = 0;
		strncpy(pw, CW2A(V_BSTR(&rgPropSet->rgProperties[3].vValue)), 4095); pw[4095] = 0;
				
		for(int i=0;i<4;i++) { VariantClear(&(rgPropSet->rgProperties[i].vValue)); }
		CoTaskMemFree(rgPropSet->rgProperties);
	}

	ATLTRACE2(atlTraceDBProvider, 2, "Location=%s;DataSource=%s;"
			  "UserID=%s;Password=%s;Port=%d;\n", lo, ds, id, pw, cas_port);
	
	// UniCAS�� ����
	int rc = cci_connect(lo, cas_port, ds, id, pw);
	ATLTRACE2(atlTraceDBProvider, 3, "cci_connect returned %d\n", rc);
	if(rc<0)
	{
		if(rc==CCI_ER_NO_MORE_MEMORY)
			return E_OUTOFMEMORY;

		// wrong hostname
		return DB_SEC_E_AUTH_FAILED;
	}

	*phConn = rc;
	return S_OK;
}

HRESULT Disconnect(int *phConn)
{
	// FIXME : check return value of cci_disconnect?

	ATLASSERT(phConn!=NULL);

	T_CCI_ERROR err_buf;
	cci_disconnect(*phConn, &err_buf);
	*phConn = 0;
	return S_OK;
}

HRESULT DoesTableExist(int hConn, char *szTableName)
{
	T_CCI_ERROR err_buf;
	int hReq = cci_schema_info(hConn, CCI_SCH_CLASS, szTableName, NULL,
							CCI_CLASS_NAME_PATTERN_MATCH, &err_buf);
	if(hReq<0)
		return E_FAIL;
	int rc = cci_cursor(hReq, 1, CCI_CURSOR_FIRST, &err_buf);
	cci_close_req_handle(hReq);
	if(rc==CCI_ER_NO_MORE_DATA)
		return S_FALSE;
	else if(rc<0)
		return E_FAIL;
	else
		return S_OK;
}

HRESULT OpenTable(int hConn, const CComBSTR &strTableName, int *phReq, int *pcResult, char flag, bool bAsynch, int maxrows)
{
	ATLASSERT(phReq && pcResult);

	if (!strTableName || wcslen(strTableName.m_str) == 0)
		return DB_E_NOTABLE;

	CComBSTR query = "select * from ";
	int len = strTableName.Length();

	//�̹� quotation�� ������ �߰����� �ʰ� ������ �߰��Ѵ�.
	if (strTableName.m_str[0] == '\"' && strTableName.m_str[len - 1] == '\"')
	{
		query.Append(strTableName);
	} else
	{
		query.Append("[");
		query.Append(strTableName);
		query.Append("]");
	}
	
	T_CCI_ERROR err_buf;
	*phReq = cci_prepare(hConn, CW2A(query), flag, &err_buf);
	if(*phReq<0)
	{
		*phReq = 0;

		HRESULT hr = DoesTableExist(hConn, CW2A(strTableName));
		if(hr==S_FALSE)
			return DB_E_NOTABLE;
		else
			return RaiseError(E_FAIL, 1, __uuidof(IOpenRowset), err_buf.err_msg);
	}

	if(maxrows!=0)
	{
//		cci_set_max_row(*phReq, maxrows);
	}

	*pcResult = cci_execute(*phReq, (bAsynch?CCI_EXEC_ASYNC:0), 0, &err_buf);
	if(*pcResult<0)
	{
		*pcResult = 0;

		// TODO: ������ �� �� �ڼ��� �з�
		cci_close_req_handle(*phReq);
		return E_FAIL;
		// return RaiseError(E_FAIL, ?, ?, err_buf.err_msg);
	}

	ATLTRACE(atlTraceDBProvider, 1, "Util::OpenTable success : return hReq=%d, cResult=%d\n", *phReq, *pcResult);

	return S_OK;
}

HRESULT GetUniqueTableName(CComBSTR& strTableName)
{
	int i;

	// Table �̸��� �־����� �ʾ����� ���Ƿ� �����Ѵ�.
	WCHAR rand_buf[31];

	srand( (unsigned)time( NULL ));

	/* Display 30 numbers. */
	for(i = 0; i < 15;i++ )
		rand_buf[i] = 'A' + (rand() % 26);

	srand( (unsigned)time( NULL ) + rand());
	for(; i < 30;i++ )
		rand_buf[i] = 'A' + (rand() % 26);
	rand_buf[30] = '\0';
	strTableName = rand_buf;

	return S_OK;
}

HRESULT GetTableNames(int hConn, CAtlArray<CStringA> &rgTableNames)
{
	int hReq, res;
	T_CCI_ERROR err_buf;
	HRESULT hr = S_OK;

	hReq = cci_schema_info(hConn, CCI_SCH_CLASS, NULL, NULL,
						CCI_CLASS_NAME_PATTERN_MATCH, &err_buf);
	if(hReq<0)
	{
		ATLTRACE2("GetTableNames: cci_schema_info fail\n");
		return E_FAIL;
	}

	res = cci_cursor(hReq, 1, CCI_CURSOR_FIRST, &err_buf);
	if(res==CCI_ER_NO_MORE_DATA) goto done;
	if(res<0) { hr = E_FAIL; goto done; }

	while(1)
	{
		char *value;
		int ind;

		res = cci_fetch(hReq, &err_buf);
		if(res<0) { hr = E_FAIL; goto done; }

		res = cci_get_data(hReq, 1, CCI_A_TYPE_STR, &value, &ind);
		if(res<0) { hr = E_FAIL; goto done; }

		rgTableNames.Add(value);

		res = cci_cursor(hReq, 1, CCI_CURSOR_CURRENT, &err_buf);
		if(res==CCI_ER_NO_MORE_DATA) goto done;
		if(res<0) { hr = E_FAIL; goto done; }
	}

done:
	cci_close_req_handle(hReq);
	return hr;
}

//Ư�� ���̺��� distinct�� �ε��� �̸� ����Ʈ�� �����´�.
HRESULT GetIndexNamesInTable(int hConn, char* table_name, CAtlArray<CStringA> &rgIndexNames, CAtlArray<int> &rgIndexTypes)
{
	int hReq, res;
	T_CCI_ERROR err_buf;
	HRESULT hr = S_OK;

	hReq = cci_schema_info(hConn, CCI_SCH_CONSTRAINT, table_name, NULL,
						CCI_ATTR_NAME_PATTERN_MATCH, &err_buf);
	if(hReq<0)
	{
		ATLTRACE2("GetIndexNamesInTable: cci_schema_info fail\n");
		return E_FAIL;
	}

	res = cci_cursor(hReq, 1, CCI_CURSOR_FIRST, &err_buf);
	if(res==CCI_ER_NO_MORE_DATA) goto done;
	if(res<0) { hr = E_FAIL; goto done; }

	while(1)
	{
		char *value;
		int ind, isUnique;

		res = cci_fetch(hReq, &err_buf);
		if(res<0) { hr = E_FAIL; goto done; }

		res = cci_get_data(hReq, 1, CCI_A_TYPE_INT, &isUnique, &ind);
		if(res<0) { hr = E_FAIL; goto done; }
		res = cci_get_data(hReq, 2, CCI_A_TYPE_STR, &value, &ind);
		if(res<0) { hr = E_FAIL; goto done; }

		bool isAlreadyExist = false;
		for (size_t i = 0; i < rgIndexNames.GetCount(); i++)
		{
			if (!strcmp(value, rgIndexNames[i].GetBuffer()))
			{
				isAlreadyExist = true;
				break;
			}
		}
		if (!isAlreadyExist)
		{
			rgIndexNames.Add(value);
			rgIndexTypes.Add(isUnique);
		}

		res = cci_cursor(hReq, 1, CCI_CURSOR_CURRENT, &err_buf);
		if(res==CCI_ER_NO_MORE_DATA) goto done;
		if(res<0) { hr = E_FAIL; goto done; }
	}

done:
	cci_close_req_handle(hReq);
	return hr;
}

void ExtractTableName(const CComBSTR &strCommandText, CComBSTR &strTableName)
{
	// Update ��츦 ���� ���̺� �̸��� ������ �д�
	// �������� ���̺��� from �ڿ� ���� ��� updatable query�� false�� �� ���̹Ƿ�
	// �� �̸��� ������� �ʴ´�. ���� �̾Ƴ� ���̺� �̸��� Valid���� �ʴ�.
	CW2A str(strCommandText);
	if(str==NULL) return;
	_strlwr(str);
	strTableName = "";

	char *tmp = strstr(str, "from ");
	if(tmp) //select, delete�� ���
	{
		char *tmp2 = strchr(tmp, '\"');
		if(tmp2) // double quotation�� ������
		{
			char *tmp3 = strchr(tmp2+1, '\"');
			if(tmp3)
			{
				*tmp3 = 0;
				strTableName = tmp2 + 1;
			}
			// else : �ݴ� quot�� ����. ����
		}
		else
		{
			tmp = tmp+5;
			while(*tmp==' ') tmp++;
			char *tmp3 = strchr(tmp, ' ');
			if(tmp3)
				*tmp3 = 0;
			strTableName = tmp; // ��ĭ Ȥ�� SQL�� �������� ���̺� �̸�
		}
	} else
	{
		tmp = strstr(str, "insert ");
		char* tmp1 = strstr(str, "into ");

		if (tmp && tmp1) //insert�� ���
		{
			char *tmp2 = strchr(tmp1 + 5, '\"');
			if(tmp2) // double quotation�� ������
			{
				char *tmp3 = strchr(tmp2+1, '\"');
				if(tmp3)
				{
					*tmp3 = 0;
					strTableName = tmp2 + 1;
				}
				// else : �ݴ� quot�� ����. ����
			}
			else
			{
				tmp = tmp1+5;
				while(*tmp==' ') tmp++;
				char *tmp3 = strpbrk(tmp, " (");
				if(tmp3)
					*tmp3 = 0;
				strTableName = tmp; // ��ĭ Ȥ�� SQL�� �������� ���̺� �̸�
			}
		} else
		{
			tmp = strstr(str, "update ");
			char* tmp1 = strstr(str, "set ");

			if (tmp && tmp1) //update�� ���
			{
				char *tmp2 = strchr(tmp + 7, '\"');
				if(tmp2) // double quotation�� ������
				{
					char *tmp3 = strchr(tmp2+1, '\"');
					if(tmp3)
					{
						*tmp3 = 0;
						strTableName = tmp2 + 1;
					}
					// else : �ݴ� quot�� ����. ����
				}
				else
				{
					tmp = tmp+7;
					while(*tmp==' ') tmp++;
					char *tmp3 = strpbrk(tmp, " ");
					if(tmp3)
						*tmp3 = 0;
					strTableName = tmp; // ��ĭ Ȥ�� SQL�� �������� ���̺� �̸�
				}
			}
		}

	}
}

bool RequestedRIIDNeedsUpdatability(REFIID riid)
{
	if (riid == IID_IRowsetUpdate ||
		riid == IID_IRowsetChange ||
		riid == IID_IRowsetRefresh ||
		riid == IID_ISequentialStream ||
		riid == IID_IRow ||
		riid == IID_IGetRow ||
		riid == IID_IGetSession)
		return true;

	return false;
}

//bool RequestedRIIDNeedsOID(REFIID riid)
//{
//	if (riid == IID_IRow ||
//		riid == IID_IGetRow ||
//		riid == IID_IGetSession)
//		return true;
//
//	return false;
//}

//bool CheckOIDFromProperties(ULONG cSets, const DBPROPSET rgSets[])
//{
//	for (ULONG i = 0; i < cSets; i++)
//	{
//		if (rgSets[i].guidPropertySet == DBPROPSET_ROWSET)
//		{
//			ULONG cProp = rgSets[i].cProperties;
//			
//			for (ULONG j=0; j < cProp; j++)
//			{
//				DBPROP* rgProp = &rgSets[i].rgProperties[j];
//				if (!rgProp) return false;
//
//				if (rgProp->dwPropertyID == DBPROP_IRow ||
//					rgProp->dwPropertyID == DBPROP_IGetRow ||
//					rgProp->dwPropertyID == DBPROP_IGetSession)
//				{
//					if (V_BOOL(&(rgProp->vValue)) == ATL_VARIANT_TRUE)
//						return true;
//				}
//			}
//		}
//	}
//	return false;
//}

bool CheckUpdatabilityFromProperties(ULONG cSets, const DBPROPSET rgSets[])
{
	for (ULONG i = 0; i < cSets; i++)
	{
		if (rgSets[i].guidPropertySet == DBPROPSET_ROWSET)
		{
			ULONG cProp = rgSets[i].cProperties;
			
			for (ULONG j=0; j < cProp; j++)
			{
				DBPROP* rgProp = &rgSets[i].rgProperties[j];
				if (!rgProp) return false;

				if (rgProp->dwPropertyID == DBPROP_IRowsetChange ||
					rgProp->dwPropertyID == DBPROP_IRowsetUpdate ||
					rgProp->dwPropertyID == DBPROP_ISequentialStream ||
					rgProp->dwPropertyID == DBPROP_IRow ||
					rgProp->dwPropertyID == DBPROP_IGetRow ||
					rgProp->dwPropertyID == DBPROP_IGetSession ||
					rgProp->dwPropertyID == DBPROP_OTHERUPDATEDELETE)
				{
					if (V_BOOL(&rgProp->vValue) == ATL_VARIANT_TRUE)
						return true;
				}
			}
		}
	}
	return false;
}

//Row���� ȣ��Ǵ� ���
HRESULT CColumnsInfo::GetColumnInfo(T_CCI_COL_INFO* info, T_CCI_CUBRID_STMT cmd_type, int cCol, bool bBookmarks, ULONG ulMaxLen)
{
	m_cColumns = cCol;

	return GetColumnInfoCommon(info, cmd_type, bBookmarks, ulMaxLen);
}

HRESULT CColumnsInfo::GetColumnInfo(int hReq, bool bBookmarks, ULONG ulMaxLen)
{
	T_CCI_CUBRID_STMT cmd_type;
	T_CCI_COL_INFO *info = cci_get_result_info(hReq, &cmd_type, &m_cColumns);
	if(info==NULL) return E_FAIL;

	return GetColumnInfoCommon(info, cmd_type, bBookmarks, ulMaxLen);
}

HRESULT CColumnsInfo::GetColumnInfoCommon(T_CCI_COL_INFO* info, T_CCI_CUBRID_STMT cmd_type, bool bBookmarks, ULONG ulMaxLen)
{
	// �ϸ�ũ �÷� �߰�
	if(bBookmarks) m_cColumns++;

	m_pInfo = new ATLCOLUMNINFO[m_cColumns];
	if(m_pInfo==NULL)
	{
		m_cColumns = 0;
		return E_OUTOFMEMORY;
	}

	//ulMaxLen���� cci_db_paramter�� ���� MAX_STRING_LENGTH���� �ش�
	//�÷� ����� �� ������ ū ��� ��� �� ���� �÷� length�� �Ѱ��ش�
	//MAX length���� �־����� �ʾ����� 1024�� ����
	if (!ulMaxLen)
		ulMaxLen = 1024;

	// �ϸ�ũ �÷��� Type Info
	const Type::StaticTypeInfo bmk_sta_info = { CCI_U_TYPE_UNKNOWN, DBTYPE_BYTES };
	const Type::DynamicTypeInfo bmk_dyn_info = { sizeof(ULONG), DBCOLUMNFLAGS_ISBOOKMARK|DBCOLUMNFLAGS_ISFIXEDLENGTH, 0, 0 };

	for(int i=0;i<m_cColumns;i++)
	{
		int iOrdinal = i;
		if(!bBookmarks) iOrdinal++;

		//multiset���� info type�� -1�� ������ ���
		//�ӽ÷� string�� �ش��ϴ� static info, dynamic info�� ��ȯ�Ѵ�.
		//modified by risensh1ne
		//2003.06.17
		int type;
		if (iOrdinal > 0)
			type = CCI_GET_RESULT_INFO_TYPE(info, iOrdinal);

		const Type::StaticTypeInfo &sta_info =
			( iOrdinal==0 ? bmk_sta_info : 
			  type==-1    ? Type::GetStaticTypeInfo(CCI_U_TYPE_STRING) :
							Type::GetStaticTypeInfo(info, iOrdinal) );
		Type::DynamicTypeInfo dyn_info =
			( iOrdinal==0 ? bmk_dyn_info : 
			  type==-1    ? Type::GetDynamicTypeInfo(CCI_U_TYPE_STRING, 0, 0, true) :
							Type::GetDynamicTypeInfo(info, iOrdinal) );

		// method�� ���� ������ result-set�� �� column-name�� "METHOD_RESULT"
		// �� �����ϰ� column-type�� varchar�� �����Ѵ�.
		if (cmd_type == CUBRID_STMT_CALL || cmd_type == CUBRID_STMT_EVALUATE || cmd_type == CUBRID_STMT_GET_STATS)
		{
			m_pInfo[i].pwszName = _wcsdup(
				iOrdinal==0 ? L"Bookmark" : L"METHOD_RESULT");
		} else
		{
			m_pInfo[i].pwszName = _wcsdup(
				iOrdinal==0 ? L"Bookmark" : CA2W(CCI_GET_RESULT_INFO_NAME(info, iOrdinal)) );
		}

		if(m_pInfo[i].pwszName==NULL)
		{
			m_cColumns = i;
			FreeColumnInfo();
			return E_OUTOFMEMORY;
		}
		m_pInfo[i].pTypeInfo = NULL;
		m_pInfo[i].iOrdinal = iOrdinal;

		// method�� ���� ������ result-set�� �� column-name�� "METHOD_RESULT"
		// �� �����ϰ� column-type�� varchar�� �����Ѵ�.
		if (cmd_type == CUBRID_STMT_CALL || cmd_type == CUBRID_STMT_EVALUATE || cmd_type == CUBRID_STMT_GET_STATS)
			m_pInfo[i].wType = DBTYPE_STR;
		else
			m_pInfo[i].wType = sta_info.nOLEDBType;

		//MAX_LENGTH �̻��� ��� ulMaxLen���� �÷������� ����
		//modified by risensh1ne 20030609
		if (dyn_info.ulColumnSize > ulMaxLen)
			m_pInfo[i].ulColumnSize = ulMaxLen;
		else
			m_pInfo[i].ulColumnSize = dyn_info.ulColumnSize;

		m_pInfo[i].bPrecision = dyn_info.bPrecision;
		m_pInfo[i].bScale = dyn_info.bScale;
		m_pInfo[i].dwFlags = dyn_info.ulFlags;
		m_pInfo[i].cbOffset = 0;

		if(iOrdinal==0)
		{
			m_pInfo[i].columnid.eKind = DBKIND_GUID_PROPID;
			memcpy(&m_pInfo[i].columnid.uGuid.guid, &DBCOL_SPECIALCOL, sizeof(GUID));
			m_pInfo[i].columnid.uName.ulPropid = 2;
		}
		else
		{
			m_pInfo[i].columnid.eKind = DBKIND_GUID_NAME;
			m_pInfo[i].columnid.uName.pwszName = m_pInfo[i].pwszName;
			_wcsupr(m_pInfo[i].pwszName);
			memset(&m_pInfo[i].columnid.uGuid.guid, 0, sizeof(GUID));
			m_pInfo[i].columnid.uGuid.guid.Data1 = iOrdinal; 
		}
	}
	
	return S_OK;
}

void CColumnsInfo::FreeColumnInfo()
{
	for(int i=0;i<m_cColumns;i++)
	{
		if(m_pInfo[i].pwszName)
			free(m_pInfo[i].pwszName);
	}
	delete [] m_pInfo;
			
	m_cColumns = 0;
	m_pInfo = 0;
}


}

