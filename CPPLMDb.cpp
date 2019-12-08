#include <windows.h>
#include <atlbase.h>

#include "CPPLMDb.h"
#include "lmdb/lmdb.h"
#pragma comment(lib, "comsuppw.lib")
#include <comutil.h>
#include <map>
#include <string>
#include <errno.h>
#include <stack>
#include <map>
#include <atlbase.h>

typedef std::map<std::wstring, MDB_env *> StringENVArray;
class CProcessEnv{
public:
	CProcessEnv():_binitialized(FALSE){
		init();
	}
	~CProcessEnv(){
		uninit();
		DeleteCriticalSection(&_cs);
	}
	void init(){
		if(!InterlockedExchange(&_binitialized, TRUE)){
			InitializeCriticalSection(&_cs);
		}
	}
	void uninit(){
		EnterCriticalSection(&_cs);
		for(StringENVArray::iterator it= _sea.begin();it != _sea.end();++it){
			mdb_env_close((*it).second);
		}
		_sea.clear();
		LeaveCriticalSection(&_cs);
	}
	MDB_env * GetEnv(LPCWSTR lpszDBPath){
		MDB_env * penv = NULL;
		WCHAR szPath[MAX_PATH] = {0};
		wcscpy(szPath, lpszDBPath);
		_wcsupr(szPath);

		EnterCriticalSection(&_cs);
		if(_sea.find((LPCWSTR)szPath) != _sea.end()){
			penv = _sea[szPath];
		}
		LeaveCriticalSection(&_cs);
		return penv;
	}
	void SetEnv(MDB_env * env, LPCWSTR lpszDBPath){
		WCHAR szPath[MAX_PATH] = {0};
		wcscpy(szPath, lpszDBPath);
		_wcsupr(szPath);
		EnterCriticalSection(&_cs);
		_sea[szPath] = env;
		LeaveCriticalSection(&_cs);
	}
private:
	StringENVArray _sea;
	CRITICAL_SECTION _cs;
	volatile LONG _binitialized;
};
static CProcessEnv gEnv;

typedef std::map<DWORD, CppLMDb::transblock *> ThreadTransBlockArray;
typedef struct tagCPPLMDBData{
	MDB_env * env;
	MDB_dbi dbi;
	MDB_stat stat;
	CRITICAL_SECTION cstxn;
	ThreadTransBlockArray ttba;
}CPPLMDBDATA;
CppLMDb::CppLMDb(void):
_p(new CPPLMDBDATA),
data(*_p)
{
	data.env = NULL;
	data.dbi = 0;
	InitializeCriticalSection(&data.cstxn);
}

 

CppLMDb::~CppLMDb(void)
{
	mdb_dbi_close(data.env, data.dbi);

	if(data.env){
		// mdb_env_close(data.env);
	}
	DeleteCriticalSection(&data.cstxn);
	delete _p;
}

HRESULT CppLMDb::init(LPCWSTR lpszDbfilePath,UINT uMaxSizeOfFile_M, int optional)
{
	HRESULT hr = S_OK;
	if(!gEnv.GetEnv(lpszDbfilePath)){
		if(MDB_SUCCESS != (hr = mdb_env_create(&data.env))) return hr;
		// if(MDB_SUCCESS != (hr = mdb_env_set_maxreaders(data.env, 1))) return hr;
		__int64 lSize = 1024 * 1024;
		lSize *= 
			uMaxSizeOfFile_M
			;
		if(MDB_SUCCESS != (hr = mdb_env_set_mapsize(data.env, lSize

			))) return hr; // 500m
// 		if(MDB_SUCCESS !=(hr = mdb_env_set_maxdbs(data.env, 4))){
// 			return hr;
// 		}
		USES_CONVERSION;
		if(MDB_SUCCESS != (hr = mdb_env_open(data.env, W2CA_CP(lpszDbfilePath, CP_UTF8), optional/*MDB_FIXEDMAP|MDB_NOSUBDIR*/ /*|MDB_NOSYNC*/, 0664)))
			return E_INVALIDARG;
		gEnv.SetEnv(data.env, lpszDbfilePath);
	}else{
		data.env = gEnv.GetEnv(lpszDbfilePath);
	}
	
	
	
	return S_OK;
}

HRESULT CppLMDb::init( UINT uMaxSizeOfFile, int optional /*= 0x04000MDB_DUPSORT*/ )
{
	HRESULT hr = S_OK;
		if(MDB_SUCCESS != (hr = mdb_env_create(&data.env))) return hr;
		__int64 lSize = 1024 * 1024;
		lSize *= 
			uMaxSizeOfFile
			;
		if(MDB_SUCCESS != (hr = mdb_env_set_mapsize(data.env, lSize))) return hr; // 500m
		if(MDB_SUCCESS != (hr = mdb_env_open(data.env, NULL, optional/*MDB_FIXEDMAP|MDB_NOSUBDIR*/ /*|MDB_NOSYNC*/, 0664)))
			return hr;
		
	return S_OK;
}

HRESULT CppLMDb::Set(transblock * ptb,  void * pKeyData, DWORD dwSizeOfKey, void * pValData, DWORD dwSizeOfVal, int optional /*= 0*/ )
{
	HRESULT hr = S_OK;
	MDB_val key = {dwSizeOfKey, pKeyData};
	MDB_val val = {dwSizeOfVal, pValData};
	if(MDB_SUCCESS != (hr = mdb_put((MDB_txn*)ptb->current(), data.dbi, &key, &val, optional))){
		return hr;
	}
	

	return S_OK;
}

HRESULT CppLMDb::Get( transblock * ptb, void * pKeyData, DWORD dwSizeOfKey, void ** ppValData, DWORD * pdwSizeOfVal )
{
	MDB_val key = {dwSizeOfKey, pKeyData};
	MDB_val val = {0};
	HRESULT hr = S_OK;
	if(MDB_SUCCESS != (hr = mdb_get((MDB_txn*)ptb->current(), data.dbi, &key, &val))){
		return hr;
	}

	if(ppValData && pdwSizeOfVal){
		*ppValData = val.mv_data;
		*pdwSizeOfVal = val.mv_size;
	}
	return S_OK;
}


HRESULT CppLMDb::Get(transblock *ptb, void * pKeyData, DWORD dwSizeOfKey, enumdata ** pgetdata)
{
	// if(!pKeyData) return E_UNEXPECTED;
	if(!pgetdata) return E_UNEXPECTED;
	HRESULT hr = S_OK;

	class mygetdata:public enumdata{
	public:
		mygetdata(CppLMDb & lmdb, transblock *ptb):_inited(false), _cursor(0),
			_src(lmdb), _tb(ptb)
		{}
		~mygetdata(){}
		bool next(void ** key, DWORD * pdwSizeofKey, void ** pdata, DWORD * pdwSizeOfVal){
			if(!_inited) return _inited;
			if(!_validnext) return false;

			*key = _key.mv_data;
			*pdwSizeofKey = _key.mv_size;
			*pdata = _val.mv_data;
			*pdwSizeOfVal = _val.mv_size;

			MDB_val &k = _key;
			MDB_val &v = _val;
			int nret = 0;
			if(MDB_SUCCESS != (nret = mdb_cursor_get(_cursor, &k, &v, MDB_NEXT_DUP))){
				if(nret == MDB_NOTFOUND)
					_validnext = false;
				else
					return false;
			}
			
			return true;
		}
		void close(){
			if(_inited && _cursor)
				mdb_cursor_close(_cursor);
			delete this;
		}
		HRESULT init(){
			HRESULT hr = S_OK;
			if(MDB_SUCCESS == (hr = mdb_cursor_open((MDB_txn*)_tb->current(), _src.data.dbi, &_cursor))){
				_inited = true;
				_validnext = true;
				// 
				if(_key.mv_data){
					 if(MDB_SUCCESS != (hr = mdb_cursor_get(_cursor, &_key, &_val, MDB_SET_KEY))){
						return hr;
					}
					_optional = MDB_NEXT_DUP;
				}else{
					_optional = MDB_NEXT;
				}
			}
			return hr;
		}
	public:
		bool		_validnext;
		bool		_inited;
		MDB_cursor * _cursor;
		MDB_val		_val;
		MDB_val		_key;
		MDB_cursor_op _optional;
		transblock * _tb;
		CppLMDb &_src;
	};
	
	mygetdata * pg = new mygetdata(*this, ptb);

	pg->_key.mv_data = pKeyData;
	pg->_key.mv_size = dwSizeOfKey;
	pg->_val.mv_data = 0;
	pg->_val.mv_size = 0;
	if(MDB_SUCCESS != (hr = pg->init())){
		delete pg;
		return hr;
	}
	*pgetdata = pg;
	return MDB_SUCCESS;
}

HRESULT CppLMDb::Del(transblock *ptb, void * pKeyData, DWORD dwSizeOfKey )
{
	MDB_val key = {dwSizeOfKey, pKeyData};
	HRESULT hr = S_OK;
	if(MDB_SUCCESS != (hr = mdb_del((MDB_txn*)ptb->current(), data.dbi, &key, NULL))){
		return hr;
	}
	return hr;
}

HRESULT CppLMDb::Del(transblock *ptb, void * pKeyData, DWORD dwSizeOfKey, void * pValData, DWORD dwSizeOfVal )
{
	MDB_val key = {dwSizeOfKey, pKeyData};
	MDB_val val = {dwSizeOfVal, pValData};
	HRESULT hr = S_OK;
	if(MDB_SUCCESS != (hr = mdb_del((MDB_txn*)ptb->current(), data.dbi, &key, &val))){
		return hr;
	}
	return hr;
}

ULONGLONG CppLMDb::Count(transblock *ptb,void * key/* = NULL*/, DWORD dwKeySize/* = 0*/)
{
	if(key != NULL){
		MDB_cursor * cursor = NULL;
		// transblock tb(*this);
		if(MDB_SUCCESS == mdb_cursor_open((MDB_txn*)ptb->current(), data.dbi, &cursor)){
			MDB_val mkey = {dwKeySize, key};
			MDB_val mval = {0};
			mdb_size_t mst = 0L;
			if(MDB_SUCCESS == mdb_cursor_get(cursor, &mkey, &mval, MDB_SET)){
				if(MDB_SUCCESS == mdb_cursor_count(cursor, &mst)){	
				}
			}
			mdb_cursor_close(cursor);
			return mst;
		}
	}else{
		MDB_stat stat = {0};
		if(MDB_SUCCESS == mdb_env_stat(data.env, &stat)){
			return stat.ms_entries;
		}
	}
	return 0L;
}

HRESULT CppLMDb::Update(transblock *ptb, void * pKeyData, DWORD dwSizeOfKey, void * pValData, DWORD dwSizeOfVal )
{
	MDB_val k = {dwSizeOfKey, pKeyData};
	MDB_val v = {dwSizeOfVal, pValData};
	return mdb_put((MDB_txn*)ptb->current(), data.dbi, &k, &v, 0);
}

HRESULT CppLMDb::Update(transblock *ptb, void * pKeyData, DWORD dwSizeOfKey, void * pOldVal, DWORD dwSizeOfOldVal, void * pNewVal, DWORD dwSizeOfNewVal )
{
	class CAutoClose{
	public:
		CAutoClose():_cursor(NULL){}
		~CAutoClose(){
			if(_cursor){
				mdb_cursor_close(_cursor);
			}
		}
		MDB_cursor * _cursor;
	};
	CAutoClose cac;
	HRESULT hr = S_OK;
	if(MDB_SUCCESS != (hr = mdb_cursor_open((MDB_txn*)ptb->current(), data.dbi, &cac._cursor))) return hr;
	while(1){
		MDB_val k = {dwSizeOfKey, pKeyData};
		MDB_val v = {dwSizeOfOldVal, pOldVal};
		MDB_val nv = {dwSizeOfNewVal, pNewVal};
		if(MDB_SUCCESS != (hr = mdb_cursor_get(cac._cursor, &k, &v, MDB_GET_BOTH))){
			return hr;
		}
		if(MDB_SUCCESS != (hr = mdb_cursor_del(cac._cursor, 0))) return hr;
		if(MDB_SUCCESS != (hr = mdb_put((MDB_txn*)ptb->current(), data.dbi, &k, &nv, 0))) return hr;	
	}
	
	return hr;
}

LPCWSTR CppLMDb::Error( int nErrorCode )
{
#define CASEERR(x) case x: return L#x;
	switch(nErrorCode){
CASEERR(MDB_KEYEXIST);//	(-30799)
	/** key/data pair not found (EOF) */
CASEERR(MDB_NOTFOUND);//	(-30798)
	/** Requested page not found - this usually indicates corruption */
CASEERR(MDB_PAGE_NOTFOUND);//	(-30797)
	/** Located page was wrong type */
CASEERR(MDB_CORRUPTED);//	(-30796)
	/** Update of meta page failed or environment had fatal error */
CASEERR(MDB_PANIC);//		(-30795)
	/** Environment version mismatch */
CASEERR(MDB_VERSION_MISMATCH);//	(-30794)
	/** File is not a valid LMDB file */
CASEERR(MDB_INVALID);//	(-30793)
	/** Environment mapsize reached */
CASEERR(MDB_MAP_FULL);//	(-30792)
	/** Environment maxdbs reached */
CASEERR(MDB_DBS_FULL);//	(-30791)
	/** Environment maxreaders reached */
CASEERR(MDB_READERS_FULL);//	(-30790)
	/** Too many TLS keys in use - Windows only */
CASEERR(MDB_TLS_FULL);//	(-30789)
	/** Txn has too many dirty pages */
CASEERR(MDB_TXN_FULL);//	(-30788)
	/** Cursor stack too deep - internal error */
CASEERR(MDB_CURSOR_FULL);//	(-30787)
	/** Page has not enough space - internal error */
CASEERR(MDB_PAGE_FULL);//	(-30786)
	/** Database contents grew beyond environment mapsize */
CASEERR(MDB_MAP_RESIZED);//	(-30785)
	/** Operation and DB incompatible, or DB type changed. This can mean:
	 *	<ul>
	 *	<li>The operation expects an #MDB_DUPSORT / #MDB_DUPFIXED database.
	 *	<li>Opening a named DB when the unnamed DB has #MDB_DUPSORT / #MDB_INTEGERKEY.
	 *	<li>Accessing a data record as a database, or vice versa.
	 *	<li>The database was dropped and recreated with different flags.
	 *	</ul>
	 */
CASEERR(MDB_INCOMPATIBLE);//	(-30784)
	/** Invalid reuse of reader locktable slot */
CASEERR(MDB_BAD_RSLOT);//		(-30783)
	/** Transaction must abort, has a child, or is invalid */
CASEERR(MDB_BAD_TXN);//			(-30782)
	/** Unsupported size of key/DB name/data, or wrong DUPFIXED size */
CASEERR(MDB_BAD_VALSIZE);//		(-30781)
	/** The specified DBI was changed unexpectedly */
CASEERR(MDB_BAD_DBI	);//	(-30780)
	/** Unexpected problem - txn should abort */
CASEERR(MDB_PROBLEM	);//	(-30779)
CASEERR(EINVAL);
CASEERR(S_OK);
CASEERR(ERROR_NOT_ENOUGH_MEMORY);
default:
	break;
	}
	return L"unknown";
}

HRESULT CppLMDb::UseTransblock( BOOL bUse )
{
	// data.bUseTransBlock = bUse;
	return S_OK;
}

BOOL CppLMDb::GetUseTransblock()
{
	// return data.bUseTransBlock;
	return TRUE;
}

HRESULT CppLMDb::EmptyDB(transblock *ptb)
{
	return MDB_SUCCESS == mdb_drop((MDB_txn*)ptb->current(), data.dbi, 0)?S_OK:E_FAIL; // clear db;
	
}


typedef struct tagTransBlockData{
	MDB_env * env;
	MDB_txn * txn;
	BOOL bValid;
	BOOL bFailure;
	BOOL bParentTxn;
	CppLMDb * _psrc;
	UINT reftxn;
}TRANSBLOCKDATA;
CppLMDb::transblock::transblock(CppLMDb & src, unsigned int optional):
_p(new TRANSBLOCKDATA),
data(*_p)
{
	data.reftxn = 0;
	// data.pptxnTransBlock = &src.data.txnTransblock;
	data._psrc = &src;
	data.bFailure = FALSE;
	data.env = src.data.env;
	data.bParentTxn = FALSE;
	data.txn = NULL;
	

	data.bValid = (MDB_SUCCESS == mdb_txn_begin(data.env, NULL, optional, &data.txn));	
	if(data.bValid){
		// EnterCriticalSection(&data._psrc->data.cstxn);
		// data._psrc->data.ttba[GetCurrentThreadId()] = this;
		// LeaveCriticalSection(&data._psrc->data.cstxn);
	}
}

CppLMDb::transblock::~transblock()
{
	
	if(data.bValid){
		MDB_txn * txn = data.txn;// data._psrc->data.txnStack.top();
		// data._psrc->data.txnStack.pop();
		if(data.bFailure){
			mdb_txn_abort(txn);
		}
		else
		{
			mdb_txn_commit(txn);
		}
	}

	CppLMDb * pSrc = data._psrc;
	// EnterCriticalSection(&pSrc->data.cstxn);
	//	pSrc->data.ttba.erase(GetCurrentThreadId());
	// LeaveCriticalSection(&pSrc->data.cstxn);
	delete _p;
	
}

void CppLMDb::transblock::failure()
{
	ATLASSERT(!"FAILURE ERROR");
	data.bFailure = TRUE;
}

void * CppLMDb::transblock::current()
{
	return data.txn;
}

void CppLMDb::transblock::addref()
{
	data.reftxn++;
}

void CppLMDb::transblock::release()
{
	data.reftxn --;
	
		if(!data.reftxn){
		delete this;
	}
}
CppLMDb::transblock * CppLMDb::GetTransBlockByThread()
{	
	transblock * p = NULL;
	// EnterCriticalSection(&data.cstxn);
	// ThreadTransBlockArray::iterator it = data.ttba.find(GetCurrentThreadId());
// 	if(it != data.ttba.end()){
// 		p = (*it).second;
// 		p->addref();
// 	}else{
		p = new transblock(*this);
		p->addref();
//	}
//	LeaveCriticalSection(&data.cstxn);
	return p;
}

HRESULT CppLMDb::OpenDB( DWORD dwFlags )
{
	transblock tb(*this);

	HRESULT hr = S_OK;
	if(MDB_SUCCESS != (hr = mdb_dbi_open((MDB_txn*)tb.current(), NULL, dwFlags, &data.dbi))){
		return hr;
	}
	return hr;
}
