#pragma once

#ifndef CPPLMDb_h__
#define CPPLMDb_h__

class CppLMDb
{
public:
	enum{
		nooverwrite = 0x10,
		nodupdata = 0x20,
		current = 0x40,
		reverse = 0x10000,
		append = 0x20000,
		appenddup = 0x40000,
		multiple = 0x80000
	};
	enum{
		DB_OptionalBegin = 0,
		DB_DUP = 0x4,
	};
	class enumdata{
	public:
		enumdata(){}
		~enumdata(){}
		virtual bool next(void ** key, DWORD * pdwSizeofKey, void ** pdata, DWORD * pdwSizeOfVal) = 0 ;
		virtual void close() = 0;
	};
	class transblock{
	public:
		transblock(CppLMDb & src, unsigned int optional = 0/*MDB_RDONLY*/);
		~transblock();
		void addref();
		void release();
		void failure();
		void * current();
	private:
		struct tagTransBlockData * _p;
		struct tagTransBlockData &data;
	};
	static LPCWSTR Error(int nErrorCode);
	CppLMDb(void);
	virtual ~CppLMDb(void);
	HRESULT UseTransblock(BOOL bUse);
	BOOL    GetUseTransblock();
	HRESULT init(UINT uMaxSizeOfFile, int optional /*= 0x04000MDB_DUPSORT*/);
	HRESULT init(LPCWSTR lpszDbfilePath,UINT uMaxSizeOfFile, int optional /*= 0x04000MDB_DUPSORT*/);
	HRESULT OpenDB(DWORD dwFlags = 0);
	HRESULT Set(transblock * ptb, void * pKeyData, DWORD dwSizeOfKey, void * pValData, DWORD dwSizeOfVal, int optional = 0);
	HRESULT Update(transblock * ptb, void * pKeyData, DWORD dwSizeOfKey, void * pValData, DWORD dwSizeOfVal);
	HRESULT Update(transblock * ptb, void * pKeyData, DWORD dwSizeOfKey, void * pOldVal, DWORD dwSizeOfOldVal, void * pNewVal, DWORD dwSizeOfNewVal);
	HRESULT Get(transblock * ptb, void * pKeyData, DWORD dwSizeOfKey, void ** ppValData, DWORD * pdwSizeOfVal);
	HRESULT Get(transblock * ptb, void * pKeyData, DWORD dwSizeOfKey, enumdata ** ppgetdata);
	HRESULT Del(transblock * ptb, void * pKeyData, DWORD dwSizeOfKey);
	HRESULT Del(transblock * ptb, void * pKeyData, DWORD dwSizeOfKey, void * pValData, DWORD dwSizeOfVal);
	ULONGLONG Count(transblock * ptb, void * key = NULL, DWORD dwKeySize = 0);
	HRESULT EmptyDB(transblock * ptb);
	
	transblock * GetTransBlockByThread();
private:
	struct tagCPPLMDBData * _p;
	struct tagCPPLMDBData & data;
};


class CDBRecord{
	CppLMDb & lmdb;
	CppLMDb::transblock & tb;
public:
	CDBRecord(CppLMDb & src, CppLMDb::transblock &stb):lmdb(src), tb(stb){}
	template<typename _key, typename _val>
	HRESULT Set(_key * _k, _val * _v, int optional = 0){
		return lmdb.Set(&tb, (void*)_k, sizeof(_key), (void*)_v, sizeof(_val), optional);
	}
	template<typename _key, typename _val>
	HRESULT Get(_key * _k, _val ** _v){
		DWORD dwSizeOfVal = 0;
		return lmdb.Get(&tb, (void*)_k, sizeof(_key), (void**)_v, &dwSizeOfVal);
	}
	template<typename _key>
	HRESULT Del(_key * _k){
		return lmdb.Del(&tb, (void*)_k, sizeof(_key));
	}

	template<typename _key, typename _val>
	HRESULT Del(_key * _k, _val * _v){
		return lmdb.Del(&tb, (void*)_k, sizeof(_key), (void*)_v, sizeof(_val));
	}
	template<typename _key, typename _val>
	HRESULT Update(_key * _k, _val * _v){
		return lmdb.Update(&tb, (void*)_k, sizeof(_key), (void*)_v, sizeof(_val));
	}

	template<typename _key, typename _val, typename _nv>
	HRESULT Update(_key * _k, _val * _v, _nv * _newv){
		return lmdb.Update(&tb, (void*)_k, sizeof(_key), (void*)_v, sizeof(_val), (void*)_newv, sizeof(_nv));
	}

	template<typename _key, typename _val>
	HRESULT RawSet(_key _k, _val _v, int optional = /*0x40000*/0 ){ // MDB_APPENDDUP
		return lmdb.Set(&tb, (void*)&_k, sizeof(_key), (void*)&_v, sizeof(_val), optional);
	}
	template<typename _key, typename _val>
	HRESULT RawGet(_key _k, _val & _v){
		DWORD dwSizeOfVal = 0;
		void * p = NULL;
		HRESULT hr = lmdb.Get(&tb, (void*)&_k, sizeof(_key), (void**)&p, &dwSizeOfVal);
		if(hr == S_OK && p)
			_v = *(_val*)p;
		return hr;
	}
	template<typename _key>
	HRESULT RawGetEnum(_key _k, CppLMDb::enumdata ** ppEnum){
		HRESULT hr = lmdb.Get(&tb, (void*)&_k, sizeof(_key), ppEnum);
		return hr;
	}
	template<typename _key>
	HRESULT RawDel(_key _k){
		return lmdb.Del(&tb, (void*)&_k, sizeof(_key));
	}

	template<typename _key, typename _val>
	HRESULT RawDel(_key _k, _val _v){
		return lmdb.Del(&tb,(void*)&_k, sizeof(_key), (void*)&_v, sizeof(_val));
	}

	template<typename _key>
	ULONGLONG RawCount(_key _k){
		return lmdb.Count(&tb, (void*)&_k, sizeof(_key));
	}

	template<typename _key, typename _val>
	HRESULT RawUpdate(_key _k, _val _v){
		return lmdb.Update(&tb, (void*)&_k, sizeof(_key), (void*)&_v, sizeof(_val));
	}

	template<typename _key, typename _val, typename _nv>
	HRESULT RawUpdate(_key  _k, _val  _v, _nv  _newv){
		return lmdb.Update(&tb, (void*)&_k, sizeof(_key), (void*)&_v, sizeof(_val), (void*)&_newv, sizeof(_nv));
	}
};
#endif // CPPLMDb_h__