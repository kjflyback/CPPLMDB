CppLMDb lmdb;
	HRESULT hr = lmdb.init(L"test.lmdb", 1024, 0x94000);
	ATLASSERT(SUCCEEDED(hr));
	

	int k = 0;
	for(int i = 0;i<100000;i++){
		CppLMDb::transblock tb(lmdb);
		CDBRecord record(lmdb, tb);
		record.Set(&i, &i);
	}
	CppLMDb::transblock tb(lmdb);
	CppLMDb::enumdata * pdata = NULL;
	if(S_OK == lmdb.Get(&tb, NULL, 0, &pdata)){
		int * pkey = NULL;
		DWORD dwSize = 0;
		int * pval = NULL;
		while(pdata->next((void**)&pkey, &dwSize, (void**)&pval, &dwSize)){
			if(*pkey && *pval)
				std::wcout<<*pkey<<L" = "<<*pval<<L"..";
			Sleep(10);
		}
		pdata->close();
	}
