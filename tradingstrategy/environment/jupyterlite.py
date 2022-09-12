from asyncio import get_running_loop

try:
    import js
except ImportError:
    print("You are not currently using JupyterLite environment")


class IndexDB:
    def __init__(self,db_name="trading-strategy-config"):
        self.db=None
        self.open_request = js.indexedDB.open(db_name, 2);
        self.open_request.onsuccess = self.db_success
        self.open_request.onupgradeneeded = self.db_init
        self.open_request.onerror = self.db_error
        self._db_open=get_running_loop().create_future()     

    async def set_file(self,name,content):
        opened=await self._db_open
        if opened:
            t=self.db.transaction("files","readwrite")
            files=t.objectStore("files")
            request=files.put(content,name)
            request.onsuccess=lambda *args:self._request_future.set_result(True) 
            request.onerror=lambda *args:self._request_future.set_result(False)
            self._request_future=get_running_loop().create_future()
            req_success=await self._request_future
            return req_success
        return False
        
    async def get_file(self,name):
        opened=await self._db_open
        if opened:
            t=self.db.transaction("files","readonly")
            files=t.objectStore("files")
            request=files.get(name)
            request.onsuccess=lambda *args:self._request_future.set_result(True) 
            request.onerror=lambda *args:self._request_future.set_result(False)
            self._request_future=get_running_loop().create_future()
            req_success=await self._request_future
            if req_success:
                content=request.result
                return content
        return None

    def db_init(self,*args):
        self.db=self.open_request.result
        self.db.createObjectStore("files")

    def db_success(self,*args):
        self.db=self.open_request.result
        self._db_open.set_result(True)

    def db_error(self,*args):
        self._db_open.set_result(False)
