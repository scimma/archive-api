#!/usr/bin/env python

import base64
import httpx
import re
import secrets
import os

from urllib.request import parse_http_list

from scramp import ScramClient

import bson

class SCRAMAuth(httpx.Auth):
    def __init__(self, username, password, mechanism, shortcut: bool=False, check_final=True):
        self.username = username
        self.password = password
        self.mechanism = mechanism.upper()
        self.shortcut = shortcut
        self.check_final = check_final
    
    def _parse_list_header(self, header: str):
        return [v[1:-1] if v[0] == v[-1] == '"' else v for v in parse_http_list(header)]
    
    def _parse_dict_header(self, header: str):
        def unquote(v: str):
            return v[1:-1] if v[0] == v[-1] == '"' else v
        d = dict()
        for item in self._parse_list_header(header):
            if '=' in item:
                k, v = item.split('=', 1)
                d[k] = unquote(v)
            else:
                d[k] = None
        return d
    
    def _redo_request(self, req: httpx.Request, auth_header: str):
        r = httpx.Request(req.method, req.url, headers=req.headers, cookies=getattr(req,"cookies",None), content=req.content)
        r.headers['Authorization'] = auth_header
        return r
    
    def _set_up_client_first(self):
        self.nonce = secrets.token_urlsafe()
        self.sclient = ScramClient([self.mechanism], 
                                   self.username, self.password, 
                                   c_nonce=self.nonce)
        cfirst = self.sclient.get_client_first()
        print(" client first:",cfirst)
        return base64.b64encode(cfirst.encode("utf-8")).decode('utf-8')
    
    def _handle_first(self, req: httpx.Request, resp: httpx.Response):
        # Need to examine which auth mechanisms the server declares it accepts to find out
        # if the one we can do is on the list
        mechanisms = self._parse_list_header(resp.headers.get("www-authenticate",""))
        matching_mech = False
        for mechanism in mechanisms:
            if mechanism.upper() == self.mechanism or mechanism.upper().startswith(self.mechanism+" "):
                matching_mech = True
                break
        if not matching_mech:
            raise RuntimeError("No auth mechanism matching "+str(mechanisms))
        # At this point we know our mechanism is allowed, so we begin the SCRAM exchange
        
        return self._redo_request(req,
            auth_header=f"{self.mechanism} data={self._set_up_client_first()}")
    
    def _handle_final(self, req: httpx.Request, resp: httpx.Response):
        # To contiue the handshake, the server should have set the www-authenticate header to
        # the mechanism we are using, followed by the data we need to use.
        # Check for this, and isolate the data to parse.
        print("Authenticate header sent by server:",resp.headers.get("www-authenticate"))
        m = re.fullmatch(f"{self.mechanism} (.+)", resp.headers.get("www-authenticate"), flags=re.IGNORECASE)
        if not m:
            raise RuntimeError("Unable to parse www-authenticate header with value: "+resp.headers.get("www-authenticate"))
        auth_data = self._parse_dict_header(m.group(1))
        # Next, make sure that both of the fields we need were actually sent in the dictionary
        if auth_data.get("sid", None) is None:
            raise RuntimeError("Missing sid in SCRAM server first: "+m.group(1))
        if auth_data.get("data", None) is None:
            raise RuntimeError("Missing data in SCRAM server first: "+m.group(1))
        
        self.sid = auth_data.get("sid")
        sfirst = auth_data.get("data")
        print(" sid:",self.sid)
        sfirst=base64.b64decode(sfirst).decode("utf-8")
        print(" server first:",sfirst)
        self.sclient.set_server_first(sfirst)
        cfinal = self.sclient.get_client_final()
        print(" client final:",cfinal)
        cfinal=base64.b64encode(cfinal.encode("utf-8")).decode('utf-8')
        return self._redo_request(req,
            auth_header=f"{self.mechanism} sid={self.sid}, data={cfinal}",)
    
    def _check_server_final(self, r: httpx.Response):
        print("\nfinal response:")
        print("  status:",r.status_code)
        print("  headers:",r.headers)
        # The standard says that we MUST authenticate the server by checking the 
        # ServerSignature, and treat it as an error if they do not match.
        raw_auth = r.headers.get("authentication-info","")
        auth_data = self._parse_dict_header(raw_auth)
        # Next, make sure that both of the fields we need were actually sent in the dictionary
        if auth_data.get("sid", None) is None:
            raise RuntimeError("Missing sid in SCRAM server final: "+raw_auth)
        if auth_data.get("data", None) is None:
            raise RuntimeError("Missing data in SCRAM server final: "+raw_auth)
        if auth_data.get("sid") != self.sid:
            raise RuntimeError("sid mismatch in server final www-authenticate header")
        sfinal=auth_data.get("data")
        self.sclient.set_server_final(base64.b64decode(sfinal).decode("utf-8"))
    
    def auth_flow(self, request):
        if self.shortcut:
            request.headers['Authorization'] = f"{self.mechanism} data={self._set_up_client_first()}"
        resp = yield request
        if not self.shortcut:
            print("\nfirst response:")
            print("  status:",resp.status_code)
            print("  headers:",resp.headers)
            if resp.status_code >= 200 and resp.status_code < 300:
                return
            if resp.status_code == 401:
                if "www-authenticate" in resp.headers:
                    resp = yield self._handle_first(request, resp)
                else:
                    raise RuntimeError(f"Not able to handle response with status {resp.status_code} but no www-authenticate header")
            else:
                raise RuntimeError(f"Not able to handle response with status {resp.status_code} or no www-authenticate header")
        print("\nsecond response:")
        print("  status:",resp.status_code)
        print("  headers:",resp.headers)
        if resp.status_code >= 200 and resp.status_code < 300:
            return
        if resp.status_code == 401 and "www-authenticate" in resp.headers:
            resp = yield self._handle_final(request, resp)
        else:
            raise RuntimeError(f"Not able to handle response with status {resp.status_code} or no www-authenticate header")		
        if self.check_final:
            self._check_server_final(resp)
    
async def do():
    import sys
    async with httpx.AsyncClient() as client:
        msg_id = sys.argv[1]
        url = f"http://localhost:8000/msg/{msg_id}"
        resp = await client.get(
            url,
            auth=SCRAMAuth(
                os.environ['HOP_USERNAME'],
                os.environ['HOP_PASSWORD'],
                "SCRAM-SHA-512",
                shortcut=False,
                check_final=True
            )
        )
        print("Response size:",len(resp.content))
        print("Response content:")
        try:
            print(bson.loads(resp.content))
        except:
            print(resp.content)


import asyncio
asyncio.run(do())
