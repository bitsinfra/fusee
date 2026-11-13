"""Retry a result upload using a short-lived grant from this instance's metadata."""
import http.client
from pathlib import Path
import sys
import time
import urllib.parse
import urllib.request
from cryptography.fernet import Fernet


def upload(path, original, secret):
    metadata = "http://169.254.169.254/latest/"
    allowed = urllib.parse.urlsplit(original)
    request = urllib.request.Request(metadata+"api/token",method="PUT",
                                     headers={"X-aws-ec2-metadata-token-ttl-seconds":"600"})
    with urllib.request.urlopen(request,timeout=5) as response:
        token=response.read().decode()
    def tag(name):
        query=urllib.request.Request(metadata+"meta-data/tags/instance/"+name,headers={"X-aws-ec2-metadata-token":token})
        with urllib.request.urlopen(query,timeout=3) as response:
            return response.read()
    deadline=time.monotonic()+150
    while time.monotonic()<deadline:
        try:
            count=int(tag("FuseeGrantParts"))
            if not 0<count<=40:
                raise ValueError("bad grant count")
            encrypted=b"".join(tag(f"FuseeGrant{i:02d}") for i in range(count))
            url=urllib.parse.urlsplit(Fernet(secret).decrypt(encrypted,ttl=600).decode())
            if (url.scheme,url.netloc,url.path)!=(allowed.scheme,allowed.netloc,allowed.path) or url.scheme!="https":
                raise ValueError("grant destination changed")
            connection=http.client.HTTPSConnection(url.hostname,timeout=60)
            try:
                with path.open("rb") as data:
                    connection.request("PUT",url.path+"?"+url.query,body=data,
                                       headers={"Content-Length":str(path.stat().st_size)})
                result=connection.getresponse()
                result.read()
                if result.status in (200,201,204):
                    return
                print(f"refreshed upload HTTP {result.status}",flush=True)
            finally:
                connection.close()
        except Exception as exc:
            # Exceptions may contain a signed URL. Record only their type.
            print(f"waiting for an authenticated upload grant ({type(exc).__name__})",flush=True)
        time.sleep(5)
    raise RuntimeError("no valid result upload before deadline")


if __name__=="__main__":
    upload(Path(sys.argv[1]),sys.argv[2],sys.argv[3].encode())
