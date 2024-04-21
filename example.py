import time
import pycurse

downloader = pycurse.CurlDownloader()
downloader.add_request("http://example.com/")
# time.sleep(0.5)
# downloader.add_request("http://ifconfig.me/ip")

response = downloader.fetch(5000)
print(response)
