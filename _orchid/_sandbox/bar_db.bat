SET PYTHONPATH=k:\algo\code
start /b python bar_storage.py 8006
start /b python heartbeat.py -s 8003 8006
start /b python streaming_bbg.py -s 8003
cmd