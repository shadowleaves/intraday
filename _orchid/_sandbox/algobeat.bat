SET PYTHONPATH=k:\algo\code
start python execution.py
pause
start /b python portfolio.py 8004 8005
start /b python algo.py -i10 -s 8002 8004
pause
start /b python heartbeat.py -i10 -s 8001 8002
start /b python streaming_bbg.py -s 8001 8007
cmd