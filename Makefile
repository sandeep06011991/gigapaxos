
server:
	rm -f tmp/*
	rm -rf paxos_logs/*
	rm -rf reconfiguration_DB/*
	./bin/gpServer.sh stop all
	ant
	./bin/gpServer.sh -debug start all

client:
	./bin/gpClient.sh edu.umass.cs.txn.TxnClient
