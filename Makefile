
server:
	ant
	rm -f tmp/*
	rm -rf paxos_logs/*
	rm -rf reconfiguration_DB/*
	./bin/gpServer.sh stop all
	ant
	./bin/gpServer.sh start all

serverd:
	ant
	rm -f tmp/*
	rm -rf paxos_logs/*
	rm -rf reconfiguration_DB/*
	./bin/gpServer.sh stop all
	ant
	./bin/gpServer.sh -debug start all


client:
	./bin/gpClient.sh edu.umass.cs.txn.TxnClient

kill:
	ps | grep java | awk 'BEGIN {}{print $$1}' | xargs  kill -9

test:
	rm -f tmp/*
	rm -rf paxos_logs/*
	rm -rf reconfiguration_DB/*
	ant runtest -Dtest=edu.umass.cs.txn.testing.SerializabilityTest
