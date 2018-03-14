
server:
	./bin/gpServer.sh stop all
	ant
	rm -f tmp/*
	rm -rf paxos_logs/*
	rm -rf reconfiguration_DB/*
	ant
	./bin/gpServer.sh -DgigapaxosConfig=src/edu/umass/cs/txn/testing/gigapaxos.properties start all

serverd:
	./bin/gpServer.sh stop all
	ant
	rm -f tmp/*
	rm -rf paxos_logs/*
	rm -rf reconfiguration_DB/*
	ant
	./bin/gpServer.sh -DgigapaxosConfig=src/edu/umass/cs/txn/testing/gigapaxos.properties -debug start all


client:
	./bin/gpClient.sh edu.umass.cs.txn.testing.TxnClient

kill:
	ps -e| grep java | awk 'BEGIN {}{print $$1}' | xargs  kill -9

test:
	rm -f tmp/*
	rm -rf paxos_logs/*
	rm -rf reconfiguration_DB/*
	ant runtest -Dtest=edu.umass.cs.txn.testing.SerializabilityTest
