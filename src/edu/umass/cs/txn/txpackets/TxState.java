package edu.umass.cs.txn.txpackets;

public enum TxState {
    INIT,

    ABORTED,

    COMPLETE,

    COMMITTED;
}