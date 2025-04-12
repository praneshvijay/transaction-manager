#include "transaction_manager.hpp"
#include <unistd.h>
#include <thread>

/*
TO be implemented -
    === Important - research pending ===
    -> Database structure, Garbage collector process
    -> Read exact commit value
    -> Rollback

    -> Read Commited - (Deadlock detection)
    -> Repeatable Read - (Optimized snapshot creation, Commit)
*/

TransactionManager::TransactionManager(Database& db, int il = READ_UNCOMMITED): db(db),  isolation_level(il), last_commit_transaction(0), finished(0) {
    transaction_id = db.get_transaction();
    if (il == SERIALIZABLE) last_commit_transaction = db.get_last_transact();
}

template<typename... Args>
int TransactionManager::read(Args... args) {
    if (finished) return 0;

    switch(isolation_level) {
        case READ_UNCOMMITED:
            return db.read(args...);
            break;
        case READ_COMMITED:
            return db.read(args...);
            break;
        case SERIALIZABLE:
            return db.read(args...);
            break;
        case REPEATABLE_READ:
            return db.read(args...);
            break;
    }

    return 0;
}

void TransactionManager::write(int key, int value) {
    if (finished) return;
    
    switch(isolation_level) {
        case READ_UNCOMMITED:
            db.write(key, value, transaction_id);
            break;
        case READ_COMMITED:
            last_write[key] = db.fetch_last_write(key);
            change_logs[key] = value;
            break;
        case SERIALIZABLE:
            change_logs[key] = value;
            break;
        case REPEATABLE_READ:
            change_logs[key] = value;
            break;
    }
}

void TransactionManager::commit() {
    if (finished) return;

    switch(isolation_level) {
        case READ_UNCOMMITED:
            break;
        case READ_COMMITED:
            break;
        case SERIALIZABLE:
            break;
        case REPEATABLE_READ:
            break;
    }

    last_write.clear();
    change_logs.clear();
    db.finish_transaction(transaction_id);

    finished = 1;
}

void TransactionManager::rollback() {
    if (finished) return;

    if (isolation_level == READ_UNCOMMITED) {
        // Remove changes made in database
    }

    last_write.clear();
    change_logs.clear();
    db.finish_transaction(transaction_id);

    finished = 1;
}