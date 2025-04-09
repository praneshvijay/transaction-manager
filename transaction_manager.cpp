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

TransactionManager::TransactionManager(Database& db, int il = READ_UNCOMMITED): db(db),  isolation_level(il) {
    if (il == REPEATABLE_READ) {
        snapshot = db;
    } else if (il == SERIALIZABLE) {
        db.lock_mutex_s();
    }
}

template<typename... Args>
int TransactionManager::read(Args... args) {
    switch(isolation_level) {
        case READ_UNCOMMITED:
            return db.read(args...);
            break;
        case READ_COMMITED:
            return db.read_commit(args...);
            break;
        case SERIALIZABLE:
            return db.read(args...);
            break;
        case REPEATABLE_READ:
            return snapshot.read(args...);
            break;
    }

    return 0;
}

template<typename... Args>
void TransactionManager::write(Args... args) {
    switch(isolation_level) {
        case READ_UNCOMMITED:
            db.write(args...);
            break;
        case READ_COMMITED:
            db.write(args...);
            break;
        case SERIALIZABLE:
            db.write(args...);
            break;
        case REPEATABLE_READ:
            snapshot.write(args...);
            break;
    }
}

void TransactionManager::commit() {
    if (isolation_level == SERIALIZABLE) {
        db.unlock_mutex_s();
    }
}