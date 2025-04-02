#include "transaction_manager.hpp"

/*
TO be implemented -
    === Important - research pending ===
    -> Snapshot creation
    -> Read Commited - (Deadlock detection)

    === Small tasks ===
    -> Rollback
*/

TransactionManager::TransactionManager(Database& db, int il = READ_UNCOMMITED): db(db),  isolation_level(il) {
    if (il == REPEATABLE_READ) {
        snapshot = db;
    } else if (il == SERIALIZABLE) {
        db.lock_mutex_s();
    }
}

template<typename... Args>
void TransactionManager::read(Args... args) {
    switch(isolation_level) {
        case READ_UNCOMMITED:
            db.read(args...);
            break;
        case READ_COMMITED:
            db.read_commit(args...);
            break;
        case SERIALIZABLE:
            db.read(args...);
            break;
        case REPEATABLE_READ:
            snapshot.read(args...);
            break;
    }
}

template<typename... Args>
void TransactionManager::write(Args... args) {
    switch(isolation_level) {
        case READ_UNCOMMITED:
            db.write(args);
            break;
        case READ_COMMITED:
            db.write(args);
            break;
        case SERIALIZABLE:
            db.write(args);
            break;
        case REPEATABLE_READ:
            db.write(args);
            break;
    }
}

void TransactionManager::commit() {
    if (isolation_level == SERIALIZABLE) {
        db.unlock_mutex_s();
    }
}

int main() {
    Database db;

    return 0;
}