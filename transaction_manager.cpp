#include "transaction_manager.hpp"
#include <unistd.h>
#include <thread>

/*
TO be implemented -
    -> READ UNCOMMITED -
        => read (check)
        => rollback
    
    -> READ COMMITED -
        => read
    
    -> SERIALIZABLE
        => read
    
    -> REPEATABLE READ
        => read
    
    -> Check which isolation levels should discard on conflicting commits
*/

/*
Main structure changes ->
    -> last write for READ_UNCOMMITED
*/

TransactionManager::TransactionManager(Database& db, int il = READ_UNCOMMITED): db(db),  isolation_level(il), last_commit_transaction(0), finished(0) {
    transaction_id = db.get_transaction();
    if (il == REPEATABLE_READ) last_commit_transaction = db.get_last_transact();
}

int TransactionManager::read(int key) {
    if (finished) return 0;

    switch(isolation_level) {
        case READ_UNCOMMITED:
            pair<int, int> val = db.read(key, -1);
            return val.first;
        case READ_COMMITED:
            pair<int, int> val = db.read(key, -2);
            
            if (val.second == last_write[key]) {
                auto it = change_logs.lower_bound(key);
                if (it != change_logs.end()) val.first = change_logs[key];
            }

            return val.first;
        case SERIALIZABLE:
            pair<int, int> val = db.read(key, transaction_id);
            
            if (val.second) {
                auto it = change_logs.lower_bound(key);
                if (it != change_logs.end()) val.first = change_logs[key];
            }

            return val.first;
        case REPEATABLE_READ:
            pair<int, int> val;
            auto it = change_logs.lower_bound(key);
            if (it != change_logs.end()) val.first = change_logs[key];
            else {
                val = db.read(key, last_commit_transaction);
            }

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

    if (isolation_level != READ_UNCOMMITED) {
        for(auto &[x, y]: change_logs) {
            db.write(x, y, transaction_id);
        }
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