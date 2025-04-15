#include "transaction_manager.hpp"
#include <unistd.h>
#include <thread>

TransactionManager::TransactionManager(Database& db, int il): db(db),  isolation_level(il), last_commit_transaction(0), finished(0) {
    transaction_id = db.get_transaction();
    if (il == REPEATABLE_READ){
        last_commit_transaction = db.get_last_transact();
        db.set_required_commit(last_commit_transaction);
    }
}

TransactionManager::~TransactionManager() {
    rollback();
}

int TransactionManager::read(int key) {
    if (finished) return 0;

    switch(isolation_level) {
        case READ_UNCOMMITED:{
            pair<int, int> val = db.read(key, -1);
            cout<<"T"<<transaction_id<<" reads "<<val.first<<endl;
            return val.first;
        }
        case READ_COMMITED:{
            pair<int, int> val = db.read(key, -2);
            
            if (val.second == last_write[key]) {
                auto it = change_logs.lower_bound(key);
                if (it != change_logs.end()) val.first = change_logs[key];
            }
            cout<<"T"<<transaction_id<<" reads "<<val.first<<endl;
            return val.first;
        }
        case SERIALIZABLE:{
            int sleep_time = 1;
            while(1){
                if(db.get_last_transact() == transaction_id - 1) break;
                sleep(sleep_time);
                sleep_time++;
            }
            pair<int, int> val;
            auto it = change_logs.lower_bound(key);
            if (it != change_logs.end()) val.first = change_logs[key];
            else {
                val = db.read(key, transaction_id);
            }
            cout<<"T"<<transaction_id<<" reads "<<val.first<<endl;
            return val.first;
        }
        case REPEATABLE_READ:{
            pair<int, int> val;
            auto it = change_logs.lower_bound(key);
            if (it != change_logs.end()) val.first = change_logs[key];
            else {
                val = db.read(key, last_commit_transaction);
            }
            cout<<"T"<<transaction_id<<" reads "<<val.first<<endl;
            return val.first;
            break;
        }
    }

    return 0;
}

void TransactionManager::write(int key, int value) {
    if (finished) return;
    
    switch(isolation_level) {
        case READ_UNCOMMITED:
            db.write(key, value, transaction_id);
            change_logs[key] = value;
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
    cout <<"T"<<transaction_id<<" writes "<<value<<endl;
}

void TransactionManager::commit() {
    if (finished) return;

    if(isolation_level == READ_COMMITED){
        db.lock_mutex();
        int flag = 0;
        for(auto &[x, y]: change_logs) {
            if(last_write[x] != db.fetch_last_write(x, 1)){
                flag = 1;
                break;
            }
        }
        if(!flag){
            for(auto &[x, y]: change_logs) {
                db.write(x, y, transaction_id, 1);
                db.commit(x, transaction_id, 1);
            }
            cout<< "T" << transaction_id << " committed\n";
        }
        else {
            cout<< "T" << transaction_id << " aborted\n";
        }
        db.unlock_mutex();
        db.finish_transaction(transaction_id);

        finished = 1;
    }
    else {
        if(isolation_level == SERIALIZABLE){
            int sleep_time = 1;
            while(1){
                if(db.get_last_transact() == transaction_id - 1) break;
                sleep(sleep_time);
                sleep_time++;
            }
        }

        for(auto &[x, y]: change_logs) {
            if (isolation_level != READ_UNCOMMITED) db.write(x, y, transaction_id);
            db.commit(x, transaction_id);
        }
    
        last_write.clear();
        change_logs.clear();
        if(isolation_level == REPEATABLE_READ){
            db.remove_required_commit(last_commit_transaction);
        }
        db.finish_transaction(transaction_id);
    
        finished = 1;
        cout<< "T" << transaction_id << " committed\n";
    }
}

void TransactionManager::rollback() { 
    if (finished) return;

    if(isolation_level == SERIALIZABLE){
        int sleep_time = 1;
        while(1){
            if(db.get_last_transact() == transaction_id - 1) break;
            sleep(sleep_time);
            sleep_time++;
        }
    }
    
    if (isolation_level == READ_UNCOMMITED) {
        // Remove changes made in database
        db.rollback(transaction_id);
        cout<< "T" << transaction_id << " rolled back\n";
    }   

    last_write.clear();
    change_logs.clear();
    db.finish_transaction(transaction_id);

    finished = 1;
}