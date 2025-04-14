#pragma once

#ifndef _TRANSACTION_MANAGER_HPP_
#define _TRANSACTION_MANAGER_HPP_
#include <iostream>
#include <vector>
#include <map>
#include <string>
#include <chrono>
#include <fstream>
#include <mutex>
#include <deque>
#include <set>

using namespace std;

#define READ_UNCOMMITED 100
#define READ_COMMITED 101
#define REPEATABLE_READ 102
#define SERIALIZABLE 103

// Special structure implemented for database
class Database_Struct {
    public:
        int recent_commit;   // Sequence of commit            
        deque<int> recent_write;    // Sequence of writes (among uncommited transactions)
        map<int, int> commit_value;

        Database_Struct(){
            recent_write.clear();
            recent_write.push_back(-1);
            commit_value.clear();
        }
};

// Reference database (Implemented here completely, will be replaced by TOY DBMS)
class Database {
    private:
        map<int, Database_Struct> data;
        int last_transaction;
        int next_transaction;   // Global transaction ID
        
        mutex db_mutex;

        set<int> live_transactions;
    
    public:
        Database(): last_transaction(0), next_transaction(1) {}

        int get_transaction() {
            lock_guard<mutex> lock(db_mutex);
            live_transactions.insert(next_transaction);
            return next_transaction++;
        }

        int get_last_transact() {
            lock_guard<mutex> lock(db_mutex);
            return last_transaction;
        }

        pair<int, int> read(int key, int transaction_id) {
            lock_guard<mutex> lock(db_mutex);
            pair<int, int> val;
            if (transaction_id > -1) {
                auto it1 = data.lower_bound(key);
                if (it1 == data.end()) {
                    val.second = 0;
                }

                auto it = data[key].commit_value.lower_bound(transaction_id+1);
                if (it == data[key].commit_value.begin()) {
                    val.second = 0;
                } else {
                    val.second = 1;
                    it--;
                    val.first = it->second;
                }
            } else {
                auto it1 = data.lower_bound(key);
                if (it1 == data.end()) {
                    val.second = 0;
                } else {
                    val.second = data[key].recent_write.back();
                    val.first = data[key].commit_value[data[key].recent_write.back()];
                }
            }

            return val;
        }

        void write(int key, int value, int transact_id, int bypass = 0) {
            if(!bypass) lock_guard<mutex> lock(db_mutex);
            data[key].commit_value[transact_id] = value;
            data[key].recent_write.push_back(transact_id);  
        }

        int fetch_last_write(int key, int bypass = 0) {
            if(!bypass) lock_guard<mutex> lock(db_mutex);
            return data[key].recent_write.back();
        }

        void commit(int key, int transaction_id, int bypass = 0) {
            if(!bypass) lock_guard<mutex> lock(db_mutex);
            data[key].recent_commit = transaction_id;
        }

        void rollback(int transaction_id) {
            lock_guard<mutex> lock(db_mutex);
            
            auto it = data.begin();
            while (it != data.end()) {
                auto it1 = it->second.commit_value.lower_bound(transaction_id);
                if (it1 != it->second.commit_value.end()) {
                    it->second.commit_value.erase(it1, it->second.commit_value.end());
                }
                ++it;
            }

            // remove from deque recent_write
            for (auto &pair : data) {
                auto &recent_write = pair.second.recent_write;
                for (auto it = recent_write.begin(); it != recent_write.end(); ) {
                    if (*it == transaction_id) {
                        it = recent_write.erase(it);
                    } else {
                        ++it;
                    }
                }
            }
        }

        void finish_transaction(int transaction_id) {
            lock_guard<mutex> lock(db_mutex);
            live_transactions.erase(transaction_id);
            last_transaction = max(transaction_id, last_transaction);
        }

        void lock_mutex() {
            db_mutex.lock();
        }

        void unlock_mutex() {
            db_mutex.unlock();
        }

        void garbage_collector(){
            int min_live = live_transactions.empty() ? next_transaction : *live_transactions.begin();

            for (auto &pair : data) {
                auto &recent_write = pair.second.recent_write;
                for (auto it = recent_write.begin(); it != recent_write.end(); ) {
                    if (*it != pair.second.recent_commit && *it < min_live) {
                        it = recent_write.erase(it);
                    } else {
                        ++it;
                    }
                }

                auto &commit_values = pair.second.commit_value;
                for (auto it = commit_values.begin(); it != commit_values.end(); ) {
                    if (it->first < min_live && it->first != pair.second.recent_commit) {
                        it = commit_values.erase(it);
                    } else {
                        ++it;
                    }
                }
            }

        }

};

// Transaction Manager class
class TransactionManager {
    private:
        Database &db;           // Global database
        int isolation_level;
        int transaction_id;
        int last_commit_transaction;
    
        int finished;
        
        map<int, int> change_logs;
        map<int, int> last_write;
    
    public:
        TransactionManager(Database&, int);

        ~TransactionManager();
        
        int read(int );
        
        void write(int, int);

        void commit();

        void rollback();
};

#endif