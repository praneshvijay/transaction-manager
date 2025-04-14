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

using namespace std;

#define READ_UNCOMMITED 100
#define READ_COMMITED 101
#define REPEATABLE_READ 102
#define SERIALIZABLE 103

// Special structure implemented for database
class Database_Struct {
    public:
        // int recent_write;               // Change it to special structure
        deque<int> recent_write;
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
    
    public:
        Database(): last_transaction(-1), next_transaction(1) {}

        int get_transaction() {
            lock_guard<mutex> lock(db_mutex);
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

                auto it = data[key].commit_value.lower_bound(transaction_id);
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

        void write(int key, int value, int transact_id) {
            lock_guard<mutex> lock(db_mutex);
            data[key].commit_value[transact_id] = value;
            data[key].recent_write.push_back(transact_id);  
        }

        int fetch_last_write(int key) {
            lock_guard<mutex> lock(db_mutex);
            return data[key].recent_write.back();
        }

        void commit() {

        }

        void rollback() {

        }

        void finish_transaction(int transaction_id) {
            lock_guard<mutex> lock(db_mutex);
            last_transaction = max(transaction_id, last_transaction);
        }
};

void GarbageCollector() {
    // Implemented later for database
}

// Transaction Manager class
class TransactionManager {
    private:
        int transaction_id;
        Database &db;           // Global database
        int isolation_level;
        int last_commit_transaction;

        int finished;

        map<int, int> change_logs;
        map<int, int> last_write;
    
    public:
        TransactionManager(Database&, int);
        
        int read(int );
        
        void write(int, int);

        void commit();

        void rollback();
};

#endif