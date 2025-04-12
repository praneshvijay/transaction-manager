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

using namespace std;

#define READ_UNCOMMITED 100
#define READ_COMMITED 101
#define REPEATABLE_READ 102
#define SERIALIZABLE 103

// Special structure implemented for database
class Database_Struct {
    public:
        int recent_write;
        map<int, int> commit_value;

        Database_Struct(): recent_write(-1) {
            commit_value.clear();
        }
};

// Reference database (Implemented here completely, will be replaced by TOY DBMS)
class Database {
    private:
        map<int, Database_Struct> data;
        int last_transaction;
        int next_transaction;   // Global transaction ID
        
        mutex db_mutex, trans_id_mutex;
    
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

        int read() {
            lock_guard<mutex> lock(db_mutex);
        }

        void write(int key, int value, int transact_id) {
            lock_guard<mutex> lock(db_mutex);
            data[key].commit_value[transact_id] = value;
            data[key].recent_write = transact_id;
        }

        int fetch_last_write(int key) {
            lock_guard<mutex> lock(db_mutex);
            return data[key].recent_write;
        }

        void commit() {

        }

        void rollback() {

        }

        // int read(const int& key) {
        //     if (!_serializable_ts) lock_guard<mutex> lock(db_mutex);
        //     return data[key][recent_commit];
        // }

        // int read_commit(const int& key) {
        //     if (!_serializable_ts) lock_guard<mutex> lock(db_mutex);
        //     return data[key][recent_commit];
        // }

        // void write(const int& key, int value, int transaction_id) {
        //     if (!_serializable_ts) lock_guard<mutex> lock(db_mutex);
        //     data[key][recent_commit] = value;            // Update main data
        // }
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

        map<int, int> change_logs;
        map<int, int> last_write;
    
    public:
        TransactionManager(Database&, int);
        
        template<typename... Args>
        int read(Args... args);
        
        void write(int, int);

        void commit();

        void rollback();
};

#endif