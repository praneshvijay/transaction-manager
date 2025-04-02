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

// Reference database (Implemented here completely, will be replaced by TOY DBMS)
class Database {
    private:
        map<string, int> data;           // Main data store
        map<string, vector<int>> deltas; // Delta changes per key
        mutex db_mutex;
        int _serializable_ts = 0; // Timestamp for Serializable transactions
    
    public:
        Database() {
            data["key1"] = 100;
            data["key2"] = 200;
        }

        void lock_mutex_s(){
            _serializable_ts++;
            db_mutex.lock();
        }

        void unlock_mutex_s(){
            _serializable_ts--;
            db_mutex.unlock();
        }

        int read(const string& key) {
            if (!_serializable_ts) lock_guard<mutex> lock(db_mutex);
            return data[key];
        }

        int read_commit(const string& key) {
            if (!_serializable_ts) lock_guard<mutex> lock(db_mutex);
            return data[key];
        }

        void write(const string& key, int value, int transaction_id) {
            if (!_serializable_ts) lock_guard<mutex> lock(db_mutex);
            if (deltas.find(key) == deltas.end()) {
                deltas[key] = vector<int>();
            }
            deltas[key].push_back(value); // Store delta
            data[key] = value;            // Update main data
        }

        void operator=(const Database& other) {
            lock_guard<mutex> lock(db_mutex);
            data = other.data; // Copy main data
            deltas = other.deltas; // Copy deltas
        }
};

// Transaction Manager class
class TransactionManager {
    private:
        Database &db; // Global database
        Database snapshot; // CoW (Implemented as a complete copy)
        int isolation_level;
    
    public:
        TransactionManager(Database&, int);
        
        template<typename... Args>
        void read(Args... args);
        
        template<typename... Args>
        void TransactionManager::write(Args... args);

        void TransactionManager::commit();
};

#endif