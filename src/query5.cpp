#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--r_name" && i + 1 < argc) {
            r_name = argv[++i];
        } else if (arg == "--start_date" && i + 1 < argc) {
            start_date = argv[++i];
        } else if (arg == "--end_date" && i + 1 < argc) {
            end_date = argv[++i];
        } else if (arg == "--threads" && i + 1 < argc) {
            num_threads = std::stoi(argv[++i]);
        } else if (arg == "--table_path" && i + 1 < argc) {
            table_path = argv[++i];
        } else if (arg == "--result_path" && i + 1 < argc) {
            result_path = argv[++i];
        }
    }

    if (r_name.empty() || start_date.empty() || end_date.empty() || num_threads <= 0 || table_path.empty() || result_path.empty()) {
        std::cerr << "Usage: " << argv[0] << " --r_name <name> --start_date <date> --end_date <date> --threads <num> --table_path <path> --result_path <path>" << std::endl;
        return false;
    }
    return true;
}

// Helper to split string by delimiter
std::vector<std::string> split(const std::string& s, char delimiter) {
   std::vector<std::string> tokens;
   std::string token;
   std::istringstream tokenStream(s);
   while (std::getline(tokenStream, token, delimiter)) {
      tokens.push_back(token);
   }
   return tokens;
}

// Helper to load a table
bool loadTable(const std::string& path, const std::vector<std::string>& columns, const std::vector<int>& col_indices, std::vector<std::map<std::string, std::string>>& data) {
    std::ifstream file(path);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << path << std::endl;
        return false;
    }
    std::string line;
    while (std::getline(file, line)) {
        std::vector<std::string> values = split(line, '|');
        std::map<std::string, std::string> row;
        for (size_t i = 0; i < columns.size(); ++i) {
            if (col_indices[i] < values.size()) {
                row[columns[i]] = values[col_indices[i]];
            }
        }
        data.push_back(row);
    }
    std::cout << "Loaded " << data.size() << " rows from " << path << std::endl;
    return true;
}

// Function to read TPCH data from the specified paths
bool readTPCHData(const std::string& table_path, std::vector<std::map<std::string, std::string>>& customer_data, std::vector<std::map<std::string, std::string>>& orders_data, std::vector<std::map<std::string, std::string>>& lineitem_data, std::vector<std::map<std::string, std::string>>& supplier_data, std::vector<std::map<std::string, std::string>>& nation_data, std::vector<std::map<std::string, std::string>>& region_data) {
    std::string sep = (table_path.back() == '/' || table_path.back() == '\\') ? "" : "/";
    
    // Region: r_regionkey(0), r_name(1)
    if (!loadTable(table_path + sep + "region.tbl", {"r_regionkey", "r_name"}, {0, 1}, region_data)) return false;

    // Nation: n_nationkey(0), n_name(1), n_regionkey(2)
    if (!loadTable(table_path + sep + "nation.tbl", {"n_nationkey", "n_name", "n_regionkey"}, {0, 1, 2}, nation_data)) return false;

    // Supplier: s_suppkey(0), s_nationkey(3)
    if (!loadTable(table_path + sep + "supplier.tbl", {"s_suppkey", "s_nationkey"}, {0, 3}, supplier_data)) return false;

    // Customer: c_custkey(0), c_nationkey(3)
    if (!loadTable(table_path + sep + "customer.tbl", {"c_custkey", "c_nationkey"}, {0, 3}, customer_data)) return false;

    // Orders: o_orderkey(0), o_custkey(1), o_orderdate(4)
    if (!loadTable(table_path + sep + "orders.tbl", {"o_orderkey", "o_custkey", "o_orderdate"}, {0, 1, 4}, orders_data)) return false;

    // Lineitem: l_orderkey(0), l_suppkey(2), l_extendedprice(5), l_discount(6)
    if (!loadTable(table_path + sep + "lineitem.tbl", {"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"}, {0, 2, 5, 6}, lineitem_data)) return false;

    return true;
}

#include <unordered_map>
#include <iomanip>
#include <vector>

// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads, const std::vector<std::map<std::string, std::string>>& customer_data, const std::vector<std::map<std::string, std::string>>& orders_data, const std::vector<std::map<std::string, std::string>>& lineitem_data, const std::vector<std::map<std::string, std::string>>& supplier_data, const std::vector<std::map<std::string, std::string>>& nation_data, const std::vector<std::map<std::string, std::string>>& region_data, std::map<std::string, double>& results) {
    
    // 1. Filter Region
    std::string target_region_key = "";
    for (const auto& row : region_data) {
        if (row.at("r_name") == r_name) {
            target_region_key = row.at("r_regionkey");
            break;
        }
    }
    if (target_region_key.empty()) return false;

    // 2. Filter Nation by Region
    std::unordered_map<std::string, std::string> nation_key_to_name;
    for (const auto& row : nation_data) {
        if (row.at("n_regionkey") == target_region_key) {
            nation_key_to_name[row.at("n_nationkey")] = row.at("n_name");
        }
    }

    // 3. Index Supplier (filtered by Nation)
    std::unordered_map<std::string, std::string> supp_key_to_nation;
    for (const auto& row : supplier_data) {
        if (nation_key_to_name.count(row.at("s_nationkey"))) {
            supp_key_to_nation[row.at("s_suppkey")] = row.at("s_nationkey");
        }
    }

    // 4. Index Customer (filtered by Nation)
    std::unordered_map<std::string, std::string> cust_key_to_nation;
    for (const auto& row : customer_data) {
        if (nation_key_to_name.count(row.at("c_nationkey"))) {
            cust_key_to_nation[row.at("c_custkey")] = row.at("c_nationkey");
        }
    }

    // 5. Index Orders (filtered by Date and Customer)
    std::unordered_map<std::string, std::string> order_key_to_cust;
    for (const auto& row : orders_data) {
        const std::string& o_date = row.at("o_orderdate");
        if (o_date >= start_date && o_date < end_date) {
            const std::string& o_cust = row.at("o_custkey");
            if (cust_key_to_nation.count(o_cust)) {
                order_key_to_cust[row.at("o_orderkey")] = o_cust;
            }
        }
    }

    // 6. Process Lineitem (Multithreaded)
    std::vector<std::thread> worker_threads;
    std::vector<std::map<std::string, double>> thread_results(num_threads);
    size_t total_lines = lineitem_data.size();
    size_t chunk_size = (total_lines + num_threads - 1) / num_threads;

    auto worker = [&](int thread_id, size_t start, size_t end) {
        for (size_t i = start; i < end; ++i) {
            const auto& row = lineitem_data[i];
            std::string l_orderkey = row.at("l_orderkey");
            
            auto it_order = order_key_to_cust.find(l_orderkey);
            if (it_order == order_key_to_cust.end()) continue;
            std::string c_custkey = it_order->second;

            std::string l_suppkey = row.at("l_suppkey");
            auto it_supp = supp_key_to_nation.find(l_suppkey);
            if (it_supp == supp_key_to_nation.end()) continue;
            std::string s_nationkey = it_supp->second;

            // Join condition: c_nation == s_nation
            std::string c_nationkey = cust_key_to_nation[c_custkey]; // Guaranteed to exist if order in map
            if (c_nationkey == s_nationkey) {
                double revenue = std::stod(row.at("l_extendedprice")) * (1.0 - std::stod(row.at("l_discount")));
                thread_results[thread_id][nation_key_to_name[c_nationkey]] += revenue;
            }
        }
    };

    for (int i = 0; i < num_threads; ++i) {
        size_t start = i * chunk_size;
        size_t end = std::min(start + chunk_size, total_lines);
        worker_threads.push_back(std::thread(worker, i, start, end));
    }

    for (size_t i = 0; i < worker_threads.size(); ++i) {
        if (worker_threads[i].joinable()) {
            worker_threads[i].join();
        }
    }

    // Aggregate results
    for (const auto& t_res : thread_results) {
        for (const auto& kv : t_res) {
            results[kv.first] += kv.second;
        }
    }

    return true;
}

// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    std::vector<std::pair<std::string, double>> sorted_results(results.begin(), results.end());
    // Sort by revenue descending
    std::sort(sorted_results.begin(), sorted_results.end(), [](const auto& a, const auto& b) {
        return a.second > b.second;
    });

    std::ofstream out(result_path);
    if (!out.is_open()) {
        std::cerr << "Failed to open result file: " << result_path << std::endl;
        return false;
    }

    out << "n_name,revenue\n";
    for (const auto& kv : sorted_results) {
        out << kv.first << "," << std::fixed << std::setprecision(4) << kv.second << "\n";
    }
    out.close();
    return true;
} 