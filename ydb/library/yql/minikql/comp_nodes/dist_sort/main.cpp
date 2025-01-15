#include <iostream>
#include <fstream>
#include <vector>
#include <thread>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <condition_variable>
#include <chrono>
#include <map>

#include "algorithms/distributed_partitioning.hpp"
#include "algorithms/helpers.hpp"
#include "algorithms/binary_search.hpp"
#include "algorithms/tlx/sort/strings.hpp"
#include "algorithms/tlx/algorithm.hpp"

#include "algorithms/merge.cpp"

using std::cout;
using std::endl;
using std::map;
using std::setw;
using std::thread;

vector<size_t> NODES = {4}; // vector of various number of nodes which to run the algorithm for
size_t NUM_NODES = 4;          // number of nodes for the current iteration
unordered_map<size_t, size_t> GROUPS = {{0, 1}, {1, 1}, {2, 2}, {3, 2}, {4, 2}, {5, 2}};

vector<size_t> factors = {1}; // buckets multipliers, ex.: NUM_NODES = 4, factor = 2 -> 8 buckets per node
size_t f = 1;                 // buckets multiplier for the current iteration
size_t p = f * NUM_NODES;     // number of buckets
size_t s = p - 1;

// !!! ПОМЕНЯТЬ НАЗВАНИЕ В SAVE_FILE !!!
// map<size_t, const char *> LEN_TO_FILENAME = {{20, "data_1M_20.txt"},{50, "data_1M_50.txt"},{100, "data_1M_100.txt"} /* { 50, "data/data_100M_50.txt" }, { 100, "data/data_100M_100.txt" } */};
// map<size_t, const char *> LEN_TO_FILENAME = {{20, "data_10M_20.txt"}, {50, "data_10M_50.txt"}, {100, "data_10M_100.txt"} /* { 50, "data/data_100M_50.txt" }, { 100, "data/data_100M_100.txt" } */};

map<size_t, const char *> LEN_TO_FILENAME = {{20, "data_100M_20.txt"}, {50, "data_100M_50.txt"} /* { 50, "data/data_100M_50.txt" }, { 100, "data/data_100M_100.txt" } */};


//merge speed test
// map<size_t, const char *> LEN_TO_FILENAME = {{20, "data_0M_20.txt"} /* { 50, "data/data_100M_50.txt" }, { 100, "data/data_100M_100.txt" } */};



// map<size_t, const char *> LEN_TO_FILENAME = {{50, "data_100M_50.txt"}, {100, "data_100M_100.txt"} /* { 50, "data/data_100M_50.txt" }, { 100, "data/data_100M_100.txt" } */};
// map<size_t, const char *> LEN_TO_FILENAME = {{100, "data_100M_100.txt"} /* { 50, "data/data_100M_50.txt" }, { 100, "data/data_100M_100.txt" } */};

map<size_t, const char *> LEN_TO_FILENAME_MONSTERS = {{20, "data_100M_20_monsters.txt"}, /* { 50, "data/data_100M_50_monsters.txt" }, { 100, "data/data_100M_100_monsters.txt" } */};
// map<size_t, const char *> LEN_TO_FILENAME_MONSTERS = {{50, "data_100M_50_monsters.txt"}, /* { 50, "data/data_100M_50_monsters.txt" }, { 100, "data/data_100M_100_monsters.txt" } */};

// !!! ПОМЕНЯТЬ НАЗВАНИЕ В SAVE_FILE !!!
// const char* filename = "data/data_test_10.txt";
// size_t string_len = 100;

size_t NETWORK_BANDWIDTH_MBps = 20 * 1024;
size_t CORE_BANDWIDTH_MBps = 6 * 1024;

vector<vector<char *>> strings(NUM_NODES);
vector<vector<vector<char *>>> strings_exchanged(NUM_NODES, vector<vector<char *>>(p));
vector<vector<char *>> samples(NUM_NODES, vector<char *>(s));

double network_delay_coeff = 0.1;
double network_delay_mcrs = 1000;
std::random_device rd;  // Seed for the random number engine
std::mt19937 gen(rd()); // Mersenne Twister random number generator
std::uniform_real_distribution<> dis(0, network_delay_mcrs);
std::mutex m_reading;
std::mutex m_local_partition;
std::mutex m_global_partition;
std::mutex m_strings_exchange;
std::condition_variable cv_reading;
std::condition_variable cv_local_partition;
std::condition_variable cv_global_partition;
std::condition_variable cv_strings_exchange;
size_t threads_finished_reading = 0;
size_t threads_finished_local_partition = 0;
size_t threads_finished_strings_exchange = 0;
bool global_partition_finished = false;

map<timestamp, vector<double>> execution_timestamps;
vector<vector<size_t>> bucket_sizes(NUM_NODES);
vector<char *> splitters(s);

unordered_set<string> MONSTER_KEYS;
vector<char *> STRINGS;
vector<char *> MONSTER_STRINGS;
size_t PART_SIZE;
bool MONSTER_FLAG = false;

void init(size_t f_, size_t nodes)
{
    NUM_NODES = nodes;
    f = f_;
    p = f * nodes;
    s = p - 1;

    for (size_t t = timestamp::DISTRIBUTED_SORT; t <= timestamp::NETWORK_DELAY; t++)
    {
        execution_timestamps[static_cast<timestamp>(t)] = vector<double>(nodes);
    }
    strings = vector<vector<char *>>(nodes);
    strings_exchanged = vector<vector<vector<char *>>>(nodes, vector<vector<char *>>(p));
    samples = vector<vector<char *>>(nodes, vector<char *>(s));
    bucket_sizes = vector<vector<size_t>>(nodes);
    splitters = vector<char *>(s);

    threads_finished_reading = 0;
    threads_finished_local_partition = 0;
    threads_finished_strings_exchange = 0;
    global_partition_finished = false;
}

unordered_set<string> find_monster_keys(vector<char *> keys)
{
    size_t edge = keys.size() / 25;
    unordered_set<string> res;
    unordered_map<string, size_t> hist;

    for (const auto &key : keys)
    {
        auto &v = hist[key];
        if (v++ == edge)
            res.insert(key);
    }

    size_t max = 1;
    for (auto &[k, v] : hist)
        if (v > max)
            max = v;
    cout << "max: " << max << endl;
    return res;
}

pair<vector<char *>, vector<char *>> separate_vectors(vector<char *> strings, unordered_set<string> monster_keys)
{
    std::vector<char *> string_not_keys;
    std::vector<char *> string_keys;

    for (char *str : strings)
    {
        if (monster_keys.find(str) != monster_keys.end())
        {
            string_keys.push_back(str);
        }
        else
        {
            string_not_keys.push_back(str);
        }
    }
    return make_pair(string_not_keys, string_keys);
}

void node_process(size_t id, char *start, char *end, size_t string_len)
{

    // size_t dataSizeBytes = strlen(data) * sizeof(char);

    size_t i_start = id * PART_SIZE;
    size_t i_end;
    if (!MONSTER_FLAG)
        i_end = (id == NUM_NODES - 1) ? STRINGS.size() : i_start + PART_SIZE;
    else
        i_end = (id == NUM_NODES - 2) ? STRINGS.size() : i_start + PART_SIZE;

    /* Step 0 (optional). Read Data */
    auto begin_time_reading_data = std::chrono::high_resolution_clock::now();

    read_data(strings[id], start, end);
    // strings[id] = read_data_from_vector(STRINGS, i_start, i_end);
    // if (MONSTER_FLAG && id == NUM_NODES - 1)
    //     strings[id] = MONSTER_STRINGS;
    // else
    //     strings[id] = read_data_from_vector(STRINGS, i_start, i_end);

    add_timestamp_in_ms(execution_timestamps, timestamp::READ_DATA, begin_time_reading_data, id);

    ++threads_finished_reading;
    // notify when all threads finished reading
    if (threads_finished_reading == NUM_NODES)
    {
        cv_reading.notify_all();
    }

    {
        // wait until all threads have finished reading data
        std::unique_lock<std::mutex> lock(m_reading);
        cv_reading.wait(lock, []
                        { return threads_finished_reading == NUM_NODES; });
    }

    /* Sort Start */
    auto begin_time_distributed_sort = std::chrono::high_resolution_clock::now();

    auto delay_time = emulateBandwidth(end - start, NETWORK_BANDWIDTH_MBps);
    add_timestamp_in_ms(execution_timestamps, timestamp::NETWORK_DELAY, delay_time, id);
    add_timestamp_in_ms(execution_timestamps, timestamp::DISTRIBUTED_SORT, delay_time, id);

    /* Step 1. MSD Radix Sort */
    cout << "Step 1...\n"
         << endl;
    auto begin_time_msd_sort = std::chrono::high_resolution_clock::now();

    // TODO: add bucket sort
    tlx::sort_strings(strings[id]);
    // std::sort(strings[id].begin(), strings[id].end(), [](const char *a, const char *b)
    //           { return strcmp(a, b) < 0; });
    add_timestamp_in_ms(execution_timestamps, timestamp::MSD_SORT, begin_time_msd_sort, id);

    /* Step 2. Partition Data */
    cout << "Step 2...\n"
         << endl;
    auto begin_time_local_partition = std::chrono::high_resolution_clock::now();
    partition_cb(strings[id], s, samples[id], string_len);

    add_timestamp_in_ms(execution_timestamps, timestamp::LOCAL_PARTITIONING, begin_time_local_partition, id);

    // notify the leader about finishing local partitioning by all threads

    ++threads_finished_local_partition;
    if (threads_finished_local_partition == NUM_NODES)
    {
        cv_local_partition.notify_one();
    }

    /* Step 3. Global Partitioning */
    cout << "Step 3...\n"
         << endl;
    // only the leader is executing this step
    if (id == 0)
    {
        // wait until all threads have finished local partitioning step
        {
            std::unique_lock<std::mutex> lock(m_local_partition);
            cv_local_partition.wait(lock, []
                                    { return threads_finished_local_partition == NUM_NODES; });
        }

        auto begin_time_global_partitioning = std::chrono::high_resolution_clock::now();
        vector<char *> sorted_samples;
        for (auto &sample : samples)
        {
            for (size_t i = 0; i < sample.size(); i++)
            {
                char *str = sample[i];
                if (strlen(str) != 0)
                {
                    sorted_samples.push_back(str);
                }
            }
        }

        tlx::sort_strings(sorted_samples);
        // partition_cb(strings[id], s, samples[id], string_len);
        partition_cb(sorted_samples, s, splitters, string_len);

        // notify all threads to continue execution
        global_partition_finished = true;
        cv_global_partition.notify_all();
        add_timestamp_in_ms(execution_timestamps, timestamp::GLOBAL_PARTITIONING, begin_time_global_partitioning, id);
    }

    // all threads other than leader are executing the step
    if (id != 0)
    {
        // wait for the leader to finish *global* partitioning
        auto begin_time_waiting_global_partition = std::chrono::high_resolution_clock::now();
        std::unique_lock<std::mutex> lock(m_global_partition);
        while (!global_partition_finished)
        {
            cv_global_partition.wait(lock);
        }
        lock.unlock();
        add_timestamp_in_ms(execution_timestamps, timestamp::GLOBAL_PARTITIONING, begin_time_waiting_global_partition, id);
    }

    cout << "Step 4...\n"
         << endl;
    /* Step 4. Buckets Determination */
    vector<size_t> partition_idx;
    auto begin_time_buckets_determination_timestamps = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < p - 1; ++i)
    {
        std::pair<size_t, bool> res = binary_search(strings[id], splitters[i]);
        if (res.second)
        {
            partition_idx.emplace_back(res.first);
        }
        else
        {
            partition_idx.emplace_back(res.first - 1);
        }
    }

    partition_idx.emplace_back(strings[id].size() - 1);
    add_timestamp_in_ms(execution_timestamps, timestamp::BUCKETS_DETERMINATION, begin_time_buckets_determination_timestamps, id);

    /* Step 5. Strings Exchange */
    cout << "Step 5...\n"
         << endl;
    auto begin_time_strings_exchange = std::chrono::high_resolution_clock::now();
    size_t prev_idx = -1;

    for (size_t i = 0; i < partition_idx.size(); i++)
    {
        size_t idx = partition_idx[i];

        vector<char *>::const_iterator first = strings[id].begin() + prev_idx + 1;
        vector<char *>::const_iterator last = strings[id].begin() + idx + 1;

        // emulate network delay depending on the size of the strings (uncomment if needed)
        // double sleep_mcrs = network_delay_coeff * (last - first);
        // sleep_mcrs += dis(gen);
        // usleep(sleep_mcrs);

        size_t node_id = i / f;
        size_t bucket_id = (f * id) + (i % f);
        strings_exchanged[node_id][bucket_id] = vector<char *>(first, last);
        prev_idx = idx;

        auto delay_time = (GROUPS[id] == GROUPS[node_id]) ? emulateBandwidth((last - first) * string_len, NETWORK_BANDWIDTH_MBps) : emulateBandwidth((last - first) * string_len, CORE_BANDWIDTH_MBps);
        if (id != node_id)
            add_timestamp_in_ms(execution_timestamps, timestamp::NETWORK_DELAY, delay_time, node_id);
        add_timestamp_in_ms(execution_timestamps, timestamp::DISTRIBUTED_SORT, delay_time, id);
        if (id != node_id)
            add_timestamp_in_ms(execution_timestamps, timestamp::STRINGS_EXCHANGE_COUNT, last - first, node_id);
    }
    add_timestamp_in_ms(execution_timestamps, timestamp::STRINGS_EXCHANGE, begin_time_strings_exchange, id);

    // notify 'Strings Exchange' step finished
    ++threads_finished_strings_exchange;
    if (threads_finished_strings_exchange == NUM_NODES)
    {
        cv_strings_exchange.notify_all();
    }

    // wait until all threads have finished 'Strings Exchange' step
    auto begin_time_strings_exchange_waiting = std::chrono::high_resolution_clock::now();
    {
        std::unique_lock<std::mutex> lock(m_strings_exchange);
        while (threads_finished_strings_exchange != NUM_NODES)
        {
            cv_strings_exchange.wait(lock);
        }
    }

    // save sizes of all buckets
    for (int i = 0; i < strings_exchanged[id].size(); i++)
    {
        bucket_sizes[id].push_back(strings_exchanged[id][i].size());
    }

    add_timestamp_in_ms(execution_timestamps, timestamp::STRINGS_EXCHANGE_WAITING, begin_time_strings_exchange_waiting, id);

    /* Step 6. Local Merge */
    cout << "Step 6...\n"
         << endl;
    auto begin_time_local_merge = std::chrono::high_resolution_clock::now();
    cout << "Step 6.1...\n";

    size_t total_size = 0;
    for (const auto &vec : strings_exchanged[id])
    {
        total_size += vec.size();
    }
    cout << "Step 6.2...\n";

    vector<char *> sorted_strings(total_size);
    std::vector<std::pair<std::vector<char *>::iterator, std::vector<char *>::iterator>> sequences;
    for (auto &vec : strings_exchanged[id])
    {
        sequences.emplace_back(vec.begin(), vec.end());
    }
    cout << "Step 6.3...\n";

    auto comp = [&string_len](const char *a, const char *b)
    {
        return std::memcmp(a, b, string_len) < 0;
    };

    // tlx::parallel_multiway_merge(sequences.begin(), sequences.end(), sorted_strings.begin(), total_size, comp);
    // tlx::multiway_merge(sequences.begin(), sequences.end(), sorted_strings.begin(), total_size, comp);

    // mergeSequences(sequences, sorted_strings);

    SequentialMerge(sequences, sorted_strings);

    // for (const auto &vec : sequences)
    // {
    //     vector<char *> res;
    //     auto comp = [string_len](const char *a, const char *b)
    //     {
    //         return std::memcmp(a, b, string_len) < 0;
    //     };
    //     if (sorted_strings.empty())
    //     {
    //         sorted_strings = std::vector(vec.first, vec.second);
    //         continue;
    //     }
    //     std::merge(sorted_strings.begin(), sorted_strings.end(), vec.first, vec.second, res.begin());
    //     std::swap(res, sorted_strings);
    // }

    cout << "Step 6.4...\n";
    add_timestamp_in_ms(execution_timestamps, timestamp::LOCAL_MERGE, begin_time_local_merge, id);

    cout << "Step 6.5...\n";
    // distributed sort finished
    add_timestamp_in_ms(execution_timestamps, timestamp::DISTRIBUTED_SORT, begin_time_distributed_sort, id);

    cout << "Step 6.6...\n";

    cout << "Finished...\n"
         << endl;
    for (auto s : strings[id])
    {
        delete[] s;
    }

    // output sorted local strings to files
    //    auto begin_time_write_data = std::chrono::high_resolution_clock::now();
    //    std::ofstream out("out" + std::to_string(id) + ".txt");
    //    for (auto& str : sorted_strings) {
    //        out << str << '\n';
    //    }
    //    out.close();
    //    add_timestamp_in_ms(execution_timestamps, timestamp::WRITE_DATA, begin_time_write_data, id);
}

int main()
{
    // fill_data();
    // fill_data_with_key_monsters();

    for (auto nodes : NODES)
    {
        // cout << "node " << nodes << " from " << NODES.size() << '\n';
        // run for various types of data
        // for (auto len_filename : LEN_TO_FILENAME_MONSTERS)
        for (auto len_filename : LEN_TO_FILENAME)

        {
            size_t string_len = len_filename.first;
            const char *filename = len_filename.second;

            // run for different number of buckets
            for (size_t f_ : factors)
            {
                // cout << "factor " << f_ << " from " << factors.size() << '\n';

                init(f_, nodes);
                // map file to memory
                vector<thread> nodes;
                cout << "filename " << filename << "\n";
                struct stat sb;
                size_t fd = open(filename, O_RDONLY);
                if (fstat(fd, &sb) == -1)
                {
                    perror("Error getting file size");
                    close(fd);
                    return 1;
                }

                char *mapped = static_cast<char *>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
                if (mapped == MAP_FAILED)
                {
                    perror("Error mapping file");
                    close(fd);
                    return 1;
                }

                /* Single Thread Test */
                cout << "Single Thread Started...\n"
                     << endl;
                vector<char *> single_thread_strings;
                char *start = mapped;
                char *end = mapped + sb.st_size;
                read_data(single_thread_strings, start, end);

                /* Finding monster keys values*/
                // for (char* str : single_thread_strings) {
                // STRINGS.push_back(strdup(str)); // Allocate and copy
                // }
                // MONSTER_KEYS = find_monster_keys(STRINGS);

                // if (MONSTER_KEYS.size() > 0)
                // {
                //     MONSTER_FLAG = true;
                //     cout << "found such monster keys: \n";
                //     for (const auto k : MONSTER_KEYS)
                //     {
                //         cout << k << " ";
                //     }
                //     cout << '\n';
                //     auto [normal, is_monster] = separate_vectors(STRINGS, MONSTER_KEYS);
                //     cout << "is_key: " << normal.size() << ": \n";
                //     cout << "is_monster: " << is_monster.size() << ": \n";
                //     STRINGS.swap(normal);
                //     MONSTER_STRINGS.swap(is_monster);
                // }

                std::chrono::high_resolution_clock::time_point begin_time = std::chrono::high_resolution_clock::now();
                tlx::sort_strings(single_thread_strings);
                auto end_time = std::chrono::high_resolution_clock::now();
                std::chrono::duration<double, std::milli> duration = end_time - begin_time;
                for (auto s : single_thread_strings)
                {
                    delete[] s;
                }
                double time_ms = duration.count();
                double bytes_in_mb = 1'048'576;
                double speed = filesize(filename) / bytes_in_mb / (time_ms / 1000);

                /* Multiple Threads Test */
                size_t part_size = sb.st_size / NUM_NODES;
                if (!MONSTER_FLAG)
                    PART_SIZE = STRINGS.size() / NUM_NODES;
                else
                    PART_SIZE = STRINGS.size() / (NUM_NODES - 1);

                // run threads
                cout << "Distributed Sorting Started...\n"
                     << endl;
                // if (!MONSTER_FLAG)
                // {
                for (size_t i = 0; i < NUM_NODES; i++)
                {
                    char *part_start = mapped + i * part_size;
                    char *part_end = (i == NUM_NODES - 1) ? mapped + sb.st_size : part_start + part_size;
                    nodes.emplace_back(node_process, i, part_start, part_end, string_len);
                }
                // }
                // else
                // {
                //     for (size_t i = 0; i < NUM_NODES - 1; i++)
                //     {
                //         char *part_start = mapped + i * part_size;
                //         char *part_end = (i == NUM_NODES - 2) ? mapped + sb.st_size : part_start + part_size;
                //         nodes.emplace_back(node_process, i, part_start, part_end, string_len);
                //     }
                //     nodes.emplace_back(monster_node_process, NUM_NODES - 1, mapped, mapped + sb.st_size, string_len);
                // }

                cout << "before join \n";
                for (auto &n : nodes)
                {
                    n.join();
                }
                cout << "after join \n";

                munmap(mapped, sb.st_size);
                close(fd);

                output_statistics(filename, execution_timestamps, bucket_sizes, p, NUM_NODES, string_len, time_ms,
                                  speed);
            }
        }
    }
    return 0;
}
