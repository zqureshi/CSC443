#include <string>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <map>
#include <sys/timeb.h>

#include "leveldb/db.h"
#include "leveldb/comparator.h"

inline long now() {
    struct timeb t;
    ftime(&t);
    return t.time * 1000 + t.millitm;
}

int attributeLength = 10;

int main(int argc, char **argv) {
    // Read arguments.
    if (argc < 4) {
        printf("USAGE :%s <db name> <start> <end>\n", argv[0]);
        exit(1);
    }
    char * db_name = argv[1],
         *   start = argv[2],
         *     end = argv[3];
    
    // Set up leveldb database
    leveldb::DB *db;
    leveldb::Options options;
    options.block_size = 65536;
    leveldb::Status status = leveldb::DB::Open(options, db_name, &db);
    if (!status.ok()) {
        printf("Error opening database with name '%s'.\n", db_name);
        exit(1);
    }

    long start_time = now();

    leveldb::Iterator *it = db->NewIterator(leveldb::ReadOptions());
    std::map<std::string, int> counts;

    for (it->Seek(start); it->Valid(); it->Next()) {
        leveldb::Slice key = it->key();
        leveldb::Slice value = it->value();

        if (options.comparator->Compare(key, end) > 0)
            break;

        std::string a2(value.data());
        counts[a2.substr(0, 5)]++;
    }

    long end_time = now();

    std::map<std::string, int>::iterator mit;
    for (mit = counts.begin(); mit != counts.end(); ++mit)
        printf("%s %d\n", mit->first.c_str(), mit->second);

    printf("TIME: %ld milliseconds\n", end_time - start_time);

    delete db;
    return 0;
}
