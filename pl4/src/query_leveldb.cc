#include <string>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <sys/timeb.h>

#include "serializer.h"
#include "leveldb/db.h"
#include "leveldb/comparator.h"

inline long now() {
    struct timeb t;
    ftime(&t);
    return t.time * 1000 + t.millitm;
}

Schema schema(100, 10);
int recordSize = fixed_len_sizeof(NULL, schema);

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
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, db_name, &db);
    if (!status.ok()) {
        printf("Error opening database with name '%s'.\n", db_name);
        exit(1);
    }

    long start_time = now();
    leveldb::Iterator *it = db->NewIterator(leveldb::ReadOptions());
    leveldb::Slice startSlice = start;
    Record record(schema);

    for (it->Seek(start); it->Valid(); it->Next()) {
        leveldb::Slice key = it->key();
        leveldb::Slice value = it->value();

        if (options.comparator->Compare(key, end) > 0)
            break;

        printf("%s\n", value.data());


        fixed_len_read((void*)value.data(), recordSize, &record, schema);
        // TODO do query stuff

        // Clear record for next iteration.
        memset((char*)record.at(0), 0, recordSize);
    }

    long end_time = now();
    printf("TIME: %ld milliseconds\n", end_time - start_time);

    delete db;
    return 0;
}
