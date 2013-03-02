#include <string>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <sys/timeb.h>

#include "leveldb/db.h"

inline long now() {
    struct timeb t;
    ftime(&t);
    return t.time * 1000 + t.millitm;
}

int attributeLength = 10;

int main(int argc, char **argv) {
    // Read arguments.
    if (argc < 3) {
        printf("USAGE :%s <csv file> <db name>\n", argv[0]);
        exit(1);
    }
    char * csv_file = argv[1],
         * db_name  = argv[2];
    
    // Set up leveldb database
    leveldb::DB *db;
    leveldb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = true;
    options.block_size = 65536;

    leveldb::Status status = leveldb::DB::Open(options, db_name, &db);
    if (!status.ok()) {
        printf("Error opening database with name '%s'.\n", db_name);
        exit(1);
    }

    // Open CSV
    std::ifstream csv(csv_file);
    if (!csv) {
        printf("Error opening CSV file.\n");
        exit(1);
    }

    leveldb::WriteOptions writeOptions;

    long start_time = now();
    int recordCount = 0;
    std::string buffer(1000, (char)0);
    while (csv) {
        // Read the next line, or leave if there are no more.
        if (!std::getline(csv, buffer))
            break;

        std::istringstream stream(buffer);

        char key[attributeLength];
        assert(stream.getline(key, attributeLength, ','));

        char value[attributeLength];
        assert(stream.getline(value, attributeLength, ','));

        if (!db->Put(writeOptions, key, value).ok()) {
            printf("WARNING: Error writing record: %s\n", buffer.c_str());
        }
        recordCount++;

        // Clear buffer and record for use in next iteration.
        buffer.assign(1000, (char)0);
    } 

    long end_time = now();

    printf("NUMBER OF RECORDS: %d\n", recordCount);
    printf("TIME: %ld milliseconds\n", end_time - start_time);

    csv.close();
    delete db;
    return 0;
}
