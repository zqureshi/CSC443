#include <string>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <sys/timeb.h>

#include "serializer.h"
#include "leveldb/db.h"

inline long now() {
    struct timeb t;
    ftime(&t);
    return t.time * 1000 + t.millitm;
}

Schema schema(100, 10);
int recordSize = fixed_len_sizeof(NULL, schema);

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
    options.block_size = 65536;
    options.create_if_missing = true;
    options.error_if_exists = true;

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
    std::string buffer(recordSize, (char)0);
    Record record(schema);
    while (csv) {

        // Read the next line, or leave if there are no more.
        if (!std::getline(csv, buffer))
            break;

        std::istringstream stream(buffer);
        int position = 0;

        char key[schema.attrLen];
        memset(key, 0, schema.attrLen);
        while (stream && position < schema.numAttrs) {
            // Read the next attribute
            char attribute[schema.attrLen + 1];
            assert(stream.getline(attribute, schema.attrLen + 1, ','));

            // A1 is the key.
            if (position == 0)
                memcpy(key, attribute, schema.attrLen);

            strcpy((char*)record.at(position++), attribute);
        }

        // Add record to database.
        leveldb::Slice keySlice(key);
        leveldb::Slice value(record.at(0), recordSize);

        if (!db->Put(writeOptions, keySlice, value).ok()) {
            printf("WARNING: Error writing record: %s\n", buffer.c_str());
        }
        recordCount++;

        // Clear buffer and record for use in next iteration.
        buffer.assign(recordSize, (char)0);
        memset((char*)record.at(0), 0, recordSize);
    } 

    long end_time = now();

    printf("NUMBER OF RECORDS: %d\n", recordCount);
    printf("TIME: %ld milliseconds\n", end_time - start_time);

    csv.close();
    delete db;
    return 0;
}
