//
//  main.c
//  dbapi4oc
//
//  Created by Bernardo Breder on 18/06/14.
//  Copyright (c) 2014 Bernardo Breder. All rights reserved.
//

#include <CoreFoundation/CoreFoundation.h>
#import <CommonCrypto/CommonDigest.h>
#import <CommonCrypto/CommonCryptor.h>
#import <CommonCrypto/CommonHMAC.h>
#import "NSData+CommonCrypto.h"
#include "database.h"

int main(int argc, const char * argv[])
{
	for (;;) {
		@autoreleasepool {
			int max = 4 * 1024 * 4;
			{
				NSString *plain = @"test text 123456";
				uint8_t cipher[16];
				cipher[0] = 0x90;
				cipher[1] = 0xF2;
				cipher[2] = 0x84;
				cipher[3] = 0x13;
				cipher[4] = 0x82;
				cipher[5] = 0x71;
				cipher[6] = 0x18;
				cipher[7] = 0x38;
				cipher[8] = 0xEA;
				cipher[9] = 0xC7;
				cipher[10] = 0xFD;
				cipher[11] = 0x9B;
				cipher[12] = 0x0D;
				cipher[13] = 0x96;
				cipher[14] = 0xCC;
				cipher[15] = 0xE0;
				NSData *cipherData = [NSData dataWithBytes:cipher length:16];
				NSData *decryptData = [cipherData decryptedDataUsingAlgorithm:kCCAlgorithmAES128 key:@"0123456789abcdef" initializationVector:@"AAAAAAAAAAAAAAAA" options:0 error:nil];
				NSString *decrypt = [[NSString alloc] initWithData:decryptData encoding:NSUTF8StringEncoding];
				assert([plain isEqualToString:decrypt]);
			}
			{
				CFShow(CFSTR("[dbtable]: Dropping...\n"));
				{
					struct db_table_t* map = db_table_open("person");
					if (map) {
						assert(!db_table_drop(map));
						db_table_free(map);
					}
				}
				CFShow(CFSTR("[dbtable]: Creating...\n"));
				{
					struct db_table_t* map = db_table_create("person", 1, 4*1024);
					if (map) {
						for (int n = 1; n <= max; n++) {
							db_table_add(map, (uint8_t*)&n, 4);
						}
						assert(!db_table_commit(map));
						db_table_free(map);
					}
				}
				CFShow(CFSTR("[dbtable]: Removing...\n"));
				{
					struct db_table_t* map = db_table_open("person");
					assert(map);
					for (int n = 1; n <= max; n++) {
						assert(!db_table_remove(map, n));
					}
					assert(!db_table_rollback(map));
					db_table_free(map);
				}
				CFShow(CFSTR("[dbtable]: Reading...\n"));
				{
					struct db_table_t* map = db_table_open("person");
					assert(map);
					db_table_get(map, max, 0);
					for (int n = 1; n <= max; n++) {
						int* value = (int*)db_table_get(map, n, 0);
						assert(n == *value);
					}
					db_table_free(map);
				}
				CFShow(CFSTR("[dbtable]: Removing...\n"));
				{
					struct db_table_t* map = db_table_open("person");
					assert(map);
					for (int n = 1; n <= max; n++) {
						assert(!db_table_remove(map, n));
					}
					assert(!db_table_commit(map));
					db_table_free(map);
				}
				CFShow(CFSTR("[dbtable]: Dropping...\n"));
				{
					struct db_table_t* map = db_table_open("person");
					assert(map);
					assert(!db_table_drop(map));
					db_table_free(map);
				}
			}
			{
				CFShow(CFSTR("[dbindex]: Dropping...\n"));
				{
					struct db_index_t* map = db_index_open("person_index");
					if (map) {
						assert(!db_index_drop(map));
						db_index_free(map);
					}
				}
				CFShow(CFSTR("[dbindex]: Creating...\n"));
				{
					struct db_index_t* map = db_index_create("person_index", 1, 4*4*1024);
					if (map) {
						for (int n = 1; n <= max; n++) {
							db_index_add(map, n, n);
						}
						assert(!db_index_commit(map));
						db_index_free(map);
					}
				}
				CFShow(CFSTR("[dbindex]: Reading...\n"));
				{
					struct db_index_t* map = db_index_open("person_index");
					assert(map);
					assert(max == db_index_get(map, max));
					for (int n = 1; n <= max; n++) {
						assert(n == db_index_get(map, n));
					}
					db_index_free(map);
				}
				CFShow(CFSTR("[dbindex]: Dropping...\n"));
				{
					struct db_index_t* map = db_index_open("person_index");
					if (map) {
						assert(!db_index_drop(map));
						db_index_free(map);
					}
				}
			}
            {
				CFShow(CFSTR("[dbindex]: Creating...\n"));
				{
					struct db_index_t* map = db_index_create("person_index", 1, 4*4*1024);
                    assert(map);
                    assert(!db_index_commit(map));
                    db_index_free(map);
				}
                CFShow(CFSTR("[dbindex]: Dropping...\n"));
				{
					struct db_index_t* map = db_index_open("person_index");
					assert(map);
                    assert(!db_index_drop(map));
                    db_index_free(map);
				}
			}
			{
				CFShow(CFSTR("[dbbytes]: Read Write 32...\n"));
				{
					struct db_bytes_t* bytes = db_bytes_create(0);
					db_bytes_write_uint32_compressed(bytes, 0);
					db_bytes_write_uint32_compressed(bytes, 1);
					db_bytes_write_uint32_compressed(bytes, 127);
					db_bytes_write_uint32_compressed(bytes, 128);
					db_bytes_write_uint32_compressed(bytes, 255);
					db_bytes_write_uint32_compressed(bytes, 16383);
					db_bytes_write_uint32_compressed(bytes, 16384);
					db_bytes_write_uint32_compressed(bytes, 2097151);
					db_bytes_write_uint32_compressed(bytes, 2097152);
					db_bytes_write_uint32_compressed(bytes, 268435455);
					db_bytes_write_uint32_compressed(bytes, 268435456);
					db_bytes_write_uint32_compressed(bytes, 4294967295);
					uint8_t* aux = db_bytes_result(bytes);
					assert(0 == db_bytes_read_uint32_compressed(&aux));
					assert(1 == db_bytes_read_uint32_compressed(&aux));
					assert(127 == db_bytes_read_uint32_compressed(&aux));
					assert(128 == db_bytes_read_uint32_compressed(&aux));
					assert(255 == db_bytes_read_uint32_compressed(&aux));
					assert(16383 == db_bytes_read_uint32_compressed(&aux));
					assert(16384 == db_bytes_read_uint32_compressed(&aux));
					assert(2097151 == db_bytes_read_uint32_compressed(&aux));
					assert(2097152 == db_bytes_read_uint32_compressed(&aux));
					assert(268435455 == db_bytes_read_uint32_compressed(&aux));
					assert(268435456 == db_bytes_read_uint32_compressed(&aux));
					assert(4294967295 == db_bytes_read_uint32_compressed(&aux));
					db_bytes_free(bytes);
				}
				CFShow(CFSTR("[dbbytes]: Read Write 64...\n"));
				{
					struct db_bytes_t* bytes = db_bytes_create(0);
					db_bytes_write_uint64_compressed(bytes, 0);
					db_bytes_write_uint64_compressed(bytes, 1);
					db_bytes_write_uint64_compressed(bytes, 127);
					db_bytes_write_uint64_compressed(bytes, 128);
					db_bytes_write_uint64_compressed(bytes, 255);
					db_bytes_write_uint64_compressed(bytes, 16383);
					db_bytes_write_uint64_compressed(bytes, 16384);
					db_bytes_write_uint64_compressed(bytes, 2097151);
					db_bytes_write_uint64_compressed(bytes, 2097152);
					db_bytes_write_uint64_compressed(bytes, 268435455);
					db_bytes_write_uint64_compressed(bytes, 268435456);
					db_bytes_write_uint64_compressed(bytes, 4294967295);
					db_bytes_write_uint64_compressed(bytes, 4294967296);
					db_bytes_write_uint64_compressed(bytes, 0x0);
					db_bytes_write_uint64_compressed(bytes, 0xF);
					db_bytes_write_uint64_compressed(bytes, 0xFF);
					db_bytes_write_uint64_compressed(bytes, 0xFFF);
					db_bytes_write_uint64_compressed(bytes, 0xFFFF);
					db_bytes_write_uint64_compressed(bytes, 0xFFFFF);
					db_bytes_write_uint64_compressed(bytes, 0xFFFFFF);
					db_bytes_write_uint64_compressed(bytes, 0xFFFFFFF);
					db_bytes_write_uint64_compressed(bytes, 0xFFFFFFFF);
					db_bytes_write_uint64_compressed(bytes, 0xFFFFFFFFF);
					db_bytes_write_uint64_compressed(bytes, 0xFFFFFFFFFF);
					db_bytes_write_uint64_compressed(bytes, 0xFFFFFFFFFFF);
					db_bytes_write_uint64_compressed(bytes, 0xFFFFFFFFFFFF);
					db_bytes_write_uint64_compressed(bytes, 0xFFFFFFFFFFFFF);
					db_bytes_write_uint64_compressed(bytes, 0xFFFFFFFFFFFFFF);
					db_bytes_write_uint64_compressed(bytes, 0xFFFFFFFFFFFFFFF);
					db_bytes_write_uint64_compressed(bytes, 0xFFFFFFFFFFFFFFFF);
					uint8_t* aux = db_bytes_result(bytes);
					assert(0 == db_bytes_read_uint64_compressed(&aux));
					assert(1 == db_bytes_read_uint64_compressed(&aux));
					assert(127 == db_bytes_read_uint64_compressed(&aux));
					assert(128 == db_bytes_read_uint64_compressed(&aux));
					assert(255 == db_bytes_read_uint64_compressed(&aux));
					assert(16383 == db_bytes_read_uint64_compressed(&aux));
					assert(16384 == db_bytes_read_uint64_compressed(&aux));
					assert(2097151 == db_bytes_read_uint64_compressed(&aux));
					assert(2097152 == db_bytes_read_uint64_compressed(&aux));
					assert(268435455 == db_bytes_read_uint64_compressed(&aux));
					assert(268435456 == db_bytes_read_uint64_compressed(&aux));
					assert(4294967295 == db_bytes_read_uint64_compressed(&aux));
					assert(4294967296 == db_bytes_read_uint64_compressed(&aux));
					assert(0x0 == db_bytes_read_uint64_compressed(&aux));
					assert(0xF == db_bytes_read_uint64_compressed(&aux));
					assert(0xFF == db_bytes_read_uint64_compressed(&aux));
					assert(0xFFF == db_bytes_read_uint64_compressed(&aux));
					assert(0xFFFF == db_bytes_read_uint64_compressed(&aux));
					assert(0xFFFFF == db_bytes_read_uint64_compressed(&aux));
					assert(0xFFFFFF == db_bytes_read_uint64_compressed(&aux));
					assert(0xFFFFFFF == db_bytes_read_uint64_compressed(&aux));
					assert(0xFFFFFFFF == db_bytes_read_uint64_compressed(&aux));
					assert(0xFFFFFFFFF == db_bytes_read_uint64_compressed(&aux));
					assert(0xFFFFFFFFFF == db_bytes_read_uint64_compressed(&aux));
					assert(0xFFFFFFFFFFF == db_bytes_read_uint64_compressed(&aux));
					assert(0xFFFFFFFFFFFF == db_bytes_read_uint64_compressed(&aux));
					assert(0xFFFFFFFFFFFFF == db_bytes_read_uint64_compressed(&aux));
					assert(0xFFFFFFFFFFFFFF == db_bytes_read_uint64_compressed(&aux));
					assert(0xFFFFFFFFFFFFFFF == db_bytes_read_uint64_compressed(&aux));
					assert(0xFFFFFFFFFFFFFFFF == db_bytes_read_uint64_compressed(&aux));
					db_bytes_free(bytes);
				}
			}
			{
				uint8_t buffer[1024];
				{
					remove("tmp.txt");
					FILE* file = fopen("tmp.txt", "wb");
					fclose(file);
					remove("tmp.txt");
				}
				{
					memset(buffer, 0, 1024);
					remove("tmp.txt");
					FILE* file = fopen("tmp.txt", "wb");
					assert(3 == fwrite("abc", 1, 3, file));
					assert(3 == fwrite("cba", 1, 3, file));
					fclose(file);
					file = fopen("tmp.txt", "rb");
					assert(3 == fread(buffer, 1, 3, file));
					assert(3 == fread(buffer + 3, 1, 3, file));
					fclose(file);
					remove("tmp.txt");
					assert(!memcmp(buffer, "abccba", 6));
				}
				{
					memset(buffer, 0, 1024);
					remove("tmp.txt");
					FILE* file = fopen("tmp.txt", "wb");
					assert(3 == fwrite("abc", 1, 3, file));
					fclose(file);
					file = fopen("tmp.txt", "rwb");
					assert(3 == fread(buffer, 1, 3, file));
					fclose(file);
					remove("tmp.txt");
					assert(!strcmp((char*)buffer, "abc"));
				}
				{
					memset(buffer, 0, 1024);
					remove("tmp.txt");
					FILE* wfile = fopen("tmp.txt", "wb");
					FILE* rfile = fopen("tmp.txt", "rb");
					assert(3 == fwrite("abc", 1, 3, wfile));
					assert(3 == fwrite("cba", 1, 3, wfile));
					fflush(wfile);
					assert(3 == fread(buffer, 1, 3, rfile));
					assert(3 == fread(buffer + 3, 1, 3, rfile));
					fclose(rfile);
					fclose(wfile);
					remove("tmp.txt");
					assert(!strcmp((char*)buffer, "abccba"));
				}
			}
			{
				CFShow(CFSTR("[dbfileset]: ...\n"));
				{
					struct db_fileset_t* fileset = db_fileset_create("test.db");
					assert(fileset);
					db_fileset_drop(fileset);
					db_fileset_free(fileset);
				}
				{
					struct db_fileset_t* fileset = db_fileset_create("test.db");
					assert(fileset);
					db_fileset_free(fileset);
					fileset = db_fileset_open("test.db");
					assert(fileset);
					db_fileset_drop(fileset);
					db_fileset_free(fileset);
				}
				{
					int value = 10;
					struct db_fileset_t* fileset = db_fileset_create("test.db");
					ssize_t id = db_fileset_add(fileset, (uint8_t*)&value, sizeof(int));
					db_fileset_free(fileset);
					fileset = db_fileset_open("test.db");
					uint8_t* bytes = db_fileset_get(fileset, id);
					assert(((int*)bytes)[0] == value);
					free(bytes);
					db_fileset_drop(fileset);
					db_fileset_free(fileset);
				}
				{
					uint8_t buffer[0xFFF7];
					memset(buffer, 1, 0xFFF7);
					struct db_fileset_t* fileset = db_fileset_create("test.db");
					ssize_t id = db_fileset_add(fileset, buffer, sizeof(buffer));
					db_fileset_free(fileset);
					fileset = db_fileset_open("test.db");
					uint8_t* bytes = db_fileset_get(fileset, id);
					assert(0 == memcmp(buffer, bytes, sizeof(buffer)));
					free(bytes);
					db_fileset_drop(fileset);
					db_fileset_free(fileset);
				}
				{
					uint8_t buffer[0xFFFF];
					memset(buffer, 1, 0xFFFF);
					buffer[0xFFFF-1] = 2;
					struct db_fileset_t* fileset = db_fileset_create("test.db");
					ssize_t id = db_fileset_add(fileset, buffer, sizeof(buffer));
					db_fileset_free(fileset);
					fileset = db_fileset_open("test.db");
					uint8_t* bytes = db_fileset_get(fileset, id);
					assert(0 == memcmp(buffer, bytes, sizeof(buffer)));
					free(bytes);
					db_fileset_drop(fileset);
					db_fileset_free(fileset);
				}
				{
					uint8_t buffer[0xFFFFF];
					memset(buffer, 1, 0xFFFFF);
					buffer[0xFFFF-1] = 2;
					buffer[0xFFFFF-1] = 2;
					struct db_fileset_t* fileset = db_fileset_create("test.db");
					ssize_t id = db_fileset_add(fileset, buffer, sizeof(buffer));
					db_fileset_free(fileset);
					fileset = db_fileset_open("test.db");
					uint8_t* bytes = db_fileset_get(fileset, id);
					assert(0 == memcmp(buffer, bytes, sizeof(buffer)));
					free(bytes);
					db_fileset_drop(fileset);
					db_fileset_free(fileset);
				}
				{
					uint8_t buffer[0x1], *bytes;
					{
						struct db_fileset_t* fileset = db_fileset_create("test.db");
						memset(buffer, 1, sizeof(buffer));
						assert(0 == db_fileset_add(fileset, buffer, sizeof(buffer)));
						memset(buffer, 2, sizeof(buffer));
						assert(1 == db_fileset_add(fileset, buffer, sizeof(buffer)));
						memset(buffer, 3, sizeof(buffer));
						assert(2 == db_fileset_add(fileset, buffer, sizeof(buffer)));
						db_fileset_free(fileset);
					}
					{
						struct db_fileset_t* fileset = db_fileset_open("test.db");
						bytes = db_fileset_get(fileset, 0);
						memset(buffer, 1, sizeof(buffer));
						assert(0 == memcmp(buffer, bytes, sizeof(buffer)));
						free(bytes);
						bytes = db_fileset_get(fileset, 1);
						memset(buffer, 2, sizeof(buffer));
						assert(0 == memcmp(buffer, bytes, sizeof(buffer)));
						free(bytes);
						bytes = db_fileset_get(fileset, 2);
						memset(buffer, 3, sizeof(buffer));
						assert(0 == memcmp(buffer, bytes, sizeof(buffer)));
						free(bytes);
						db_fileset_drop(fileset);
						db_fileset_free(fileset);
					}
				}
				{
					uint8_t buffer[0xF], *bytes;
					{
						struct db_fileset_t* fileset = db_fileset_create("test.db");
						memset(buffer, 1, sizeof(buffer));
						assert(0 == db_fileset_add(fileset, buffer, sizeof(buffer)));
						memset(buffer, 2, sizeof(buffer));
						assert(1 == db_fileset_add(fileset, buffer, sizeof(buffer)));
						memset(buffer, 3, sizeof(buffer));
						assert(2 == db_fileset_add(fileset, buffer, sizeof(buffer)));
						db_fileset_free(fileset);
					}
					{
						struct db_fileset_t* fileset = db_fileset_open("test.db");
						bytes = db_fileset_get(fileset, 0);
						memset(buffer, 1, sizeof(buffer));
						assert(0 == memcmp(buffer, bytes, sizeof(buffer)));
						free(bytes);
						bytes = db_fileset_get(fileset, 1);
						memset(buffer, 2, sizeof(buffer));
						assert(0 == memcmp(buffer, bytes, sizeof(buffer)));
						free(bytes);
						bytes = db_fileset_get(fileset, 2);
						memset(buffer, 3, sizeof(buffer));
						assert(0 == memcmp(buffer, bytes, sizeof(buffer)));
						free(bytes);
						db_fileset_drop(fileset);
						db_fileset_free(fileset);
					}
				}
				{
					uint8_t buffer[0xFFF7], *bytes;
					{
						struct db_fileset_t* fileset = db_fileset_create("test.db");
						memset(buffer, 1, sizeof(buffer));
						assert(0 == db_fileset_add(fileset, buffer, sizeof(buffer)));
						memset(buffer, 2, sizeof(buffer));
						assert(1 == db_fileset_add(fileset, buffer, sizeof(buffer)));
						memset(buffer, 3, sizeof(buffer));
						assert(2 == db_fileset_add(fileset, buffer, sizeof(buffer)));
						db_fileset_free(fileset);
					}
					{
						struct db_fileset_t* fileset = db_fileset_open("test.db");
						bytes = db_fileset_get(fileset, 0);
						memset(buffer, 1, sizeof(buffer));
						assert(0 == memcmp(buffer, bytes, sizeof(buffer)));
						free(bytes);
						bytes = db_fileset_get(fileset, 1);
						memset(buffer, 2, sizeof(buffer));
						assert(0 == memcmp(buffer, bytes, sizeof(buffer)));
						free(bytes);
						bytes = db_fileset_get(fileset, 2);
						memset(buffer, 3, sizeof(buffer));
						assert(0 == memcmp(buffer, bytes, sizeof(buffer)));
						free(bytes);
						db_fileset_drop(fileset);
						db_fileset_free(fileset);
					}
				}
				{
					uint8_t buffer[0xFFFF], *bytes;
					{
						struct db_fileset_t* fileset = db_fileset_create("test.db");
						memset(buffer, 1, sizeof(buffer));
						assert(0 == db_fileset_add(fileset, buffer, sizeof(buffer)));
						memset(buffer, 2, sizeof(buffer));
						assert(2 == db_fileset_add(fileset, buffer, sizeof(buffer)));
						memset(buffer, 3, sizeof(buffer));
						assert(4 == db_fileset_add(fileset, buffer, sizeof(buffer)));
						db_fileset_free(fileset);
					}
					{
						struct db_fileset_t* fileset = db_fileset_open("test.db");
						bytes = db_fileset_get(fileset, 0);
						memset(buffer, 1, sizeof(buffer));
						assert(0 == memcmp(buffer, bytes, sizeof(buffer)));
						free(bytes);
						bytes = db_fileset_get(fileset, 2);
						memset(buffer, 2, sizeof(buffer));
						assert(0 == memcmp(buffer, bytes, sizeof(buffer)));
						free(bytes);
						bytes = db_fileset_get(fileset, 4);
						memset(buffer, 3, sizeof(buffer));
						assert(0 == memcmp(buffer, bytes, sizeof(buffer)));
						free(bytes);
						db_fileset_drop(fileset);
						db_fileset_free(fileset);
					}
				}
			}
			{
				CFShow(CFSTR("[db]: Drop...\n"));
				{
					DbDatabase *db = [[DbDatabase alloc] initOpen];
					if (db) [db drop];
				}
				CFShow(CFSTR("[db]: Create and Open...\n"));
				{
					uint32_t id;
					[[[DbDatabase alloc] initCreate] commit];
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db createTable:@"person" page:1 slot:4];
						id = [personTable insert:[[[DbOutData alloc] init] writeUInt:5]];
						[db commit];
					}
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db openTable:@"person"];
						DbInData* data = [personTable search:id];
						assert(5 == [data readUInt]);
					}
					[[[DbDatabase alloc] initOpen] drop];
				}
				CFShow(CFSTR("[db]: Add and Remove...\n"));
				{
					uint32_t id;
					[[[DbDatabase alloc] initCreate] commit];
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db createTable:@"person" page:1 slot:4];
						id = [personTable insert:[[[DbOutData alloc] init] writeUInt:5]];
						[db commit];
					}
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db openTable:@"person"];
						assert(nil != [personTable search:id]);
						assert(true == [personTable remove:id]);
						[db commit];
					}
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db openTable:@"person"];
						assert(nil == [personTable search:id]);
					}
					[[[DbDatabase alloc] initOpen] drop];
				}
				CFShow(CFSTR("[db]: Add and Update...\n"));
				{
					uint32_t id;
					[[[DbDatabase alloc] initCreate] commit];
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db createTable:@"person" page:1 slot:4];
						id = [personTable insert:[[[DbOutData alloc] init] writeUInt:5]];
						[db commit];
					}
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db openTable:@"person"];
						assert(5 == [[personTable search:id] readUInt]);
						assert(true == [personTable update:id data:[[[DbOutData alloc] init] writeUInt:2]]);
						[db commit];
					}
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db openTable:@"person"];
						DbInData* data = [personTable search:id];
						assert(2 == [data readUInt]);
					}
					[[[DbDatabase alloc] initOpen] drop];
				}
				CFShow(CFSTR("[db]: Adds, Removes and Updates...\n"));
				{
					uint32_t id1, id2, id3, id4;
					[[[DbDatabase alloc] initCreate] commit];
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db createTable:@"person" page:1 slot:2];
						id1 = [personTable insert:[[[DbOutData alloc] init] writeUInt:5]];
						id2 = [personTable insert:[[[DbOutData alloc] init] writeUInt:10]];
						id3 = [personTable insert:[[[DbOutData alloc] init] writeUInt:20]];
						id4 = [personTable insert:[[[DbOutData alloc] init] writeUInt:50]];
						[db commit];
					}
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db openTable:@"person"];
						assert(5 == [[personTable search:id1] readUInt]);
						assert(10 == [[personTable search:id2] readUInt]);
						assert(20 == [[personTable search:id3] readUInt]);
						assert(50 == [[personTable search:id4] readUInt]);
						assert(true == [personTable update:id1 data:[[[DbOutData alloc] init] writeUInt:6]]);
						assert(true == [personTable update:id2 data:[[[DbOutData alloc] init] writeUInt:11]]);
						assert(true == [personTable update:id3 data:[[[DbOutData alloc] init] writeUInt:21]]);
						assert(true == [personTable remove:id4]);
						[db commit];
					}
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db openTable:@"person"];
						assert(6 == [[personTable search:id1] readUInt]);
						assert(11 == [[personTable search:id2] readUInt]);
						assert(21 == [[personTable search:id3] readUInt]);
						assert(nil == [personTable search:id4]);
					}
					[[[DbDatabase alloc] initOpen] drop];
				}
				CFShow(CFSTR("[db]: Add Many...\n"));
				{
					[[[DbDatabase alloc] initCreate] commit];
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db createTable:@"person" page:1 slot:4*1024];
						for (uint32_t n = 0; n < max ; n++) {
							[personTable insert:[[[DbOutData alloc] init] writeUInt:(n+1)]];
						}
						[db commit];
					}
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db openTable:@"person"];
						for (uint32_t n = 0; n < max ; n++) {
							DbInData* data = [personTable search:(n+1)];
							assert((n+1) == [data readUInt]);
						}
					}
					[[[DbDatabase alloc] initOpen] drop];
				}
				CFShow(CFSTR("[db]: Drop...\n"));
				{
					[[[DbDatabase alloc] initOpen] drop];
				}
			}
			{
				CFShow(CFSTR("[db]: Drop...\n"));
				{
					DbDatabase *db = [[DbDatabase alloc] initOpen];
					if (db) [db drop];
				}
				CFShow(CFSTR("[db]: Create and Open...\n"));
				{
					uint32_t id;
					[[[DbDatabase alloc] initCreate] commit];
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db createTable:@"person" page:1 slot:4];
						DbIndex* idIndex = [personTable createIndex:@"person_id"];
						id = [personTable insert:[[[DbOutData alloc] init] writeUInt:5]];
						[idIndex insert:id id:id];
						[db commit];
					}
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db openTable:@"person"];
						DbIndex* idIndex = [personTable openIndex:@"person_id"];
						assert(id == [idIndex search:id]);
					}
					[[[DbDatabase alloc] initOpen] drop];
				}
				CFShow(CFSTR("[db]: Add and Remove...\n"));
				{
					uint32_t id;
					[[[DbDatabase alloc] initCreate] commit];
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db createTable:@"person" page:1 slot:4];
						DbIndex* idIndex = [personTable createIndex:@"person_id"];
						id = [personTable insert:[[[DbOutData alloc] init] writeUInt:5]];
						[idIndex insert:id id:id];
						[db commit];
					}
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db openTable:@"person"];
						DbIndex* idIndex = [personTable openIndex:@"person_id"];
						assert(id == [idIndex search:id]);
						assert(true == [idIndex remove:id]);
						[db commit];
					}
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db openTable:@"person"];
						DbIndex* idIndex = [personTable openIndex:@"person_id"];
						assert(0 == [idIndex search:id]);
					}
					[[[DbDatabase alloc] initOpen] drop];
				}
				CFShow(CFSTR("[db]: Add Many...\n"));
				{
					[[[DbDatabase alloc] initCreate] commit];
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db createTable:@"person" page:1 slot:16*1024];
						for (uint32_t n = 0; n < max ; n++) {
							[personTable insert:[[[DbOutData alloc] init] writeUInt:(n+1)]];
						}
						[db commit];
					}
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db openTable:@"person"];
						for (uint32_t n = 0; n < max ; n++) {
							DbInData* data = [personTable search:(n+1)];
							assert((n+1) == [data readUInt]);
						}
					}
					[[[DbDatabase alloc] initOpen] drop];
				}
				{
					[[[DbDatabase alloc] initCreate] commit];
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db createTable:@"person" page:1 slot:16*1024];
						for (uint32_t n = 0; n < 32 ; n++) {
							[personTable insert:[[[DbOutData alloc] init] writeUInt:(n+1)]];
						}
						[db commit];
						assert(32 == personTable.size);
					}
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db openTable:@"person"];
						assert(32 == personTable.size);
						for (uint32_t n = 0; n < personTable.size ; n++) {
							DbInData* data = [personTable search:(n+1)];
							assert((n+1) == [data readUInt]);
						}
					}
					[[[DbDatabase alloc] initOpen] drop];
				}
				{
					[[[DbDatabase alloc] initCreate] commit];
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db createTable:@"person" page:1 slot:16*1024];
						for (uint32_t n = 0; n < 5 ; n++) {
							[personTable insert:[[[DbOutData alloc] init] writeUInt:(n+1)]];
						}
						[db commit];
						assert(5 == personTable.size);
					}
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db openTable:@"person"];
						assert(YES == [personTable remove:2]);
						assert(YES == [personTable remove:4]);
						[db commit];
						assert(5 == personTable.size);
					}
					{
						DbDatabase *db = [[DbDatabase alloc] initOpen];
						DbTable* personTable = [db openTable:@"person"];
						assert(5 == personTable.size);
						assert(nil == [personTable search:-1]);
						assert(nil == [personTable search:0xFF]);
						assert(nil == [personTable search:-0xFF]);
						assert(nil == [personTable search:0]);
						assert(1 == [[personTable search:1] readUInt]);
						assert(nil == [personTable search:2]);
						assert(3 == [[personTable search:3] readUInt]);
						assert(nil == [personTable search:4]);
						assert(5 == [[personTable search:5] readUInt]);
						assert(nil == [personTable search:6]);
					}
					[[[DbDatabase alloc] initOpen] drop];
				}
				CFShow(CFSTR("[db]: Drop...\n"));
				{
					[[[DbDatabase alloc] initOpen] drop];
				}
			}
			{
                {
                    [[[DbDatabase alloc] initCreate] commit];
                    {
                        DbDatabase *db = [[DbDatabase alloc] initOpen];
                        DbTable* personTable = [db createTable:@"person" page:1 slot:16*1024];
                        [personTable insert:[[[DbOutData alloc] init] writeCoding:@"abc"]];
                        [db commit];
                    }
                    {
                        DbDatabase *db = [[DbDatabase alloc] initOpen];
                        DbTable* personTable = [db openTable:@"person"];
                        DbInData* data = [personTable search:1];
                        NSString *text = (NSString*)[data readCoding];
                        assert([text compare:@"abc"] == NSOrderedSame);
                    }
                    [[[DbDatabase alloc] initOpen] drop];
                }
                {
                    [[[DbDatabase alloc] initCreate] commit];
                    NSDictionary *dic = [[NSMutableDictionary alloc] initWithObjectsAndKeys:@"id", @1, @"firstname", @"Bernardo", @"lastname", @"Breder", nil];
                    {
                        DbDatabase *db = [[DbDatabase alloc] initOpen];
                        DbTable* personTable = [db createTable:@"person" page:1 slot:16*1024];
                        [personTable insert:[[[DbOutData alloc] init] writeCoding:dic]];
                        [db commit];
                    }
                    {
                        DbDatabase *db = [[DbDatabase alloc] initOpen];
                        DbTable* personTable = [db openTable:@"person"];
                        DbInData* data = [personTable search:1];
                        NSDictionary *value = (NSDictionary*)[data readCoding];
                        assert(YES == [value isEqualToDictionary:dic]);
                    }
                    [[[DbDatabase alloc] initOpen] drop];
                }
                {
                    [[[DbDatabase alloc] initCreate] commit];
                    {
                        DbDatabase *db = [[DbDatabase alloc] initOpen];
                        DbIndex* personIndex = [db createIndex:@"person"];
                        [personIndex insert:1 id:1];
                        [personIndex insert:1 id:2];
                        [personIndex insert:1 id:3];
                        [personIndex insert:1 id:4];
                        [personIndex insert:2 id:1];
                        [personIndex insert:2 id:2];
                        [personIndex insert:3 id:1];
                        [db commit];
                    }
                    {
                        DbDatabase *db = [[DbDatabase alloc] initOpen];
                        DbIndex* personIndex = [db openIndex:@"person"];
                        uint32_t values[10];
                        assert(0 == [personIndex search:0 offset:0 limit:10 array:values]);
                        assert(4 == [personIndex search:1 offset:0 limit:10 array:values]);
                        assert(1 == values[0]);
                        assert(2 == values[1]);
                        assert(3 == values[2]);
                        assert(4 == values[3]);
                        assert(2 == [personIndex search:1 offset:2 limit:10 array:values]);
                        assert(3 == values[0]);
                        assert(4 == values[1]);
                        assert(0 == [personIndex search:1 offset:4 limit:10 array:values]);
                        assert(2 == [personIndex search:2 offset:0 limit:10 array:values]);
                        assert(1 == values[0]);
                        assert(2 == values[1]);
                        assert(1 == [personIndex search:3 offset:0 limit:10 array:values]);
                        assert(1 == values[0]);
                        assert(0 == [personIndex search:4 offset:0 limit:10 array:values]);
                    }
                    [[[DbDatabase alloc] initOpen] drop];
                }
                {
                    [[[DbDatabase alloc] initCreate] commit];
                    {
                        DbDatabase *db = [[DbDatabase alloc] initOpen];
                        DbIndex* personIndex = [db createIndex:@"person"];
                        [personIndex insert:3 id:1];
                        [personIndex insert:2 id:2];
                        [personIndex insert:2 id:1];
                        [personIndex insert:1 id:4];
                        [personIndex insert:1 id:3];
                        [personIndex insert:1 id:2];
                        [personIndex insert:1 id:1];
                        [db commit];
                    }
                    {
                        DbDatabase *db = [[DbDatabase alloc] initOpen];
                        DbIndex* personIndex = [db openIndex:@"person"];
                        uint32_t values[10];
                        assert(0 == [personIndex search:0 offset:0 limit:10 array:values]);
                        assert(4 == [personIndex search:1 offset:0 limit:10 array:values]);
                        assert(4 == values[0]);
                        assert(1 == values[1]);
                        assert(2 == values[2]);
                        assert(3 == values[3]);
                        assert(2 == [personIndex search:1 offset:2 limit:10 array:values]);
                        assert(2 == values[0]);
                        assert(3 == values[1]);
                        assert(0 == [personIndex search:1 offset:4 limit:10 array:values]);
                        assert(2 == [personIndex search:2 offset:0 limit:10 array:values]);
                        assert(2 == values[0]);
                        assert(1 == values[1]);
                        assert(1 == [personIndex search:3 offset:0 limit:10 array:values]);
                        assert(1 == values[0]);
                        assert(0 == [personIndex search:4 offset:0 limit:10 array:values]);
                    }
                    [[[DbDatabase alloc] initOpen] drop];
                }
                {
                    [[[DbDatabase alloc] initCreate] commit];
                    {
                        DbDatabase *db = [[DbDatabase alloc] initOpen];
                        DbTable* personTable = [db createTable:@"person" page:1 slot:1];
                        assert(![personTable insert:1 data:[[[DbOutData alloc] init] writeCoding:@"1"]]);
                        assert(![personTable insert:2 data:[[[DbOutData alloc] init] writeCoding:@"2"]]);
                        assert(![personTable insert:3 data:[[[DbOutData alloc] init] writeCoding:@"3"]]);
                        assert(![personTable insert:4 data:[[[DbOutData alloc] init] writeCoding:@"4"]]);
                        assert(![personTable insert:5 data:[[[DbOutData alloc] init] writeCoding:@"5"]]);
                        assert(![personTable insert:6 data:[[[DbOutData alloc] init] writeCoding:@"6"]]);
                        [db commit];
                    }
                    {
                        DbDatabase *db = [[DbDatabase alloc] initOpen];
                        DbTable* personTable = [db openTable:@"person"];
                        assert([(NSString*)[[personTable search:1] readCoding] isEqualToString:@"1"]);
                        assert([(NSString*)[[personTable search:2] readCoding] isEqualToString:@"2"]);
                        assert([(NSString*)[[personTable search:3] readCoding] isEqualToString:@"3"]);
                        assert([(NSString*)[[personTable search:4] readCoding] isEqualToString:@"4"]);
                        assert([(NSString*)[[personTable search:5] readCoding] isEqualToString:@"5"]);
                        assert([(NSString*)[[personTable search:6] readCoding] isEqualToString:@"6"]);
                    }
                    [[[DbDatabase alloc] initOpen] drop];
                }
                {
                    [[[DbDatabase alloc] initCreate] commit];
                    {
                        DbDatabase *db = [[DbDatabase alloc] initOpen];
                        DbTable* personTable = [db createTable:@"person" page:1 slot:32];
                        assert(![personTable insert:1 data:[[[DbOutData alloc] init] writeCoding:@"1"]]);
                        assert(![personTable insert:2 data:[[[DbOutData alloc] init] writeCoding:@"2"]]);
                        assert(![personTable insert:3 data:[[[DbOutData alloc] init] writeCoding:@"3"]]);
                        assert(![personTable insert:4 data:[[[DbOutData alloc] init] writeCoding:@"4"]]);
                        assert(![personTable insert:5 data:[[[DbOutData alloc] init] writeCoding:@"5"]]);
                        assert(![personTable insert:6 data:[[[DbOutData alloc] init] writeCoding:@"6"]]);
                        [db commit];
                    }
                    {
                        DbDatabase *db = [[DbDatabase alloc] initOpen];
                        DbTable* personTable = [db openTable:@"person"];
                        assert([(NSString*)[[personTable search:1] readCoding] isEqualToString:@"1"]);
                        assert([(NSString*)[[personTable search:2] readCoding] isEqualToString:@"2"]);
                        assert([(NSString*)[[personTable search:3] readCoding] isEqualToString:@"3"]);
                        assert([(NSString*)[[personTable search:4] readCoding] isEqualToString:@"4"]);
                        assert([(NSString*)[[personTable search:5] readCoding] isEqualToString:@"5"]);
                        assert([(NSString*)[[personTable search:6] readCoding] isEqualToString:@"6"]);
                    }
                    [[[DbDatabase alloc] initOpen] drop];
                }
                {
                    [[[DbDatabase alloc] initCreate] commit];
                    {
                        DbDatabase *db = [[DbDatabase alloc] initOpen];
                        DbTable* personTable = [db createTable:@"person" page:1 slot:1];
                        assert(![personTable insert:10 data:[[[DbOutData alloc] init] writeCoding:@"1"]]);
                        assert(![personTable insert:2 data:[[[DbOutData alloc] init] writeCoding:@"2"]]);
                        assert(![personTable insert:30 data:[[[DbOutData alloc] init] writeCoding:@"3"]]);
                        assert(![personTable insert:4 data:[[[DbOutData alloc] init] writeCoding:@"4"]]);
                        assert(![personTable insert:50 data:[[[DbOutData alloc] init] writeCoding:@"5"]]);
                        assert(![personTable insert:1 data:[[[DbOutData alloc] init] writeCoding:@"6"]]);
                        [db commit];
                    }
                    {
                        DbDatabase *db = [[DbDatabase alloc] initOpen];
                        DbTable* personTable = [db openTable:@"person"];
                        assert([(NSString*)[[personTable search:10] readCoding] isEqualToString:@"1"]);
                        assert([(NSString*)[[personTable search:2] readCoding] isEqualToString:@"2"]);
                        assert([(NSString*)[[personTable search:30] readCoding] isEqualToString:@"3"]);
                        assert([(NSString*)[[personTable search:4] readCoding] isEqualToString:@"4"]);
                        assert([(NSString*)[[personTable search:50] readCoding] isEqualToString:@"5"]);
                        assert([(NSString*)[[personTable search:1] readCoding] isEqualToString:@"6"]);
                    }
                    [[[DbDatabase alloc] initOpen] drop];
                }
                {
                    [[[DbDatabase alloc] initCreate] commit];
                    {
                        DbDatabase *db = [[DbDatabase alloc] initOpen];
                        DbTable* personTable = [db createTable:@"person" page:1 slot:1];
                        assert(![personTable insert:10 data:[[[DbOutData alloc] init] writeCoding:@"1"]]);
                        assert(11 == [personTable insert:[[[DbOutData alloc] init] writeCoding:@"2"]]);
                        assert(12 == [personTable insert:[[[DbOutData alloc] init] writeCoding:@"3"]]);
                        [db commit];
                    }
                    {
                        DbDatabase *db = [[DbDatabase alloc] initOpen];
                        DbTable* personTable = [db openTable:@"person"];
                        assert([(NSString*)[[personTable search:10] readCoding] isEqualToString:@"1"]);
                        assert([(NSString*)[[personTable search:11] readCoding] isEqualToString:@"2"]);
                        assert([(NSString*)[[personTable search:12] readCoding] isEqualToString:@"3"]);
                    }
                    [[[DbDatabase alloc] initOpen] drop];
                }
                {
                    [[[DbDatabase alloc] initCreate] commit];
                    {
                        DbDatabase *db = [[DbDatabase alloc] initOpen];
                        DbTable* personTable = [db createTable:@"person" page:1 slot:3];
                        DbIndex* personIndex = [db createIndex:@"personIndex"];
                        assert(![personTable insert:1 data:[[[DbOutData alloc] init] writeCoding:@"1"]]);
                        assert(![personTable insert:2 data:[[[DbOutData alloc] init] writeCoding:@"2"]]);
                        assert(![personTable insert:3 data:[[[DbOutData alloc] init] writeCoding:@"3"]]);
                        assert(TRUE == [personIndex insert:1 id:1]);
                        assert(TRUE == [personIndex insert:2 id:2]);
                        assert(TRUE == [personIndex insert:3 id:3]);
                        [db commit];
                    }
                    {
                        DbDatabase *db = [[DbDatabase alloc] initOpen];
                        DbTable* personTable = [db openTable:@"person"];
                        DbIndex* personIndex = [db openIndex:@"personIndex"];
                        assert([(NSString*)[[personTable search:1] readCoding] isEqualToString:@"1"]);
                        assert([(NSString*)[[personTable search:2] readCoding] isEqualToString:@"2"]);
                        assert([(NSString*)[[personTable search:3] readCoding] isEqualToString:@"3"]);
                        assert(1 == [personIndex search:1]);
                        assert(2 == [personIndex search:2]);
                        assert(3 == [personIndex search:3]);
                    }
                    {
                        DbDatabase *db = [[DbDatabase alloc] initOpen];
                        DbTable* personTable = [db openTable:@"person"];
                        assert(![personTable insert:2 data:[[[DbOutData alloc] init] writeCoding:@"20"]]);
                        [db commit];
                    }
                    {
                        DbDatabase *db = [[DbDatabase alloc] initOpen];
                        DbTable* personTable = [db openTable:@"person"];
                        DbIndex* personIndex = [db openIndex:@"personIndex"];
                        assert([(NSString*)[[personTable search:1] readCoding] isEqualToString:@"1"]);
                        assert([(NSString*)[[personTable search:2] readCoding] isEqualToString:@"20"]);
                        assert([(NSString*)[[personTable search:3] readCoding] isEqualToString:@"3"]);
                        assert(1 == [personIndex search:1]);
                        assert(2 == [personIndex search:2]);
                        assert(3 == [personIndex search:3]);
                    }
                    [[[DbDatabase alloc] initOpen] drop];
                }
            }
		}
	}
	CFShow(CFSTR("Finished\n"));
    return 0;
}