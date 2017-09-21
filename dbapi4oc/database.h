//
//  main.c
//  dbapi4oc
//
//  Created by Bernardo Breder on 18/06/14.
//  Copyright (c) 2014 Bernardo Breder. All rights reserved.
//

#include <stdlib.h>
#include <stdio.h>

void db_bit_write_uint32(uint8_t* bytes, uint32_t value);

void db_bit_write_int32(uint8_t* bytes, int32_t value);

void db_bit_write_uint16(uint8_t* bytes, uint16_t value);

void db_bit_write_int16(uint8_t* bytes, int16_t value);

void db_bit_write_uint8(uint8_t* bytes, uint8_t value);

void db_bit_write_int8(uint8_t* bytes, int8_t value);

struct db_bytes_t* db_bytes_create(size_t capacity) ;

void db_bytes_free(struct db_bytes_t* self);

void db_bytes_reset(struct db_bytes_t* self);

uint8_t* db_bytes_result(struct db_bytes_t* self);

size_t db_bytes_size(struct db_bytes_t* self);

uint8_t db_bytes_write_bytes(struct db_bytes_t* self, const uint8_t* bytes, size_t length);

uint8_t db_bytes_write_ascii(struct db_bytes_t* self, const char* text, uint16_t length);

uint8_t db_bytes_write_utf8(struct db_bytes_t* self, const wchar_t* text, uint16_t length);

uint8_t db_bytes_write_uint64(struct db_bytes_t* self, uint64_t value);

uint8_t db_bytes_write_uint32(struct db_bytes_t* self, uint32_t value);

uint8_t db_bytes_write_uint16(struct db_bytes_t* self, uint16_t value);

uint8_t db_bytes_write_uint8(struct db_bytes_t* self, uint8_t value);

uint8_t db_bytes_write_uint32_compressed(struct db_bytes_t* self, uint32_t value);

uint8_t db_bytes_write_uint64_compressed(struct db_bytes_t* self, uint64_t value);

uint32_t db_bytes_read_uint32_compressed(uint8_t** bytes) ;

uint64_t db_bytes_read_uint64_compressed(uint8_t** bytes) ;

uint64_t db_bytes_read_uint64(uint8_t* bytes) ;

uint32_t db_bytes_read_uint32(uint8_t* bytes) ;

int32_t db_bytes_read_int32(uint8_t* bytes);

uint16_t db_bytes_read_uint16(uint8_t* bytes) ;

uint8_t db_bytes_read_uint8(uint8_t* bytes) ;

wchar_t* db_bytes_read_utf8(uint8_t* bytes, uint16_t* length);

char* db_bytes_read_ascii(uint8_t** bytes, uint16_t* length);

uint8_t* db_file_read_bytes_range(FILE* file, size_t offset, size_t length);

uint8_t db_file_write_bytes_range(FILE* file, size_t offset, uint8_t* bytes, size_t length);

uint8_t* db_file_read_bytes(const char* filename, size_t* length) ;

uint8_t db_file_write_bytes(const char* filename, uint8_t* bytes, size_t length);

uint8_t db_file_exist(const char* filename) ;

struct db_fileset_t* db_fileset_create(const char* name);

struct db_fileset_t* db_fileset_open(const char* name);

uint8_t db_fileset_drop(struct db_fileset_t* self);

void db_fileset_free(struct db_fileset_t* self);

size_t db_fileset_size(struct db_fileset_t* self);

uint8_t* db_fileset_get(struct db_fileset_t* self, size_t index);

uint8_t db_fileset_set(struct db_fileset_t* self, size_t index, uint8_t* bytes, size_t size);

ssize_t db_fileset_add(struct db_fileset_t* self, uint8_t* bytes, size_t size);

uint8_t db_fileset_remove(struct db_fileset_t* self, size_t index);

struct db_table_t* db_table_create(const char* name, int32_t page, int32_t slot) ;

struct db_table_t* db_table_open(const char* name) ;

void db_table_free(struct db_table_t* self) ;

int32_t db_table_add(struct db_table_t* self, uint8_t* data, size_t size);

uint8_t db_table_put(struct db_table_t* self, uint32_t id, uint8_t* data, size_t size);

struct db_table_data_t* db_table_get_entry(struct db_table_t* self, uint32_t key, uint8_t maskChanged);

uint8_t* db_table_get(struct db_table_t* self, uint32_t key, size_t* length);

uint8_t db_table_set(struct db_table_t* self, uint32_t key, uint8_t* data, size_t size);

uint8_t db_table_remove(struct db_table_t* self, uint32_t key);

uint32_t db_table_size(struct db_table_t* self);

uint8_t db_table_drop(struct db_table_t* self);

uint8_t db_table_commit(struct db_table_t* self);

uint8_t db_table_rollback(struct db_table_t* self);

struct db_index_t* db_index_create(const char* name, int32_t page, int32_t slot);

struct db_index_t* db_index_open(const char* name);

void db_index_free(struct db_index_t* self);

int32_t db_index_get(struct db_index_t* self, int64_t key);

uint32_t db_index_search_range(struct db_index_t* self, uint64_t key, uint32_t offset, uint32_t limit, uint32_t* array);

uint8_t db_index_add(struct db_index_t* self, int64_t key, uint32_t value);

uint8_t db_index_remove(struct db_index_t* self, int64_t key);

uint8_t db_index_drop(struct db_index_t* self);

uint8_t db_index_commit(struct db_index_t* self);

uint8_t db_index_rollback(struct db_index_t* self);

struct db_database_t* db_database_create();

struct db_database_t* db_database_open();

uint8_t db_database_exist();

void db_database_free(struct db_database_t* self);

uint8_t db_database_drop(struct db_database_t* self);

void db_database_set_version(struct db_database_t* self, uint32_t version);

uint32_t db_database_get_version(struct db_database_t* self);

struct db_table_t* db_database_table_create(struct db_database_t* self, const char* name, uint32_t pages, uint32_t slots) ;

struct db_table_t* db_database_table_open(struct db_database_t* self, const char* name);

struct db_index_t* db_database_index_create(struct db_database_t* self, const char* name, uint32_t pages, uint32_t slots) ;

struct db_index_t* db_database_index_open(struct db_database_t* self, const char* name);

uint8_t db_database_commit(struct db_database_t* self) ;

uint8_t db_database_rollback(struct db_database_t* self) ;

int32_t db_database_insert(struct db_database_t* self, int32_t table_index, uint8_t* bytes, size_t size) ;

uint8_t db_database_update(struct db_database_t* self, int32_t table_index, uint32_t id, uint8_t* bytes, size_t size) ;

uint8_t db_database_remove(struct db_database_t* self, int32_t table_index, uint32_t id) ;

#ifdef __OBJC2__

#import <Foundation/Foundation.h>

@class DbData;
@class DbInData;
@class DbOutData;
@class DbDatabase;
@class DbTable;
@class DbIndex;

@interface DbDatabase : NSObject

+ (BOOL)exist;

- (DbDatabase*)drop;

- (DbDatabase*)initCreate;

- (DbDatabase*)initOpen;

- (DbTable*)createTable:(NSString*)name page:(uint32_t)pages slot:(uint32_t)slots;

- (DbTable*)openTable:(NSString*)name;

- (DbIndex*)createIndex:(NSString*)name;

- (DbIndex*)openIndex:(NSString*)name;

- (BOOL)commit;

- (BOOL)rollback;

@property (nonatomic, assign) uint32_t version;

@end

@interface DbTable : NSObject

- (DbIndex*)createIndex:(NSString*)name;

- (DbIndex*)openIndex:(NSString*)name;

- (DbInData*)search:(uint32_t)id;

- (BOOL)contain:(uint32_t)id;

- (uint32_t)insert:(DbOutData*)data;

- (uint8_t)insert:(uint32_t)id data:(DbOutData*)data;

- (BOOL)update:(uint32_t)id data:(DbOutData*)data;

- (BOOL)remove:(uint32_t)id;

- (uint32_t)size;

@end

@interface DbIndex : NSObject

- (uint32_t)search:(uint64_t)key;

- (BOOL)insert:(uint64_t)key id:(uint32_t)value;

- (BOOL)remove:(uint64_t)key;

- (uint32_t)search:(uint64_t)key offset:(uint32_t)offset limit:(uint32_t)limit array:(uint32_t*)array;

- (void)search:(uint64_t)key callback:(void(^)(uint32_t id, BOOL* stop))callback;

@end

@interface DbInData : NSObject

- (uint8_t)readUByte;

- (uint16_t)readUShort;

- (uint32_t)readUInt;

- (uint64_t)readULong;

- (NSString*)readStringAscii;

- (id<NSCoding>)readCoding;

- (DbInData*)reset;

@end

@interface DbOutData : NSObject

- (DbOutData*)writeUByte:(uint8_t)value;

- (DbOutData*)writeUShort:(uint16_t)value;

- (DbOutData*)writeUInt:(uint32_t)value;

- (DbOutData*)writeULong:(uint64_t)value;

- (DbOutData*)writeStringAscii:(NSString*)value;

- (DbOutData*)writeCoding:(id<NSCoding>)value;

- (DbOutData*)reset;

@end

#endif