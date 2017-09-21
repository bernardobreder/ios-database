//
//  DbDatabase.h
//  dbapi4i
//
//  Created by Bernardo Breder on 08/07/14.
//  Copyright (c) 2014 Breder Organization. All rights reserved.
//

#import <CoreFoundation/CoreFoundation.h>

@class DbIO;
@class DbFileIO;
@class DbSecurityFileIO;
@class DbMemoryIO;
@class DbInput;
@class DbOutput;
@class DbTable;
@class DbIndex;
@class DbDatabase;

@protocol DbIO <NSObject>

@required

- (BOOL)exist:(NSString*)name;

- (DbInput*)read:(NSString*)name;

- (void)write:(NSString*)name data:(DbOutput*)data;

- (BOOL)remove:(NSString*)name;

- (void)commit;

- (void)rollback;

@end

@interface DbFileIO : NSObject <DbIO>

@property (nonatomic, strong) NSData *keyData;

@property (nonatomic, strong) NSData *ivData;

@end

@interface DbSecurityFileIO : DbFileIO

@end

@interface DbMemoryIO : NSObject <DbIO>

@end

@interface DbInput : NSObject

- (id)initWithData:(NSData*)data;

- (NSUInteger)size;

- (uint8_t)readUInt8;

- (uint16_t)readUInt16;

- (uint32_t)readUInt32;

- (uint64_t)readUInt64;

- (double)readDouble;

- (NSData*)readData:(size_t)length;

- (NSMutableDictionary*)readMap;

- (NSMutableArray*)readArray;

- (NSObject*)readObject;

@end

@interface DbOutput : NSObject

- (DbOutput*)writeUInt8:(uint8_t)value;

- (DbOutput*)writeUInt16:(uint16_t)value;

- (DbOutput*)writeUInt32:(uint32_t)value;

- (DbOutput*)writeUInt64:(uint64_t)value;

- (DbOutput*)writeDouble:(double)value;

- (DbOutput*)writeBytes:(uint8_t*)value length:(NSUInteger)length;

- (DbOutput*)writeMap:(NSDictionary*)value;

- (DbOutput*)writeArray:(NSArray*)value;

- (DbOutput*)writeObject:(NSObject*)value;

- (NSData*)toData;

@end

@interface DbTable : NSObject

@property (nonatomic, strong) NSString *name;

@property (nonatomic, assign, readonly) int size;

@property (nonatomic, strong) NSMutableDictionary *entrys;

- (id)init:(NSString*)name io:(id<DbIO>)io slot:(int)slot;

- (id)init:(NSString*)name io:(id<DbIO>)io;

- (DbInput*)get:(int)key;

- (BOOL)contain:(int)key;

- (int)add:(NSData*)data;

- (void)add:(NSData*)data id:(int)id;

- (void)remove:(int)id;

- (void)drop;

- (void)commit;

- (void)rollback;

@end

@interface DbIndex : NSObject

- (id)init:(NSString*)name io:(id<DbIO>)io slot:(int)slot;

- (id)init:(NSString*)name io:(id<DbIO>)io;

- (NSMutableIndexSet*)get:(int)key;

- (BOOL)contain:(int)key;

- (void)add:(uint32_t)key set:(NSIndexSet*)indexSet;

- (void)add:(uint32_t)key index:(uint32_t)value;

- (void)remove:(int)id;

- (void)search:(uint32_t)key callback:(void(^)(uint32_t value, BOOL* stop))callback;

- (void)drop;

- (void)commit;

- (void)rollback;

@end

@interface DbDatabase : NSObject

@property (nonatomic, assign) uint32_t version;

- (id)init:(id<DbIO>)io;

- (DbTable*)createTable:(NSString*)name slot:(int)slot;

- (DbTable*)openTable:(NSString*)name;

- (DbIndex*)createIndex:(NSString*)name slot:(int)slot;

- (DbIndex*)openIndex:(NSString*)name;

- (void)drop;

- (void)commit;

- (void)rollback;

@end
