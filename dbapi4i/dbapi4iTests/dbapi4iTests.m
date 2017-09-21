//
//  dbapi4iTests.m
//  dbapi4iTests
//
//  Created by Bernardo Breder on 25/06/14.
//  Copyright (c) 2014 Breder Organization. All rights reserved.
//

#import <XCTest/XCTest.h>
#include "DbDatabase.h"

@interface dbapi4iTests : XCTestCase

@end

@implementation dbapi4iTests

- (void)setUp
{
    [super setUp];
}

- (void)tearDown
{
    [super tearDown];
}

#define SLOT_MAX 32
#define ITEM_MAX 32

- (void)testTableAddAndCheck
{
    for (int s = 1; s <= SLOT_MAX ; s++) {
        NSLog(@"[testAddAndCheck]: Slot: %d", s);
		@autoreleasepool {
			id<DbIO> io = [[DbMemoryIO alloc] init];
			{
				DbTable *table = [[DbTable alloc] init:@"person" io:io slot:s];
				for (int n = 1; n <= ITEM_MAX ; n++) {
					[table add:[[[[DbOutput alloc] init] writeUInt32:n] toData]];
				}
				[table commit];
			}
			{
				DbTable *table = [[DbTable alloc] init:@"person" io:io];
				for (int n = 1; n <= ITEM_MAX ; n++) {
					XCTAssertEqual(n, [[table get:n] readUInt32]);
				}
				[table rollback];
			}
		}
    }
}

- (void)testTableAddAndRemove
{
    for (int s = 1; s <= SLOT_MAX ; s++) {
        NSLog(@"[testTableAddAndRemove]: Slot: %d", s);
		@autoreleasepool {
			id<DbIO> io = [[DbMemoryIO alloc] init];
			{
				DbTable *table = [[DbTable alloc] init:@"person" io:io slot:s];
				for (int n = 1; n <= ITEM_MAX ; n++) {
					[table add:[[[[DbOutput alloc] init] writeUInt32:n] toData]];
				}
				[table commit];
			}
			{
				DbTable *table = [[DbTable alloc] init:@"person" io:io];
				for (int n = 1; n <= ITEM_MAX ; n++) {
					[table remove:n];
				}
				[table commit];
			}
			{
				DbTable *table = [[DbTable alloc] init:@"person" io:io];
				for (int n = 1; n <= ITEM_MAX ; n++) {
					XCTAssertNil([table get:n]);
				}
				[table rollback];
			}
		}
    }
}

- (void)testTableAddAndSet
{
    for (int s = 1; s <= SLOT_MAX ; s++) {
        NSLog(@"[testTableAddAndSet]: Slot: %d", s);
		@autoreleasepool {
			id<DbIO> io = [[DbMemoryIO alloc] init];
			{
				DbTable *table = [[DbTable alloc] init:@"person" io:io slot:s];
				for (int n = 1; n <= ITEM_MAX ; n++) {
					[table add:[[[[DbOutput alloc] init] writeUInt32:n] toData]];
				}
				[table commit];
			}
			{
				DbTable *table = [[DbTable alloc] init:@"person" io:io];
				for (int n = 1; n <= ITEM_MAX ; n++) {
					[table add:[[[[DbOutput alloc] init] writeUInt32:n+1] toData] id:n];
				}
				[table commit];
			}
			{
				DbTable *table = [[DbTable alloc] init:@"person" io:io];
				for (int n = 1; n <= ITEM_MAX ; n++) {
					XCTAssertEqual(n + 1, [[table get:n] readUInt32]);
				}
				[table rollback];
			}
		}
	}
}

- (void)testIndex
{
    for (int s = 1; s <= SLOT_MAX ; s++) {
        NSLog(@"[testIndex]: Slot: %d", s);
		@autoreleasepool {
			id<DbIO> io = [[DbMemoryIO alloc] init];
			{
				DbIndex *index = [[DbIndex alloc] init:@"person" io:io slot:s];
				for (int n = 1; n <= ITEM_MAX ; n++) {
					NSMutableIndexSet *set = [[NSMutableIndexSet alloc] init];
					[set addIndex:n];
					[index add:n set:set];
				}
				[index commit];
			}
			{
				DbIndex *index = [[DbIndex alloc] init:@"person" io:io];
				for (int n = 1; n <= ITEM_MAX ; n++) {
					NSIndexSet *set = [index get:n];
					XCTAssertNotNil(set);
					XCTAssertEqual(1, set.count);
					XCTAssertTrue([set containsIndex:n]);
				}
				[index rollback];
			}
			{
				DbIndex *index = [[DbIndex alloc] init:@"person" io:io];
				for (int n = ITEM_MAX+1; n <= 2*ITEM_MAX ; n++) {
					NSMutableIndexSet *set = [[NSMutableIndexSet alloc] init];
					[set addIndex:n];
					[index add:n set:set];
				}
				[index commit];
			}
			{
				DbIndex *index = [[DbIndex alloc] init:@"person" io:io];
				for (int n = 1; n <= 2*ITEM_MAX ; n++) {
					NSIndexSet *set = [index get:n];
					XCTAssertNotNil(set);
					XCTAssertEqual(1, set.count);
					XCTAssertTrue([set containsIndex:n]);
				}
				[index rollback];
			}
		}
	}
}

- (void)testDatabase
{
    for (int s = 1; s <= SLOT_MAX ; s++) {
        NSLog(@"[testDatabase]: Slot: %d", s);
		@autoreleasepool {
			id<DbIO> io = [[DbMemoryIO alloc] init];
			{
				DbDatabase *db = [[DbDatabase alloc] init:io];
				DbTable *person = [db createTable:@"person" slot:s];
				DbTable *phone = [db createTable:@"phone" slot:s];
				DbIndex *index = [db createIndex:@"index" slot:s];
				for (int n = 1; n <= ITEM_MAX ; n++) {
					[person add:[[[[DbOutput alloc] init] writeUInt32:n] toData]];
					[phone add:[[[[DbOutput alloc] init] writeUInt32:n + 1] toData]];
					[index add:n set:[[NSMutableIndexSet alloc] initWithIndex:n]];
				}
				[db commit];
			}
			{
				DbDatabase *db = [[DbDatabase alloc] init:io];
				DbTable *person = [db openTable:@"person"];
				DbTable *phone = [db openTable:@"phone"];
				DbIndex *index = [db openIndex:@"index"];
				for (int n = 1; n <= ITEM_MAX ; n++) {
					XCTAssertEqual(n, [[person get:n] readUInt32]);
					XCTAssertEqual(n + 1, [[phone get:n] readUInt32]);
					XCTAssertTrue([[index get:n] containsIndex:n]);
				}
				[db rollback];
			}
		}
    }
}

- (void)testDouble
{
    double values[] = { 1.0, 0.5, 0.85, -1.0, -0.5, 56.65, -0.01 };
    for (int n = 0; n < sizeof(values) / sizeof(double) ; n++) {
        double value = values[n];
        DbOutput *out = [[DbOutput alloc] init];
        [out writeDouble:value];
        DbInput *in = [[DbInput alloc] initWithData:[out toData]];
        XCTAssertEqual(value, [in readDouble]);
    }
}

@end
