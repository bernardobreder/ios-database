//
//  DbTable.m
//  dbapi4oc
//
//  Created by Bernardo Breder on 18/06/14.
//  Copyright (c) 2014 Bernardo Breder. All rights reserved.
//

#import "DbTable.h"

#define MAXIMUM_CAPACITY 0x7FFFFFFF

@interface DbTable ()

@property (nonatomic, assign) NSUInteger loadFactor;

@property (nonatomic, assign) NSUInteger maxSize;

@property (nonatomic, assign) NSUInteger maxCell;

@property (nonatomic, strong) NSMutableArray *pages;

@end

@interface DbTablePage ()

@property (nonatomic, assign) NSUInteger size;

- (id)initWithOrder:(NSUInteger)order;

@end

@implementation DbTable

@synthesize pages = _pages;

- (id)initWithInitialCapacity:(NSUInteger)initialCapacity loadFactor:(int)loadFactor
{
	if (!(self = [super init])) return nil;
	_loadFactor = loadFactor;
	_maxSize = MIN(initialCapacity * loadFactor, MAXIMUM_CAPACITY);
	_maxCell = _maxSize / initialCapacity;
	_pages = [[NSMutableArray alloc] initWithCapacity:initialCapacity];
	for (NSUInteger n = 0; n < initialCapacity ; n++) {
		[_pages addObject:[NSNull null]];
	}
	return self;
}

+ (NSInteger)binarySearch:(NSArray*)entitys low:(NSUInteger)low high:(NSUInteger)high key:(int)key
{
	while (low <= high) {
		NSUInteger mid = (low + high) >> 1;
		int midVal = ((DbTableEntity*)entitys[mid]).key;
		if (midVal < key) {
			low = mid + 1;
		} else if (midVal > key) {
			high = mid - 1;
		} else {
			return mid;
		}
	}
	return -low;
}

- (void)resize:(NSUInteger)newCapacity
{
	_maxSize = MIN(newCapacity * _loadFactor, MAXIMUM_CAPACITY);
	_maxCell = _maxSize / _pages.count;
	NSMutableArray *newPages = [[NSMutableArray alloc] initWithCapacity:newCapacity];
	for (NSUInteger n = 0; n < _pages.count ; n++) {
		DbTablePage *page = _pages[n];
		for (NSUInteger cellIndex = 0; cellIndex < page.entitys.count; cellIndex++) {
			DbTableEntity *entity = page.entitys[cellIndex];
			int index = entity.key & (newPages.count - 1);
			DbTablePage *newPage = newPages[index];
			if (!newPage) {
				newPages[index] = newPage = [[DbTablePage alloc] initWithOrder:_maxCell];
				newPage.changed = true;
			}
			[newPage.entitys addObject:entity];
		}
	}
	_pages = newPages;
}

- (id)get:(int)key
{
	int pageIndex = key & (_pages.count - 1);
    DbTablePage *page = _pages[pageIndex];
    if ((NSNull*)page == [NSNull null]) {
		return nil;
    }
    NSInteger cellIndex = [DbTable binarySearch:page.entitys low:0 high:page.size-1 key:key];
    if (cellIndex < 0) {
		return nil;
    }
	DbTableEntity *entity = page.entitys[cellIndex];
    return entity.value;
}

- (int)add:(NSObject*)value
{
	int key = _size + 1;
	if (_size >= _maxSize) {
		[self resize:2 * _pages.count];
	}
	NSUInteger pageIndex = key & (_pages.count - 1);
	DbTablePage *page = _pages[pageIndex];
	if ((NSNull*)page == [NSNull null]) {
		_pages[pageIndex] = page = [[DbTablePage alloc] initWithOrder:_maxCell];
	}
	DbTableEntity *entity = [[DbTableEntity alloc] init];
	entity.key = key;
	entity.value = value;
	[page.entitys addObject:entity];
	page.changed = true;
	page.size++;
	_size++;
	return key;
}

@end

@implementation DbTablePage

- (id)initWithOrder:(NSUInteger)order
{
	if (!(self = [super init])) return nil;
	_entitys = [[NSMutableArray alloc] initWithCapacity:order];
	return self;
}

@end

@implementation DbTableEntity

@synthesize key = _key;

@synthesize value = _value;

@end