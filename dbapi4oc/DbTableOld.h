//
//  DbTable.h
//  dbapi4oc
//
//  Created by Bernardo Breder on 18/06/14.
//  Copyright (c) 2014 Bernardo Breder. All rights reserved.
//

#import <Foundation/Foundation.h>

@interface DbTable : NSObject

@property (nonatomic, assign) int size;

- (id)initWithInitialCapacity:(NSUInteger)initialCapacity loadFactor:(int)loadFactor;

- (int)add:(NSObject*)value;

- (id)get:(int)key;

@end

@interface DbTablePage : NSObject

@property (nonatomic, strong) NSMutableArray *entitys;

@property (nonatomic, assign) BOOL changed;

@end

@interface DbTableEntity : NSObject

@property (nonatomic, assign) int key;

@property (nonatomic, strong) NSObject *value;

@end