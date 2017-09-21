//
//  main.m
//  dbapi4i
//
//  Created by Bernardo Breder on 25/06/14.
//  Copyright (c) 2014 Breder Organization. All rights reserved.
//

#import <UIKit/UIKit.h>

#import "AppDelegate.h"
#import "DbDatabase.h"

int main(int argc, char * argv[])
{
	@autoreleasepool {
        if (false) {
            DbFileIO *io = [[DbFileIO alloc] init];
            io.keyData = [@"ßernardo Tavares ßreder ação" dataUsingEncoding:NSUTF8StringEncoding];;
            {
                DbDatabase *db = [[DbDatabase alloc] init:io];
                [db createTable:@"person" slot:4];
                [db commit];
            }
            {
                DbDatabase *db = [[DbDatabase alloc] init:io];
                DbTable *table = [db openTable:@"person"];
                [table add:[[[[DbOutput alloc] init] writeUInt32:1] toData]];
                [db commit];
            }
            {
                DbDatabase *db = [[DbDatabase alloc] init:io];
                DbTable *table = [db openTable:@"person"];
                [table add:[[[[DbOutput alloc] init] writeUInt32:2] toData]];
                [db commit];
            }
            {
                DbDatabase *db = [[DbDatabase alloc] init:io];
                DbTable *table = [db openTable:@"person"];
                NSLog(@"1 == %d", [[table get:1] readUInt32]);
                NSLog(@"2 == %d", [[table get:2] readUInt32]);
                [db rollback];
            }
            {
                DbDatabase *db = [[DbDatabase alloc] init:io];
                [db drop];
            }
        } else {
            return UIApplicationMain(argc, argv, nil, NSStringFromClass([AppDelegate class]));
        }
	}
}
