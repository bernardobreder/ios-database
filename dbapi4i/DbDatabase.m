//
//  DbDatabase.m
//  dbapi4i
//
//  Created by Bernardo Breder on 08/07/14.
//  Copyright (c) 2014 Breder Organization. All rights reserved.
//

#import "DbDatabase.h"

#if __OBJC2__
#import <Foundation/Foundation.h>
#import <CommonCrypto/CommonDigest.h>
#import <CommonCrypto/CommonCryptor.h>
#import <CommonCrypto/CommonHMAC.h>

@interface NSData (Security)

- (NSData*)encrypt:(NSData*)keyData ivData:(NSData*)ivData;

@end

@implementation NSData (Security)

- (NSData*)encrypt:(NSData*)keyData ivData:(NSData*)ivData
{
    if (keyData.length != 16 && keyData.length != 24 && keyData.length != 32) return nil;
    if (keyData.length != ivData.length) return nil;
    CCCryptorRef cryptor = NULL;
	if ( CCCryptorCreate( kCCEncrypt, kCCAlgorithmAES128, kCCOptionPKCS7Padding, [keyData bytes], [keyData length], [ivData bytes], &cryptor) != kCCSuccess ) return nil;
    size_t bufsize = CCCryptorGetOutputLength(cryptor, (size_t)[self length], true);
    void* buf = malloc(bufsize);
    size_t bufused = 0;
    size_t bytesTotal = 0;
    if (CCCryptorUpdate(cryptor, [self bytes], (size_t)[self length], buf, bufsize, &bufused) != kCCSuccess) {
        free(buf);
        return nil;
    }
    bytesTotal += bufused;
    if (CCCryptorFinal(cryptor, buf + bufused, bufsize - bufused, &bufused) != kCCSuccess) {
        free(buf);
        return nil;
    }
    bytesTotal += bufused;
	CCCryptorRelease(cryptor);
	return [NSData dataWithBytesNoCopy: buf length: bytesTotal];
}

- (NSData*)decrypted:(NSData*)keyData ivData:(NSData*)ivData
{
    if (keyData.length != 16 && keyData.length != 24 && keyData.length != 32) return nil;
    if (keyData.length != ivData.length) return nil;
    CCCryptorRef cryptor = NULL;
	if ( CCCryptorCreate( kCCDecrypt, kCCAlgorithmAES128, kCCOptionPKCS7Padding, [keyData bytes], [keyData length], [ivData bytes], &cryptor) != kCCSuccess ) return nil;
    size_t bufsize = CCCryptorGetOutputLength(cryptor, (size_t)[self length], true);
    void* buf = malloc(bufsize);
    size_t bufused = 0;
    size_t bytesTotal = 0;
    if (CCCryptorUpdate(cryptor, [self bytes], (size_t)[self length], buf, bufsize, &bufused) != kCCSuccess) {
        free(buf);
        return nil;
    }
    bytesTotal += bufused;
    if (CCCryptorFinal(cryptor, buf + bufused, bufsize - bufused, &bufused) != kCCSuccess) {
        free(buf);
        return nil;
    }
    bytesTotal += bufused;
	CCCryptorRelease(cryptor);
	return [NSData dataWithBytesNoCopy: buf length: bytesTotal];
}

@end

#endif

@interface DbTableEntry : NSObject

@property (nonatomic, strong) NSData *data;

@property (nonatomic, assign) BOOL changed;

- (id)init:(NSData*)data changed:(BOOL)changed;

@end

@interface DbFileIO ()

@property (nonatomic, strong) NSMutableDictionary *data;

@end

@implementation DbFileIO

@synthesize data = _data;

@synthesize keyData;

@synthesize ivData;

- (BOOL)recovery
{
	NSFileManager *fs = [NSFileManager defaultManager];
	NSString* path = [[NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES) objectAtIndex:0] stringByAppendingPathComponent:@"database.tmp"];
	if ([fs fileExistsAtPath:path]) {
		NSDictionary *dic = [[NSDictionary alloc] initWithContentsOfFile:path];
		__block BOOL success = true;
		[dic enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *stop) {
			NSString *name = key;
			NSData *data = obj;
			if (![data writeToFile:[[NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES) objectAtIndex:0] stringByAppendingPathComponent:name] atomically:NO]) {
				success = false;
				*stop = true;
			}
		}];
		if (!success) {
			return false;
		}
		[fs removeItemAtPath:path error:nil];
	}
	return true;
}

- (id)init
{
	if (!(self = [super init])) return nil;
	if (![self recovery]) return nil;
	_data = [[NSMutableDictionary alloc] init];
	return self;
}

- (BOOL)exist:(NSString*)name
{
	NSString* path = [[NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES) objectAtIndex:0] stringByAppendingPathComponent:name];
	return [[NSFileManager defaultManager] fileExistsAtPath:path];
}

- (DbInput*)read:(NSString*)name
{
	NSString* path = [[NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES) objectAtIndex:0] stringByAppendingPathComponent:name];
	NSData *data = [NSData dataWithContentsOfFile:path];
	if (!data) @throw [[NSException alloc] initWithName:@"Can not found a node" reason:@"FileNotFoundException" userInfo:nil];
    if (keyData) {
        data = [data decrypted:keyData ivData:ivData ? ivData : keyData];
        if (!data) @throw [[NSException alloc] initWithName:@"Can not decrypt the data" reason:@"SecurityException" userInfo:nil];
    }
	return [[DbInput alloc] initWithData:data];
}

- (void)write:(NSString*)name data:(DbOutput*)output
{
	_data[name] = [output toData];
}

- (BOOL)remove:(NSString*)name
{
	NSString* path = [[NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES) objectAtIndex:0] stringByAppendingPathComponent:name];
	return [[NSFileManager defaultManager] removeItemAtPath:path error:nil];
}

- (void)commit
{
	NSString *dir = [NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES) objectAtIndex:0];
    NSString* path = [dir stringByAppendingPathComponent:@"database.tmp"];
    {
        NSMutableDictionary *backup = [_data mutableCopy];
        [backup enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *stop) {
            NSString *name = key;
            if ([[NSFileManager defaultManager] fileExistsAtPath:[dir stringByAppendingPathComponent:name]]) {
                NSData *data = [NSData dataWithContentsOfFile:[dir stringByAppendingPathComponent:name]];
                backup[name] = data;
            }
        }];
        [backup writeToFile:path atomically:NO];
    }
    __block BOOL success = true;
	[_data enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *stop) {
		NSString *name = key;
		NSData *data = obj;
        if (keyData) {
            data = [data encrypt:keyData ivData:ivData ? ivData : keyData];
            if (!data) @throw [[NSException alloc] initWithName:@"Can not encrypt a node" reason:@"SecurityException" userInfo:nil];
        }
		if (![data writeToFile:[dir stringByAppendingPathComponent:name] atomically:NO]) {
			success = false;
			*stop = true;
		}
	}];
	if (!success) {
		while (![self recovery]) {
			sleep(1);
		}
	}
	[[NSFileManager defaultManager] removeItemAtPath:path error:nil];
	_data = [[NSMutableDictionary alloc] init];
}

- (void)rollback
{
	_data = [[NSMutableDictionary alloc] init];
}

@end

@implementation DbSecurityFileIO

@end

@interface DbMemoryIO ()

@property (nonatomic, strong) NSMutableDictionary *bytes;

@end

@implementation DbMemoryIO

@synthesize bytes = _bytes;

- (id)init
{
	if (!(self = [super init])) return nil;
	_bytes = [[NSMutableDictionary alloc] init];
	return self;
}

- (BOOL)exist:(NSString*)name
{
	return _bytes[name] != nil;
}

- (DbInput*)read:(NSString*)name
{
	NSData *data = _bytes[name];
	if (!data) @throw [[NSException alloc] initWithName:@"Can not found a node" reason:@"FileNotFoundException" userInfo:nil];
	return [[DbInput alloc] initWithData:data];
}

- (void)write:(NSString*)name data:(DbOutput*)data
{
	_bytes[name] = [data toData];
}

- (BOOL)remove:(NSString*)name
{
	BOOL exist = _bytes[name] != nil;
	[_bytes removeObjectForKey:name];
	return exist;
}

- (void)commit
{
}

- (void)rollback
{
}

@end

@interface DbInput ()

@property (nonatomic, assign) const uint8_t *bytes;

@property (nonatomic, assign) size_t offset;

@property (nonatomic, assign) size_t length;

@end

@implementation DbInput

@synthesize bytes = _bytes;

@synthesize offset = _offset;

@synthesize length = _length;

- (id)initWithData:(NSData*)data
{
	if (!(self = [super init])) return nil;
    _bytes = (const uint8_t*)data.bytes;
    _offset = 0;
    _length = data.length;
	return self;
}

- (NSUInteger)size
{
    return _length;
}

- (uint8_t)readUInt8
{
    if (_offset >= _length) {
        @throw [[NSException alloc] initWithName:@"EOF Exception" reason:nil userInfo:nil];
    }
    _offset++;
    return *_bytes++;
}

- (uint16_t)readUInt16
{
    if (_offset + 1 >= _length) {
        @throw [[NSException alloc] initWithName:@"EOF Exception" reason:nil userInfo:nil];
    }
    uint16_t result = (*_bytes++ << 8);
    result += *_bytes++;
    _offset += 2;
    return result;
}

- (uint32_t)readUInt32
{
    if (_offset + 3 >= _length) {
        @throw [[NSException alloc] initWithName:@"EOF Exception" reason:nil userInfo:nil];
    }
    uint32_t result = (*_bytes++ << 24);
    result += (*_bytes++ << 16);
    result += (*_bytes++ << 8);
    result += *_bytes++;
    _offset += 4;
    return result;
}

- (uint64_t)readUInt64
{
    if (_offset + 7 >= _length) {
        @throw [[NSException alloc] initWithName:@"EOF Exception" reason:nil userInfo:nil];
    }
    uint64_t left = (*_bytes++ << 24);
    left += (*_bytes++ << 16);
    left += (*_bytes++ << 8);
    left += *_bytes++;
    uint32_t right = (*_bytes++ << 24);
    right += (*_bytes++ << 16);
    right += (*_bytes++ << 8);
    right += *_bytes++;
    _offset += 8;
    return (left << 32) + right;
}

- (double)readDouble
{
    if (true) {
        NSString *value = [self readUTF];
        return [value doubleValue];
    } else {
        if (_offset + 7 >= _length) {
            @throw [[NSException alloc] initWithName:@"EOF Exception" reason:nil userInfo:nil];
        }
        int ebits = 11, fbits = 52;
        BOOL bits[64];
        int index = 0;
        for (int i = 0; i < 8; i++) {
            int b = *_bytes++;
            _offset++;
            for (int j = 0; j < 8; j++) {
                bits[index++] = b % 2 == 1 ? true : false;
                b = b >> 1;
            }
        }
        int bias = (1 << (ebits - 1)) - 1;
        int s = bits[63] ? -1 : 1;
        int e = 0;
        for (int n = 62; n >= 64 - ebits - 1; n--) {
            if (bits[n]) {
                e += pow(2, ebits - 1 - 62 + n);
            }
        }
        long f = 0;
        int imax = 64 - ebits - 2;
        for (int n = imax; n >= 0; n--) {
            if (bits[n]) {
                f += pow(2, n);
            }
        }
        if (e == (1 << ebits) - 1) {
            return f != 0 ? NAN : s * INFINITY;
        }
        else if (e > 0) {
            return s * pow(2, e - bias) * (1 + f / pow(2, fbits));
        }
        else if (f != 0) {
            return s * pow(2, -(bias - 1)) * (f / pow(2, fbits));
        }
        else {
            return s * 0;
        }
    }
}

- (NSData*)readData:(size_t)length
{
	if (length == 0) return [[NSData alloc] init];
    if (_offset + length - 1 >= _length) {
        @throw [[NSException alloc] initWithName:@"EOF Exception" reason:nil userInfo:nil];
    }
    NSData *data = [NSData dataWithBytes:_bytes length:length];
    _bytes += length;
    _offset += length;
    return data;
}

- (NSString*)readUTF
{
	uint16_t length = [self readUInt16];
	char bytes[length + 1];
        memcpy(bytes, _bytes, length);
	bytes[length] = 0;
	_bytes += length;
	_offset += length;
	return [NSString stringWithCString:bytes encoding:NSUTF8StringEncoding];
}

- (NSObject*)readObject
{
    uint8_t code = [self readUInt8];
    switch (code) {
        case 1:
            return [NSNumber numberWithUnsignedChar:[self readUInt8]];
        case 2:
            return [NSNumber numberWithUnsignedShort:[self readUInt16]];
        case 3:
            return [NSNumber numberWithUnsignedInt:[self readUInt32]];
        case 4:
            return [NSNumber numberWithUnsignedLongLong:[self readUInt64]];
        case 5:
            return [NSNumber numberWithUnsignedChar:[self readUInt8]];
        case 6:
            return [NSNumber numberWithUnsignedShort:[self readUInt16]];
        case 7:
            return [NSNumber numberWithUnsignedInt:[self readUInt32]];
        case 8:
            return [NSNumber numberWithUnsignedLongLong:[self readUInt64]];
        case 9:
            return [NSNumber numberWithDouble:[self readDouble]];
        case 10:
            return [NSNumber numberWithDouble:[self readDouble]];
        case 11:
            return [self readUTF];
        case 12:
            return [self readMap];
        case 13:
            return [self readArray];
        default:
            return nil;
    }
}

- (NSMutableDictionary*)readMap
{
    uint16_t length = [self readUInt16];
	if (length == 0) return [[NSMutableDictionary alloc] init];
    NSMutableDictionary *dic = [[NSMutableDictionary alloc] initWithCapacity:length];
    for (int n = 0 ; n < length ; n++) {
        NSString *key = [self readUTF];
        NSObject *value = [self readObject];
        dic[key] = value;
    }
	return dic;
}

- (NSMutableArray*)readArray
{
    uint16_t length = [self readUInt16];
	if (length == 0) return [[NSMutableArray alloc] init];
    NSMutableArray *result = [[NSMutableArray alloc] initWithCapacity:length];
    for (int n = 0 ; n < length ; n++) {
        NSObject *value = [self readObject];
        [result addObject:value];
    }
	return result;
}

@end

@interface DbOutput ()

@property (nonatomic, strong) NSMutableData *data;

@end

@implementation DbOutput

@synthesize data = _data;

- (id)init
{
    if (!(self = [super init])) return nil;
    _data = [[NSMutableData alloc] init];
    return self;
}

- (DbOutput*)writeUInt8:(uint8_t)value
{
    [_data appendBytes:&value length:1];
    return self;
}

- (DbOutput*)writeUInt16:(uint16_t)value
{
    uint8_t bytes[2];
    bytes[0] = (value >> 8) & 0xFF;
    bytes[1] = value & 0xFF;
    [_data appendBytes:&bytes length:2];
    return self;
}

- (DbOutput*)writeUInt32:(uint32_t)value
{
    uint8_t bytes[4];
    bytes[0] = (value >> 24) & 0xFF;
    bytes[1] = (value >> 16) & 0xFF;
    bytes[2] = (value >> 8) & 0xFF;
    bytes[3] = value & 0xFF;
    [_data appendBytes:&bytes length:4];
	return self;
}

- (DbOutput*)writeUInt64:(uint64_t)value
{
    uint8_t bytes[4];
    bytes[0] = (value >> 24) & 0xFF;
    bytes[1] = (value >> 16) & 0xFF;
    bytes[2] = (value >> 8) & 0xFF;
    bytes[3] = value & 0xFF;
    [_data appendBytes:&bytes length:4];
    return self;
}

- (DbOutput*)writeDouble:(double)value
{
    if (true) {
        [self writeUTF:[[NSNumber numberWithDouble:value] stringValue]];
    } else {
        int ebits = 11, fbits = 52;
        int bias = (1 << (ebits - 1)) - 1;
        BOOL s;
        double f;
        int e;
        if (isnan(value)) {
            e = (1 << bias) - 1;
            f = 1;
            s = false;
        }
        else if (isinf(value)) {
            e = (1 << bias) - 1;
            f = 0;
            s = (value < 0) ? true : false;
        }
        else if (value == 0) {
            e = 0;
            f = 0;
            s = (1 / value == -INFINITY) ? true : false;
        }
        else {
            s = value < 0;
            value = fabs(value);
            if (value >= pow(2, 1 - bias)) {
                double ln2 = 0.6931471805599453;
                double ln = floor(log(value) / ln2) < bias ? floor(log(value) / ln2) : bias;
                e = (int) (ln + bias);
                f = value * pow(2, fbits - ln) - pow(2, fbits);
            }
            else {
                e = 0;
                f = value / pow(2, 1 - bias - fbits);
            }
        }
        BOOL bits[64];
        int index = 0;
        for (int i = 0; i < fbits; i++) {
            bits[index++] = (uint64_t)f % 2 == 1 ? true : false;
            f = floor(f / 2);
        }
        for (int i = 0; i < ebits; i++) {
            bits[index++] = e % 2 == 1 ? true : false;
            e = (int) floor(e / 2);
        }
        bits[index] = s;
        uint8_t bytes[8];
        for (int n = 0; n < 8; n++) {
            int v = 0;
            for (int m = 0; m < 8; m++) {
                if (bits[n * 8 + m]) {
                    v += pow(2, m);
                }
            }
            bytes[n] = v;
        }
        [_data appendBytes:&bytes length:8];
    }
    return self;
}

- (DbOutput*)writeBytes:(uint8_t*)value length:(NSUInteger)length
{
	[_data appendBytes:value length:length];
    return self;
}

- (DbOutput*)writeUTF:(NSString*)value
{
	uint32_t length = 0;
	for (int n = 0; n < value.length ; n++) {
		unichar v = [value characterAtIndex:n];
		if (v <= 0x7F) {
			length++;
		} else if (v <= 0x7FF) {
			length += 2;
		} else {
			length += 3;
		}
	}
	if (length > 0xFFFF) return nil;
	[self writeUInt16:length];
	uint8_t bytes[length+1], *aux = bytes;
	for (int n = 0; n < value.length ; n++) {
		unichar c = [value characterAtIndex:n];
		if (c <= 0x7F) {
			*aux++ = c ;
		} else if (c <= 0x7FF) {
			*aux++ = ((c >> 6) & 0x1F) + 0xC0;
			*aux++ = (c & 0x3F) + 0x80;
		} else {
			*aux++ = ((c >> 12) & 0xF) + 0xE0;
			*aux++ = ((c >> 6) & 0x3F) + 0x80;
			*aux++ = (c & 0x3F) + 0x80;
		}
	}
	[_data appendBytes:bytes length:length];
    return self;
}

- (DbOutput*)writeObject:(NSObject*)value
{
    if ([value isKindOfClass:[NSNumber class]]) {
        NSNumber *numberValue = (NSNumber*)value;
        const char* type = [numberValue objCType];
        if (*type == 'c') {
            [self writeUInt8:1];
            [self writeUInt8:[numberValue charValue]];
        } else if (*type == 's') {
            [self writeUInt8:2];
            [self writeUInt16:[numberValue shortValue]];
        } else if (*type == 'i') {
            [self writeUInt8:3];
            [self writeUInt32:[numberValue intValue]];
        } else if (*type == 'l' || *type == 'q') {
            [self writeUInt8:4];
            [self writeUInt64:[numberValue longLongValue]];
        } else if (*type == 'C') {
            [self writeUInt8:5];
            [self writeUInt8:[numberValue unsignedCharValue]];
        } else if (*type == 'S') {
            [self writeUInt8:6];
            [self writeUInt16:[numberValue unsignedShortValue]];
        } else if (*type == 'I') {
            [self writeUInt8:7];
            [self writeUInt32:[numberValue unsignedIntValue]];
        } else if (*type == 'L' || *type == 'Q') {
            [self writeUInt8:8];
            [self writeUInt64:[numberValue unsignedLongLongValue]];
        } else if (*type == 'f') {
            [self writeUInt8:9];
            [self writeDouble:[numberValue floatValue]];
        } else if (*type == 'd') {
            [self writeUInt8:10];
            [self writeDouble:[numberValue doubleValue]];
        } else {
            @throw [[NSException alloc] initWithName:@"class is unknown" reason:@"ClassException" userInfo:nil];
        }
    } else if ([value isKindOfClass:[NSString class]]) {
        NSString *stringValue = (NSString*)value;
        [self writeUInt8:11];
        [self writeUTF:stringValue];
    } else if ([value isKindOfClass:[NSDictionary class]]) {
        NSDictionary *dicValue = (NSDictionary*)value;
        [self writeUInt8:12];
        [self writeMap:dicValue];
    } else if ([value isKindOfClass:[NSArray class]]) {
        NSArray *arrayValue = (NSArray*)value;
        [self writeUInt8:13];
        [self writeArray:arrayValue];
    } else {
        @throw [[NSException alloc] initWithName:@"class is unknown" reason:@"ClassException" userInfo:nil];
    }
    return self;
}

- (DbOutput*)writeMap:(NSDictionary*)value
{
    if (value.count > 0xFFFF) return nil;
    [self writeUInt16:value.count];
    [value enumerateKeysAndObjectsUsingBlock:^(id keyid, id obj, BOOL *stop) {
        NSString *key = keyid;
        NSObject *value = obj;
        [self writeUTF:key];
        [self writeObject:value];
    }];
    return self;
}

- (DbOutput*)writeArray:(NSArray*)value
{
    if (value.count > 0xFFFF) return nil;
    [self writeUInt16:value.count];
    [value enumerateObjectsUsingBlock:^(id obj, NSUInteger idx, BOOL *stop) {
        NSObject *value = obj;
        [self writeObject:value];
    }];
    return self;
}

- (NSData *)toData
{
	return _data;
}

@end

@interface DbTable ()

@property (nonatomic, strong) id<DbIO> io;

@property (nonatomic, assign) int page;

@property (nonatomic, assign) int slot;

@property (nonatomic, strong) NSMutableIndexSet *dataChanged;

@end

@implementation DbTable

@synthesize name = _name;

@synthesize size = _size;

@synthesize io = _io;

@synthesize page = _page;

@synthesize slot = _slot;

@synthesize entrys = _entrys;

@synthesize dataChanged = _dataChanged;

- (id)init:(NSString*)name io:(id<DbIO>)io slot:(int)slot
{
	if (!(self = [super init])) return nil;
	if ([io exist:[NSString stringWithFormat:@"%@.db", name]]) {
		@throw [[NSException alloc] initWithName:@"table already exist" reason:nil userInfo:nil];
	}
	_name = name;
	_io = io;
	_slot = slot;
	_entrys = [[NSMutableDictionary alloc] init];
	_dataChanged = [[NSMutableIndexSet alloc] init];
	return self;
}

- (id)init:(NSString*)name io:(id<DbIO>)io
{
	if (!(self = [super init])) return nil;
    NSString *path = [NSString stringWithFormat:@"%@.db", name];
	if (![io exist:path]) {
		@throw [[NSException alloc] initWithName:@"table not exist" reason:nil userInfo:nil];
	}
	_name = name;
	_io = io;
    DbInput *in = [io read:path];
    _size = [in readUInt32];
    _page = [in readUInt32];
    _slot = [in readUInt32];
	if ([in readUInt8] != 0xFF) return nil;
	_entrys = [[NSMutableDictionary alloc] initWithCapacity:_page * _slot];
	_dataChanged = [[NSMutableIndexSet alloc] init];
	return self;
}

- (void)loadPage:(int)page
{
	NSNumber *c = [NSNumber numberWithInteger:page * _slot];
	if (!_entrys[c]) {
		NSString *filename = [NSString stringWithFormat:@"%@_%d.db", _name, page];
		if ([_io exist:filename]) {
			DbInput *in = [_io read:filename];
			for (int n = 0, c = page * _slot; n < _slot; n++, c++) {
				size_t bytesLength = [in readUInt32];
				NSData *bytes = [in readData:bytesLength];
				NSNumber *cObj = [NSNumber numberWithInteger:c];
				_entrys[cObj] = [[DbTableEntry alloc] init:bytes changed:false];
			}
		}
	}
}

- (DbInput*)get:(int)key
{
	if (key == 0 || key > _size) return nil;
	[self loadPage:(key - 1) / _slot];
	NSNumber *c = [NSNumber numberWithInteger:key - 1];
	DbTableEntry *entry = _entrys[c];
	if (!entry || entry.data.length == 0) return nil;
	return [[DbInput alloc] initWithData:entry.data];
}

- (BOOL)contain:(int)key
{
	if (key == 0 || key > _size) return false;
	[self loadPage:(key - 1) / _slot];
	NSNumber *c = [NSNumber numberWithInteger:key - 1];
	DbTableEntry *entry = _entrys[c];
	if (!entry) return false;
	if (entry.data.length == 0) return false;
	return true;
}

- (int)add:(NSData*)data
{
	int index = _size;
	int page = index / _slot;
	[self loadPage:page];
	NSNumber *c = [NSNumber numberWithInteger:index];
	_entrys[c] =[[DbTableEntry alloc] init:data changed:YES];
	[_dataChanged addIndex:page];
	_size++;
	_page = ceil((float)_size / _slot);
	return index + 1;
}

- (void)add:(NSData*)data id:(int)id
{
	if (id > _size) _size = id;
	_page = ceil((float)_size / _slot);
	int page = (id - 1) / _slot;
	[self loadPage:page];
	NSNumber *c = [NSNumber numberWithInteger:id - 1];
	_entrys[c] = [[DbTableEntry alloc] init:data changed:YES];
	[_dataChanged addIndex:page];
}

- (void)remove:(int)id
{
	int page = (id - 1) / _slot;
	[self loadPage:page];
	NSNumber *c = [NSNumber numberWithInteger:id - 1];
	_entrys[c] = [[DbTableEntry alloc] init:nil changed:YES];
	[_dataChanged addIndex:page];
}

- (void)drop
{
	[_io remove:[NSString stringWithFormat:@"%@.db", _name]];
	if (_size > 0) for (int n = 0; n <= _size / _slot; n++) {
		[_io remove:[NSString stringWithFormat:@"%@_%d.db", _name, n]];
	}
}

- (void)commit
{
	[_dataChanged enumerateIndexesUsingBlock:^(NSUInteger page, BOOL *stop) {
		DbOutput *out = [[DbOutput alloc] init];
		for (int n = 0, c = (uint32_t)(page * _slot); n < _slot ; n++, c++) {
			NSNumber *cObj = [NSNumber numberWithInteger:c];
			DbTableEntry *entry = _entrys[cObj];
            if (entry) {
                NSData *data = entry.data;
                [out writeUInt32:(uint32_t)data.length];
                [out writeBytes:(uint8_t*)data.bytes length:data.length];
            } else {
                [out writeUInt32:0];
            }
		}
		[out writeUInt8:0xFF];
		[_io write:[NSString stringWithFormat:@"%@_%d.db", _name, (uint32_t)page] data:out];
	}];
    {
        DbOutput *out = [[DbOutput alloc] init];
        [out writeUInt32:_size];
        [out writeUInt32:_page];
        [out writeUInt32:_slot];
		[out writeUInt8:0xFF];
        [_io write:[NSString stringWithFormat:@"%@.db", _name] data:out];
    }
	[_dataChanged removeAllIndexes];
}

- (void)rollback
{
	[_dataChanged enumerateIndexesUsingBlock:^(NSUInteger page, BOOL *stop) {
		for (int n = 0, c = (uint32_t)(page * _slot); n < _slot ; n++, c++) {
            [_entrys removeObjectForKey:[NSNumber numberWithInteger:c]];
		}
	}];
	[_dataChanged removeAllIndexes];
}

@end

@implementation DbTableEntry

@synthesize data = _data;

@synthesize changed = _changed;

- (id)init:(NSData*)data changed:(BOOL)changed;
{
	if (!(self = [super init])) return nil;
	_data = data;
	_changed = changed;
	return self;
}

@end

@interface DbIndex ()

@property (nonatomic, strong) DbTable *table;

@end

@implementation DbIndex

@synthesize table = _table;

- (id)init:(NSString*)name io:(id<DbIO>)io slot:(int)slot
{
    if (!(self = [super init])) return nil;
    if (!(_table = [[DbTable alloc] init:name io:io slot:slot])) {
		return nil;
	}
    return self;
}

- (id)init:(NSString*)name io:(id<DbIO>)io
{
    if (!(self = [super init])) return nil;
    if (!(_table = [[DbTable alloc] init:name io:io])) {
		return nil;
	}
    return self;
}

- (NSMutableIndexSet*)get:(int)key
{
    DbInput *in = [_table get:key];
	if (!in) return nil;
	NSMutableIndexSet *set = [[NSMutableIndexSet alloc] init];
	int size = [in readUInt32];
	for (int n = 0 ; n < size ; n++) {
		[set addIndex:[in readUInt32]];
	}
	if ([in readUInt8] != 0xFF) return nil;
	return set;
}

- (BOOL)contain:(int)key
{
	return [_table get:key] != nil;
}

- (void)add:(uint32_t)key set:(NSIndexSet*)indexSet
{
	DbOutput *out = [[DbOutput alloc] init];
	[out writeUInt32:(uint32_t)indexSet.count];
	[indexSet enumerateIndexesUsingBlock:^(NSUInteger idx, BOOL *stop) {
		[out writeUInt32:(uint32_t)idx];
	}];
	[out writeUInt8:0xFF];
    [_table add:[out toData] id:key];
}

- (void)add:(uint32_t)key index:(uint32_t)value;
{
    NSMutableIndexSet *indexSet = [self get:key];
    if (!indexSet) indexSet = [[NSMutableIndexSet alloc] init];
    [indexSet addIndex:value];
    DbOutput *out = [[DbOutput alloc] init];
	[out writeUInt32:(uint32_t)indexSet.count];
	[indexSet enumerateIndexesUsingBlock:^(NSUInteger idx, BOOL *stop) {
		[out writeUInt32:(uint32_t)idx];
	}];
	[out writeUInt8:0xFF];
    [_table add:[out toData] id:key];
}

- (void)search:(uint32_t)key callback:(void(^)(uint32_t value, BOOL* stop))callback {
    NSMutableIndexSet *indexSet = [self get:key];
    [indexSet enumerateIndexesUsingBlock:^(NSUInteger idx, BOOL *stop) {
        callback((uint32_t)idx, stop);
	}];
}

- (void)remove:(int)id
{
    [_table remove:id];
}

- (void)drop
{
    [_table drop];
}

- (void)commit
{
    [_table commit];
}

- (void)rollback
{
    [_table rollback];
}

@end

@interface DbDatabase ()

@property (nonatomic, strong) id<DbIO> io;

@property (nonatomic, strong) NSMutableDictionary *tables;

@property (nonatomic, strong) NSMutableDictionary *indexs;

@end

@implementation DbDatabase

@synthesize io = _io;

@synthesize tables = _tables;

@synthesize indexs = _indexs;

- (id)init:(id<DbIO>)io
{
	if (!(self = [super init])) return nil;
	_io = io;
	if ([io exist:@"database.db"]) {
		DbInput *in = [io read:@"database.db"];
		uint16_t magic = [in readUInt16];
		if (magic != 0xDBFF) return nil;
		_version = [in readUInt32];
		_tables = [[NSMutableDictionary alloc] init];
		int tableCount = [in readUInt32];
		for (int n = 0; n < tableCount ; n++) {
			NSString *name = [in readUTF];
			_tables[name] = [[DbTable alloc] init:name io:io];
		}
		_indexs = [[NSMutableDictionary alloc] init];
		int indexCount = [in readUInt32];
		for (int n = 0; n < indexCount ; n++) {
			NSString *name = [in readUTF];
			_indexs[name] = [[DbIndex alloc] init:name io:io];
		}
		uint16_t eof = [in readUInt8];
		if (eof != 0xFF) return nil;
	} else {
		_tables = [[NSMutableDictionary alloc] init];
		_indexs = [[NSMutableDictionary alloc] init];
	}
	return self;
}

- (DbTable*)createTable:(NSString*)name slot:(int)slot
{
	DbTable *table = [[DbTable alloc] init:name io:_io slot:slot];
	if (!table) return nil;
	_tables[name] = table;
	return table;
}

- (DbTable*)openTable:(NSString*)name
{
	return _tables[name];
}

- (DbIndex*)createIndex:(NSString*)name slot:(int)slot
{
	DbIndex *index = [[DbIndex alloc] init:name io:_io slot:slot];
	if (!index) return nil;
	_indexs[name] = index;
	return index;
}

- (DbIndex*)openIndex:(NSString*)name
{
	return _indexs[name];
}

- (void)drop
{
	[_io remove:@"database.db"];
	[_tables enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *stop) {
		DbTable *table = obj;
		[table drop];
	}];
	[_indexs enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *stop) {
		DbIndex *index = obj;
		[index drop];
	}];
}

- (void)commit
{
	[_tables enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *stop) {
		DbTable *table = obj;
		[table commit];
	}];
	[_indexs enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *stop) {
		DbIndex *index = obj;
		[index commit];
	}];
    {
        DbOutput *out = [[DbOutput alloc] init];
        [out writeUInt16:0xDBFF];
        [out writeUInt32:_version];
        [out writeUInt32:(uint32_t)_tables.count];
        [_tables enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *stop) {
            DbTable *table = obj;
            [out writeUTF:table.name];
        }];
		[out writeUInt32:(uint32_t)_indexs.count];
        [_indexs enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *stop) {
            DbIndex *index = obj;
            [out writeUTF:index.table.name];
        }];
        [out writeUInt8:0xFF];
        [_io write:[NSString stringWithFormat:@"database.db"] data:out];
    }
	[_io commit];
}

- (void)rollback
{
	[_tables enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *stop) {
		DbTable *table = obj;
		[table rollback];
	}];
	[_indexs enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *stop) {
		DbIndex *index = obj;
		[index rollback];
	}];
	[_io rollback];
}

@end
