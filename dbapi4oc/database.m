//
//  main.c
//  dbapi4oc
//
//  Created by Bernardo Breder on 18/06/14.
//  Copyright (c) 2014 Bernardo Breder. All rights reserved.
//

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

#if __OBJC2__
#import <Foundation/Foundation.h>
#import <CommonCrypto/CommonDigest.h>
#import <CommonCrypto/CommonCryptor.h>
#import <CommonCrypto/CommonHMAC.h>
#import "NSData+CommonCrypto.h"
#endif

//#undef __OBJC2__

struct db_table_data_t {
	uint8_t loaded;
	uint8_t changed;
	size_t size;
	uint8_t* data;
};

struct db_table_t {
	char* name;
	uint16_t nameLength;
	struct db_table_data_t* array;
	uint32_t size;
	uint32_t max;
	uint32_t page;
	uint32_t slot;
	uint8_t data_changed;
    uint8_t structure_changed;
};

struct db_bytes_t {
    uint8_t* bytes;
    uint8_t* next;
    size_t size;
    size_t max;
};

void db_bit_write_uint32(uint8_t* bytes, uint32_t value) {
	*bytes++ = (value >> 24);
	*bytes++ = (value >> 16);
	*bytes++ = (value >> 8);
	*bytes++ = value;
}

void db_bit_write_int32(uint8_t* bytes, int32_t value) {
	if (value < 0) {
		value = -value;
		*bytes++ = (value >> 24) + 0x80;
		*bytes++ = value >> 16;
		*bytes++ = value >> 8;
		*bytes++ = value;
	} else {
		*bytes++ = value >> 24;
		*bytes++ = value >> 16;
		*bytes++ = value >> 8;
		*bytes++ = value;
		
	}
}

void db_bit_write_uint16(uint8_t* bytes, uint16_t value) {
	*bytes++ = (value >> 8);
	*bytes++ = value;
}

void db_bit_write_int16(uint8_t* bytes, int16_t value) {
	*bytes++ = value < 0 ? (-value >> 8) + 0x80 : (value >> 8);
	*bytes++ = value;
}

void db_bit_write_uint8(uint8_t* bytes, uint8_t value) {
	*bytes++ = value;
}

void db_bit_write_int8(uint8_t* bytes, int8_t value) {
	*bytes++ = value < 0 ? -value + 0x80 : value;
}

struct db_bytes_t* db_bytes_create(size_t capacity) {
    struct db_bytes_t* self = (struct db_bytes_t*)malloc(sizeof(struct db_bytes_t));
    if (!self) return 0;
    self->bytes = (uint8_t*)malloc(capacity);
    if (!self->bytes) { free(self); return 0; }
    self->next = self->bytes;
    self->max = capacity;
    self->size = 0;
    return self;
}

void db_bytes_free(struct db_bytes_t* self) {
    free(self->bytes);
    free(self);
}

void db_bytes_reset(struct db_bytes_t* self) {
    self->size = 0;
}

uint8_t* db_bytes_result(struct db_bytes_t* self) {
    return self->bytes;
}

size_t db_bytes_size(struct db_bytes_t* self) {
    return self->size;
}

uint8_t db_bytes_write_bytes(struct db_bytes_t* self, const uint8_t* bytes, size_t length) {
    if (self->size + length > self->max) {
        self->max = self->max * 2 + length;
        uint8_t* bytes = (uint8_t*)realloc(self->bytes, self->max);
        if (!bytes) return 1;
        self->bytes = bytes;
        self->next = bytes + self->size;
    }
    memcpy(self->next, bytes, length);
    self->next += length;
    self->size += length;
    return 0;
}

uint8_t db_bytes_write_utf8(struct db_bytes_t* self, const wchar_t* text, uint16_t length) {
    size_t len = 0;
	int32_t n;
    for (n = 0; n < length ; n++) {
		wchar_t c = text[n] & 0xFF;
		if (c <= 0x7F) {
			len++;
		} else if ((c >> 5) == 0x6) {
			len += 2;
		} else {
			len += 3;
		}
	}
    if (self->size + len + 2 > self->max) {
        self->max = self->max * 2 + len;
        uint8_t* bytes = (uint8_t*)realloc(self->bytes, self->max);
        if (!bytes) return 1;
        self->bytes = bytes;
        self->next = bytes + self->size;
    }
    *self->next++ = (length >> 8);
	*self->next++ = (length >> 0);
	for (n = 0; n < length ; n++) {
		wchar_t c = text[n] & 0xFF;
		if (c <= 0x7F) {
			*self->next++ = c ;
		} else if ((c >> 5) == 0x6) {
			*self->next++ = ((c >> 6) & 0x1F) + 0xC0;
			*self->next++ = (c & 0x3F) + 0x80;
		} else {
			*self->next++ = ((c >> 12) & 0xF) + 0xE0;
			*self->next++ = ((c >> 6) & 0x3F) + 0x80;
			*self->next++ = (c & 0x3F) + 0x80;
		}
	}
	self->size += 2 + len;
	return 0;
}

uint8_t db_bytes_write_ascii(struct db_bytes_t* self, const char* text, uint16_t length) {
    if (self->size + length + 2 > self->max) {
        self->max = self->max * 2 + length;
        uint8_t* bytes = (uint8_t*)realloc(self->bytes, self->max);
        if (!bytes) return 1;
        self->bytes = bytes;
        self->next = bytes + self->size;
    }
    *self->next++ = (length >> 8);
	*self->next++ = (length >> 0);
	memcpy(self->next, text, length);
	self->next += length;
	self->size += 2 + length;
	return 0;
}

uint8_t db_bytes_write_uint64(struct db_bytes_t* self, uint64_t value) {
    if (self->size + 8 > self->max) {
        self->max = self->max * 2 + 8;
        uint8_t* bytes = (uint8_t*)realloc(self->bytes, self->max);
        if (!bytes) return 1;
        self->bytes = bytes;
        self->next = bytes + self->size;
    }
	*self->next++ = (value >> 56);
	*self->next++ = (value >> 48);
	*self->next++ = (value >> 40);
	*self->next++ = (value >> 32);
	*self->next++ = (value >> 24);
	*self->next++ = (value >> 16);
	*self->next++ = (value >> 8);
	*self->next++ = (value >> 0);
    self->size += 8;
	return 0;
}

uint8_t db_bytes_write_uint32(struct db_bytes_t* self, uint32_t value) {
    if (self->size + 4 > self->max) {
        self->max = self->max * 2 + 4;
        uint8_t* bytes = (uint8_t*)realloc(self->bytes, self->max);
        if (!bytes) return 1;
        self->bytes = bytes;
        self->next = bytes + self->size;
    }
	*self->next++ = (value >> 24);
	*self->next++ = (value >> 16);
	*self->next++ = (value >> 8);
	*self->next++ = (value >> 0);
    self->size += 4;
	return 0;
}

uint8_t db_bytes_write_uint16(struct db_bytes_t* self, uint16_t value) {
    if (self->size + 2 > self->max) {
        self->max = self->max * 2 + 2;
        uint8_t* bytes = (uint8_t*)realloc(self->bytes, self->max);
        if (!bytes) return 1;
        self->bytes = bytes;
        self->next = bytes + self->size;
    }
	*self->next++ = (value >> 8);
	*self->next++ = (value >> 0);
    self->size += 2;
	return 0;
}

uint8_t db_bytes_write_uint8(struct db_bytes_t* self, uint8_t value) {
    if (self->size + 1 > self->max) {
        self->max = self->max * 2 + 1;
        uint8_t* bytes = (uint8_t*)realloc(self->bytes, self->max);
        if (!bytes) return 1;
        self->bytes = bytes;
        self->next = bytes + self->size;
    }
	*self->next++ = value;
    self->size++;
	return 1;
}

uint8_t db_bytes_write_uint32_compressed(struct db_bytes_t* self, uint32_t value) {
    if (self->size + 5 > self->max) {
        self->max = self->max * 2 + 5;
        uint8_t* bytes = (uint8_t*)realloc(self->bytes, self->max);
        if (!bytes) return 1;
        self->bytes = bytes;
        self->next = bytes + self->size;
    }
	if (value <= 0x7F) {
        *self->next++ = value;
        self->size++;
		return 0;
	} else if (value <= 0x3FFF) {
        *self->next++ = (value & 0x7F) + 0x80;
        *self->next++ = value >> 7;
        self->size += 2;
        return 0;
	} else if (value <= 0x1FFFFF) {
        *self->next++ = (value & 0x7F) + 0x80;
        *self->next++ = (value >> 7 & 0x7F) + 0x80;
        *self->next++ = value >> 14;
        self->size += 3;
        return 0;
	} else if (value <= 0xFFFFFFF) {
        *self->next++ = (value & 0x7F) + 0x80;
        *self->next++ = (value >> 7 & 0x7F) + 0x80;
        *self->next++ = (value >> 14 & 0x7F) + 0x80;
        *self->next++ = value >> 21;
        self->size += 4;
        return 0;
	} else {
        *self->next++ = (value & 0x7F) + 0x80;
        *self->next++ = (value >> 7 & 0x7F) + 0x80;
        *self->next++ = (value >> 14 & 0x7F) + 0x80;
        *self->next++ = (value >> 21 & 0x7F) + 0x80;
        *self->next++ = value >> 28 & 0xF;
        self->size += 5;
        return 0;
	}
}

uint8_t db_bytes_write_uint64_compressed(struct db_bytes_t* self, uint64_t value) {
    if (self->size + 9 > self->max) {
        self->max = self->max * 2 + 9;
        uint8_t* bytes = (uint8_t*)realloc(self->bytes, self->max);
        if (!bytes) return 1;
        self->bytes = bytes;
        self->next = bytes + self->size;
    }
	if (value <= 0x7F) {
        *self->next++ = value;
        self->size++;
		return 0;
	} else if (value <= 0x3FFF) {
        *self->next++ = (value & 0x7F) + 0x80;
        *self->next++ = value >> 7;
        self->size += 2;
        return 0;
	} else if (value <= 0x1FFFFF) {
        *self->next++ = (value & 0x7F) + 0x80;
        *self->next++ = (value >> 7 & 0x7F) + 0x80;
        *self->next++ = value >> 14;
        self->size += 3;
        return 0;
	} else if (value <= 0xFFFFFFF) {
        *self->next++ = (value & 0x7F) + 0x80;
        *self->next++ = (value >> 7 & 0x7F) + 0x80;
        *self->next++ = (value >> 14 & 0x7F) + 0x80;
        *self->next++ = value >> 21;
        self->size += 4;
        return 0;
	} else if (value <= 0x7FFFFFFFF) {
        *self->next++ = (value & 0x7F) + 0x80;
        *self->next++ = (value >> 7 & 0x7F) + 0x80;
        *self->next++ = (value >> 14 & 0x7F) + 0x80;
        *self->next++ = (value >> 21 & 0x7F) + 0x80;
        *self->next++ = value >> 28;
        self->size += 5;
        return 0;
	} else if (value <= 0x3FFFFFFFFFF) {
        *self->next++ = (value & 0x7F) + 0x80;
        *self->next++ = (value >> 7 & 0x7F) + 0x80;
        *self->next++ = (value >> 14 & 0x7F) + 0x80;
        *self->next++ = (value >> 21 & 0x7F) + 0x80;
        *self->next++ = (value >> 28 & 0x7F) + 0x80;
        *self->next++ = value >> 35;
        self->size += 6;
        return 0;
	} else if (value <= 0x1FFFFFFFFFFFF) {
        *self->next++ = (value & 0x7F) + 0x80;
        *self->next++ = (value >> 7 & 0x7F) + 0x80;
        *self->next++ = (value >> 14 & 0x7F) + 0x80;
        *self->next++ = (value >> 21 & 0x7F) + 0x80;
        *self->next++ = (value >> 28 & 0x7F) + 0x80;
        *self->next++ = (value >> 35 & 0x7F) + 0x80;
        *self->next++ = value >> 42;
        self->size += 7;
        return 0;
	} else if (value <= 0xFFFFFFFFFFFFFF) {
        *self->next++ = (value & 0x7F) + 0x80;
        *self->next++ = (value >> 7 & 0x7F) + 0x80;
        *self->next++ = (value >> 14 & 0x7F) + 0x80;
        *self->next++ = (value >> 21 & 0x7F) + 0x80;
        *self->next++ = (value >> 28 & 0x7F) + 0x80;
        *self->next++ = (value >> 35 & 0x7F) + 0x80;
        *self->next++ = (value >> 42 & 0x7F) + 0x80;
        *self->next++ = value >> 49;
        self->size += 8;
        return 0;
	} else {
        *self->next++ = (value & 0x7F) + 0x80;
        *self->next++ = (value >> 7 & 0x7F) + 0x80;
        *self->next++ = (value >> 14 & 0x7F) + 0x80;
        *self->next++ = (value >> 21 & 0x7F) + 0x80;
        *self->next++ = (value >> 28 & 0x7F) + 0x80;
        *self->next++ = (value >> 35 & 0x7F) + 0x80;
        *self->next++ = (value >> 42 & 0x7F) + 0x80;
        *self->next++ = (value >> 49 & 0x7F) + 0x80;
        *self->next++ = value >> 56 & 0xFF;
        self->size += 9;
        return 0;
	}
}

uint32_t db_bytes_read_uint32_compressed(uint8_t** bytes) {
	uint8_t i1 = *(*bytes)++;
	if (i1 < 0x80) {
		return i1;
	}
    i1 -= 0x80;
    uint8_t i2 = *(*bytes)++;
	if (i2 < 0x80) {
		return (i2 << 7) + i1;
	}
    i2 -= 0x80;
    uint8_t i3 = *(*bytes)++;
	if (i3 < 0x80) {
		return (i3 << 14) + (i2 << 7) + i1;
	}
    i3 -= 0x80;
    uint8_t i4 = *(*bytes)++;
	if (i4 < 0x80) {
		return (i4 << 21) + (i3 << 14) + (i2 << 7) + i1;
	}
    i4 -= 0x80;
    uint8_t i5 = *(*bytes)++;
    return (i5 << 28) + (i4 << 21) + (i3 << 14) + (i2 << 7) + i1;
}

uint64_t db_bytes_read_uint64_compressed(uint8_t** bytes) {
	uint8_t i1 = *(*bytes)++;
	if (i1 < 0x80) {
		return i1;
	}
    i1 -= 0x80;
    uint8_t i2 = *(*bytes)++;
	if (i2 < 0x80) {
		return (i2 << 7) + i1;
	}
    i2 -= 0x80;
    uint8_t i3 = *(*bytes)++;
	if (i3 < 0x80) {
		return (i3 << 14) + (i2 << 7) + i1;
	}
    i3 -= 0x80;
    uint8_t i4 = *(*bytes)++;
	if (i4 < 0x80) {
		return (i4 << 21) + (i3 << 14) + (i2 << 7) + i1;
	}
    i4 -= 0x80;
    uint64_t i5 = *(*bytes)++;
	if (i5 < 0x80) {
		return (i5 << 28) + (i4 << 21) + (i3 << 14) + (i2 << 7) + i1;
	}
    i5 -= 0x80;
    uint64_t i6 = *(*bytes)++;
	if (i6 < 0x80) {
		return (i6 << 35) + (i5 << 28) + (i4 << 21) + (i3 << 14) + (i2 << 7) + i1;
	}
    i6 -= 0x80;
    uint64_t i7 = *(*bytes)++;
	if (i7 < 0x80) {
		return (i7 << 42) + (i6 << 35) + (i5 << 28) + (i4 << 21) + (i3 << 14) + (i2 << 7) + i1;
	}
    i7 -= 0x80;
    uint64_t i8 = *(*bytes)++;
	if (i8 < 0x80) {
		return (i8 << 49) + (i7 << 42) + (i6 << 35) + (i5 << 28) + (i4 << 21) + (i3 << 14) + (i2 << 7) + i1;
	}
    i8 -= 0x80;
    uint64_t i9 = *(*bytes)++;
	return (i9 << 56) + (i8 << 49) + (i7 << 42) + (i6 << 35) + (i5 << 28) + (i4 << 21) + (i3 << 14) + (i2 << 7) + i1;
}

uint64_t db_bytes_read_uint64(uint8_t* bytes) {
	return ((uint64_t)bytes[0] << 56) + ((uint64_t)bytes[1] << 48) + ((uint64_t)bytes[2] << 40) + ((uint64_t)bytes[3] << 32) + (bytes[4] << 24) + (bytes[5] << 16) + (bytes[6] << 8) + bytes[7];
}

uint32_t db_bytes_read_uint32(uint8_t* bytes) {
	return (bytes[0] << 24) + (bytes[1] << 16) + (bytes[2] << 8) + bytes[3];
}

int32_t db_bytes_read_int32(uint8_t* bytes) {
	return ((bytes[0] & 0x80) == 0x80) ? -(((bytes[0] - 0x80) << 24) + (bytes[1] << 16) + (bytes[2] << 8) + bytes[3]) : ((bytes[0] << 24) + (bytes[1] << 16) + (bytes[2] << 8) + bytes[3]);
}

uint16_t db_bytes_read_uint16(uint8_t* bytes) {
	return (bytes[0] << 8) + bytes[1];
}

uint8_t db_bytes_read_uint8(uint8_t* bytes) {
	return *bytes;
}

wchar_t* db_bytes_read_utf8(uint8_t* bytes, uint16_t* length) {
	uint32_t len = (bytes[0] << 8) + bytes[1];
	bytes += 2;
	if (length) {
		*length = len;
	}
	wchar_t* data = (wchar_t*)malloc(len + 1);
	if (!data) {
		return 0;
	}
	uint32_t n, m;
	for (n = 0, m = 0; n < len ; n++) {
		int c = *bytes++;
		if (c <= 0x7F) {
			data[m++] = c;
		} else if ((c >> 5) == 0x6) {
			char i2 = *bytes++;
			data[m++] = ((c & 0x1F) << 6) + (i2 & 0x3F);
		} else {
			char i2 = *bytes++;
			char i3 = *bytes++;
			data[m++] = ((c & 0xF) << 12) + ((i2 & 0x3F) << 6) + (i3 & 0x3F);
		}
	}
	return data;
}

char* db_bytes_read_ascii(uint8_t** bytes, uint16_t* length) {
	uint8_t* aux = *bytes;
	uint32_t len = (aux[0] << 8) + aux[1];
	if (len >= 0xFFFF) {
		return 0;
	}
	aux += 2;
	if (length) {
		*length = len;
	}
	char* data = (char*)malloc(len + 1);
	if (!data) {
		return 0;
	}
	memcpy(data, aux, len);
	data[len] = 0;
	*bytes += len + 2;
	return data;
}

uint8_t* db_file_read_bytes_range(FILE* file, size_t offset, size_t length) {
    if (!file) return 0;
    if (fseek(file, offset, SEEK_SET)) return 0;
    uint8_t* bytes = malloc(length);
    if (!bytes) return 0;
    if (fread(bytes, 1, length, file) != length) { free(bytes); return 0; }
    return bytes;
}

uint8_t db_file_write_bytes_range(FILE* file, size_t offset, uint8_t* bytes, size_t length) {
    if (!file) return 1;
    if (fseek(file, offset, SEEK_SET)) return 1;
    if (fwrite(bytes, 1, length, file) != length) return 1;
    return 0;
}

uint8_t* db_file_read_bytes(const char* filename, size_t* length) {
#ifdef __OBJC2__
    NSString* password = @"Bernardo Breder";
    //    NSData *passwordData = [password dataUsingEncoding:NSUTF8StringEncoding];
    //    unsigned char hash[CC_SHA256_DIGEST_LENGTH];
    //	(void) CC_SHA256( [passwordData bytes], (CC_LONG)[passwordData length], hash);
    //	NSData *passwordHashData = [NSData dataWithBytes:hash length:CC_SHA256_DIGEST_LENGTH];
    //    data = [data decryptedDataUsingAlgorithm: kCCAlgorithmAES128 key: passwordHashData options: kCCOptionPKCS7Padding error: nil];
    //    data = [data decryptedAES256DataUsingKey:passwordHashData error:nil];
    NSString* path = [[NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES) objectAtIndex:0] stringByAppendingPathComponent:[NSString stringWithUTF8String:filename]];
    NSData* data = [NSData dataWithContentsOfFile:path];
    if (!data) return 0;
	data = [data decryptedDataUsingAlgorithm:kCCAlgorithmAES128 key:[[password dataUsingEncoding:NSUTF8StringEncoding] MD5Sum] error:nil];
//    data = [data decryptedAES256DataUsingKey:[[password dataUsingEncoding:NSUTF8StringEncoding] SHA256Hash] error:nil];
    if (!data) return 0;
    uint8_t* bytes = (uint8_t*)malloc(data.length);
    if (!bytes) return 0;
	[data getBytes:bytes length:data.length];
    return bytes;
#else
	FILE *file = fopen(filename, "rb");
	if (!file) {
		return 0;
	}
	fseek(file, 0, SEEK_END);
	size_t bytes_len = ftell(file);
	if (length) {
		*length = bytes_len;
	}
	fseek(file, 0, SEEK_SET);
	uint8_t* bytes = (uint8_t*)malloc(bytes_len);
	if (fread(bytes, 1, bytes_len, file) != bytes_len) {
		free(bytes);
		fclose(file);
		return 0;
	}
	fclose(file);
	return bytes;
#endif
}

uint8_t db_file_write_bytes(const char* filename, uint8_t* bytes, size_t length) {
#ifdef __OBJC2__
    NSData* data = [NSData dataWithBytes:bytes length:length];
    if (!data) {
        return 1;
    }
    NSString* password = @"Bernardo Breder";
    data = [data dataEncryptedUsingAlgorithm: kCCAlgorithmAES128 key: [[password dataUsingEncoding:NSUTF8StringEncoding] MD5Sum] options: kCCOptionPKCS7Padding error: nil];
    NSString* path = [[NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES) objectAtIndex:0] stringByAppendingPathComponent:[NSString stringWithUTF8String:filename]];
    if (![data writeToFile:path atomically:YES]) {
        return 1;
    }
#else
	FILE *file = fopen(filename, "wb");
	if (!file) {
		return 1;
	}
    if (fwrite(bytes, 1, length, file) != length) {
		fclose(file);
		return 1;
	}
	fclose(file);
#endif
	return 0;
}

uint8_t db_file_remove(const char* filename) {
#ifdef __OBJC2__
    NSString* path = [[NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES) objectAtIndex:0] stringByAppendingPathComponent:[NSString stringWithUTF8String:filename]];
    return [[NSFileManager defaultManager] removeItemAtPath:path error:nil] == NO;
#else
	return remove(filename) != 0;
#endif
}

uint8_t db_file_exist(const char* filename) {
#ifdef __OBJC2__
    NSString* path = [[NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES) objectAtIndex:0] stringByAppendingPathComponent:[NSString stringWithUTF8String:filename]];
    return [[NSFileManager defaultManager] fileExistsAtPath:path isDirectory:false] == YES;
#else
	FILE *file = fopen(filename, "rb");
	fclose(file);
	return file != 0;
#endif
}

struct db_fileset_entry_t {
    uint8_t loaded;
    size_t offset;
    size_t max, size;
    uint8_t* bytes;
    size_t nextId;
};

struct db_fileset_t {
    char* name;
    size_t name_length;
    size_t max, size;
    FILE *rfile, *wfile;
};

struct db_fileset_t* db_fileset_create(const char* name) {
    struct db_fileset_t* self = (struct db_fileset_t*)malloc(sizeof(struct db_fileset_t));
    if (!self) return 0;
    if(!(self->name = strdup(name))) { free(self); return 0; }
    self->name_length = strlen(name);
    self->size = 0;
    if (!(self->wfile = fopen(name, "wb"))) { free(self->name); free(self); return 0; }
    if (!(self->rfile = fopen(name, "rb"))) { fclose(self->wfile); free(self->name); free(self); return 0; }
    return self;
}

struct db_fileset_t* db_fileset_open(const char* name) {
    struct db_fileset_t* self = (struct db_fileset_t*)malloc(sizeof(struct db_fileset_t));
    if (!self) return 0;
    if(!(self->name = strdup(name))) { free(self); return 0; }
    self->name_length = strlen(name);
    if (!(self->rfile = fopen(name, "rb"))) { free(self->name); free(self); return 0; }
    if (!(self->wfile = fopen(name, "ab"))) { fclose(self->rfile); free(self->name); free(self); return 0; }
	fseek(self->rfile, 0, SEEK_END);
	size_t bytes_len = ftell(self->rfile);
	fseek(self->rfile, 0, SEEK_SET);
    self->size = (uint32_t)ceil((double)bytes_len / 0xFFFF);
	self->max = self->size + (self->size >> 1);
    return self;
}

void db_fileset_free(struct db_fileset_t* self) {
    if (self->rfile) {
        fclose(self->rfile);
		self->rfile = 0;
    }
    if (self->wfile) {
        fclose(self->wfile);
		self->wfile = 0;
    }
    free(self->name);
    free(self);
}

uint8_t db_fileset_drop(struct db_fileset_t* self) {
    fclose(self->rfile);
    self->rfile = 0;
    fclose(self->wfile);
    self->wfile = 0;
    if (db_file_remove(self->name)) return 1;
    return 0;
}

size_t db_fileset_size(struct db_fileset_t* self) {
    return self->size;
}

uint8_t* db_fileset_get(struct db_fileset_t* self, size_t index) {
	if (index > self->size) {
		return 0;
	}
	int32_t next;
	uint8_t header[sizeof(uint8_t) + sizeof(uint16_t) + sizeof(int32_t)];
	uint8_t* bytes;
	size_t length;
	{
		if (fseek(self->rfile, index * 0xFFFF, SEEK_SET)) return 0;
		if (fread(header, 1, sizeof(header), self->rfile) != sizeof(header)) return 0;
		uint16_t size = db_bytes_read_uint16(header + 1);
		next = db_bytes_read_int32(header + 3);
		if (!(bytes = malloc(size))) return 0;
		if (fread(bytes, 1, size, self->rfile) != size) { free(bytes); return 0; }
		length = size;
	}
	while (next > 0) {
		if (fseek(self->rfile, next * 0xFFFF, SEEK_SET)) { free(bytes); return 0; }
		if (fread(header, 1, sizeof(header), self->rfile) != sizeof(header)) { free(bytes); return 0; }
		uint16_t size = db_bytes_read_uint16(header + 1);
		next = db_bytes_read_int32(header + 3);
		uint8_t* aux = (uint8_t*)realloc(bytes, length + size);
		if (!aux) { free(bytes); return 0; }
		bytes = aux;
		if (fread(bytes + length, 1, size, self->rfile) != size) { free(bytes); return 0; }
		length += size;
	}
    return bytes;
}

uint8_t db_fileset_set(struct db_fileset_t* self, size_t index, uint8_t* bytes, size_t size) {
	uint8_t header[sizeof(uint8_t) + sizeof(uint16_t) + sizeof(int32_t)];
	do {
		uint16_t delta = size > 0xFFF7 ? 0xFFF7 : size;
		if (fseek(self->rfile, index * 0xFFFF, SEEK_SET)) { return -1; }
		if (fread(header, 1, sizeof(header), self->rfile) != sizeof(header)) return -1;
		int32_t nextIndex = db_bytes_read_int32(header + 3);
		db_bit_write_uint8(header, 1);
		db_bit_write_uint16(header + 1, delta);
		if (delta == size) {
			db_bit_write_int32(header + 3, -1);
		} else {
			if (nextIndex < 0) {
				index = self->size++;
			} else {
				index = nextIndex;
			}
			db_bit_write_int32(header + 3, (int32_t)index);
		}
		if (fseek(self->wfile, index * 0xFFFF, SEEK_SET)) { return -1; }
		if (fwrite(header, 1, sizeof(header), self->wfile) != sizeof(header)) { return -1; }
		if (fwrite(bytes, 1, delta, self->wfile) != delta) { return -1; }
		bytes += delta;
		size -= delta;
	} while (size > 0);
    if (fflush(self->wfile)) return -1;
	return 0;
}

ssize_t db_fileset_add(struct db_fileset_t* self, uint8_t* bytes, size_t size) {
	uint8_t header[sizeof(uint8_t) + sizeof(uint16_t) + sizeof(int32_t)];
	size_t index = self->size, result = self->size;
	int32_t next;
	do {
		size_t delta = size > 0xFFF7 ? 0xFFF7 : size;
		db_bit_write_uint8(header, 1);
		db_bit_write_uint16(header + 1, delta);
		if (delta == size) {
			next = -1;
            self->size++;
		} else {
			next = (int32_t)++self->size;
		}
		db_bit_write_int32(header + 3, (int32_t)next);
		if (fseek(self->wfile, index * 0xFFFF, SEEK_SET)) { return -1; }
		if (fwrite(header, 1, sizeof(header), self->wfile) != sizeof(header)) { return -1; }
		if (fwrite(bytes, 1, delta, self->wfile) != delta) { return -1; }
		bytes += delta;
		size -= delta;
		index = next;
	} while (size > 0);
    if (fflush(self->wfile)) return -1;
    return result;
}

struct db_table_t* db_table_create(const char* name, int32_t page, int32_t slot) {
	int16_t length = strlen(name);
	char filename[length + 4];
	sprintf(filename, "%s.db", name);
	if (db_file_exist(filename)) {
		return 0;
	}
	int32_t max = page * slot;
	struct db_table_t* self = (struct db_table_t*)calloc(1, sizeof(struct db_table_t));
	if (!self) {
		return 0;
	}
	if (!(self->array = (struct db_table_data_t*)calloc(max, sizeof(struct db_table_data_t)))) {
		free(self);
		return 0;
	}
	if (!(self->name = strdup(name))) {
		free(self->array);
		free(self);
		return 0;
	}
	self->nameLength = length;
	self->page = page;
	self->slot = slot;
	self->max = max;
	self->size = 0;
    self->structure_changed = 1;
	return self;
}

struct db_table_t* db_table_open(const char* name) {
	int16_t length = strlen(name);
	char filename[length + 4];
	sprintf(filename, "%s.db", name);
	uint8_t* bytes = db_file_read_bytes(filename, 0), *aux = bytes;
	if (!bytes) {
		return 0;
	}
	int32_t size = db_bytes_read_uint32(aux);
	aux += 4;
	int32_t page = db_bytes_read_uint32(aux);
	aux += 4;
	int32_t slot = db_bytes_read_uint32(aux);
	free(bytes);
	int32_t max = page * slot;
    if (size > max) max = size + (size >> 1);
	struct db_table_t* self = (struct db_table_t*)calloc(1, sizeof(struct db_table_t));
	if (!self) {
		return 0;
	}
	self->array = (struct db_table_data_t*)calloc(max, sizeof(struct db_table_data_t));
	if (!self->array) {
		free(self);
		return 0;
	}
	if (!(self->name = strdup(name))) {
		free(self->array);
		free(self);
		return 0;
	}
	self->nameLength = length;
	self->page = page;
	self->slot = slot;
	self->max = max;
	self->size = size;
	return self;
}

void db_table_free(struct db_table_t* self) {
	uint32_t n;
	for (n = 0 ; n < self->size ; n++) {
		struct db_table_data_t* item = &self->array[n];
		if (item->data) {
			free(item->data);
		}
	}
	free(self->name);
	free(self->array);
	free(self);
}

struct db_table_data_t* db_table_get_entry(struct db_table_t* self, uint32_t key, uint8_t maskChanged) {
	if (key == 0 || key > self->size) {
		return 0;
	}
	uint32_t pageIndex = --key % self->page;
	if (!self->array[pageIndex].loaded) {
		char filename[strlen(self->name) + 12 + 5];
		sprintf(filename, "%s_%d.db", self->name, pageIndex);
		uint8_t* bytes = db_file_read_bytes(filename, 0), *aux = bytes;
		if (!bytes) return 0;
		uint32_t m, count = db_bytes_read_uint32(aux);
		aux += 4;
		uint32_t n, page = self->page;
		for (n = 0 ; n < count ; n++) {
			uint32_t size = db_bytes_read_uint32(aux);
			aux += 4;
            uint8_t* data = 0;
            if (size > 0) {
                data = (uint8_t*)malloc(size);
                if (!data) {
                    free(bytes);
                    for (m = 0 ; m < n ; m++) {
                        free(self->array[n * page + pageIndex].data);
                    }
                    return 0;
                }
                memcpy(data, aux, size);
                aux += size;
            }
			struct db_table_data_t* item = &self->array[n * page + pageIndex];
			item->data = data;
			item->size = size;
			item->changed = 0;
			item->loaded = 1;
		}
		free(bytes);
	}
	if (maskChanged) {
		self->array[pageIndex].changed = 1;
	}
	return &self->array[key];
}

uint8_t db_table_put(struct db_table_t* self, uint32_t id, uint8_t* data, size_t size) {
    if (id > self->max) {
        uint32_t old_max = self->max;
        self->max = (self->max * 2 > id) ? (self->max * 2) : (id + (id >> 1));
        while (self->slot * self->page < self->max) {
            self->page++;
        }
		struct db_table_data_t* table = realloc(self->array, self->max * sizeof(struct db_table_data_t));
		if (!table) return 1;
        memset(table + old_max, 0, (self->max - old_max) * sizeof(struct db_table_data_t));
		self->array = table;
        self->structure_changed = 1;
    }
	uint8_t* bytes = (uint8_t*)malloc(size);
	if (!bytes) return 1;
	memcpy(bytes, data, size);
    if (!self->array[(id-1) % self->page].loaded) {
        db_table_get_entry(self, id, 0);
    }
    if (id > self->size) self->size = id;
	struct db_table_data_t* item = &self->array[id-1];
    if (item->loaded && item->data) {
        free(item->data);
    }
	item->data = bytes;
	item->size = size;
	item->changed = 1;
	item->loaded = 0;
	self->data_changed = 1;
	return 0;
}

int32_t db_table_add(struct db_table_t* self, uint8_t* data, size_t size) {
	if (self->size >= self->max) {
		self->max *= 2;
		self->page *= 2;
		struct db_table_data_t* table = realloc(self->array, self->max * sizeof(struct db_table_data_t));
		if (!table) {
			return -1;
		}
		self->array = table;
        self->structure_changed = 1;
	}
	uint8_t* bytes = (uint8_t*)malloc(size);
	if (!bytes) {
		return -0xFF;
	}
	memcpy(bytes, data, size);
	int32_t result = self->size++;
    if (!self->array[result % self->page].loaded) {
        db_table_get_entry(self, result + 1, 0);
    }
	struct db_table_data_t* item = &self->array[result];
	item->data = bytes;
	item->size = size;
	item->changed = 1;
	self->data_changed = 1;
	return result + 1;
}

uint32_t db_table_size(struct db_table_t* self) {
    return self->size;
}

uint8_t* db_table_get(struct db_table_t* self, uint32_t key, size_t* length) {
	struct db_table_data_t* item = db_table_get_entry(self, key, 0);
	if (!item) {
		return 0;
	}
	if (length) {
		*length = item->size;
	}
	return item->data;
}

uint8_t db_table_set(struct db_table_t* self, uint32_t key, uint8_t* data, size_t size) {
	struct db_table_data_t* item = db_table_get_entry(self, key, 1);
	if (!item) {
		return 1;
	}
    uint8_t *aux = (uint8_t*)malloc(size);
    if (!aux) return 1;
    memcpy(aux, data, size);
    if (!self->array[(key-1) % self->page].loaded) {
        db_table_get_entry(self, key, 0);
    }
	if (item->loaded && item->data) {
		free(item->data);
	}
	item->data = aux;
	item->size = size;
	item->changed = 1;
    self->data_changed = 1;
	return 0;
}

uint8_t db_table_remove(struct db_table_t* self, uint32_t key) {
	struct db_table_data_t* item = db_table_get_entry(self, key, 1);
	if (!item) {
		return 1;
	}
    if (!self->array[(key-1) % self->page].loaded) {
        db_table_get_entry(self, key, 0);
    }
	if (item->loaded && item->data) {
		free(item->data);
	}
	item->size = 0;
	item->data = 0;
	item->changed = 1;
	self->data_changed = 1;
	return 0;
}

uint8_t db_table_drop(struct db_table_t* self) {
	uint32_t n;
	char filename[self->nameLength + 12 + 4];
	sprintf(filename, "%s.db", self->name);
	uint8_t result = db_file_exist(filename) && db_file_remove(filename);
	for (n = 0; n < self->page ; n++) {
		sprintf(filename, "%s_%d.db", self->name, n);
		result |= db_file_exist(filename) && db_file_remove(filename);
	}
	self->data_changed = 1;
	return result;
}

uint8_t db_table_commit(struct db_table_t* self) {
	if (self->data_changed) {
		uint32_t pageIndex;
		for (pageIndex = 0 ; pageIndex < self->page ; pageIndex++) {
			uint32_t slotIndex, cellIndex, page = self->page;
			uint8_t changed = 0;
			size_t length = 0;
			for (slotIndex = 0, cellIndex = pageIndex; cellIndex < self->size ; slotIndex++, cellIndex += page) {
				struct db_table_data_t* item = &self->array[cellIndex];
				length += item->size;
				changed |= item->changed;
			}
			if (changed) {
                struct db_bytes_t* bytes = db_bytes_create(4 + slotIndex * 4 + length);
                db_bytes_write_uint32(bytes, slotIndex);
				for (slotIndex = 0, cellIndex = pageIndex; slotIndex * page + pageIndex < self->size ; slotIndex++, cellIndex += page) {
					struct db_table_data_t* item = &self->array[cellIndex];
                    db_bytes_write_uint32(bytes, (uint32_t)item->size);
                    db_bytes_write_bytes(bytes, item->data, item->size);
					item->changed = 0;
				}
				char filename[strlen(self->name) + 12];
				sprintf(filename, "%s_%d.db", self->name, pageIndex);
				if (db_file_write_bytes(filename, bytes->bytes, bytes->size)) {
                    db_bytes_free(bytes);
					return 1;
				}
                db_bytes_free(bytes);
			}
		}
	}
    if (self->structure_changed) {
        struct db_bytes_t* bytes = db_bytes_create(12);
        db_bytes_write_uint32(bytes, self->size);
        db_bytes_write_uint32(bytes, self->page);
        db_bytes_write_uint32(bytes, self->slot);
        char filename[strlen(self->name) + 4];
        sprintf(filename, "%s.db", self->name);
        if (db_file_write_bytes(filename, bytes->bytes, bytes->size)) {
            db_bytes_free(bytes);
            return 1;
        }
        db_bytes_free(bytes);
    }
	return 0;
}

uint8_t db_table_rollback(struct db_table_t* self) {
	uint32_t pageIndex, cellIndex, page = self->page;
	for (pageIndex = 0; pageIndex < page ; pageIndex++) {
		if (self->array[pageIndex].loaded) {
			uint8_t changed = 0;
			for (cellIndex = pageIndex; !changed && cellIndex < self->size ; cellIndex += page) {
				changed |= self->array[cellIndex].changed;
			}
			if (changed) {
				for (cellIndex = pageIndex; cellIndex < self->size ; cellIndex += page) {
					struct db_table_data_t* item = &self->array[cellIndex];
					if (item->data) {
						free(item->data);
					}
					item->data = 0;
					item->changed = 0;
					item->loaded = 0;
				}
			}
		}
	}
	return 0;
}

struct db_index_entry_t {
	int64_t key;
	int32_t value;
	uint8_t changed;
};

struct db_index_t {
	char* name;
	uint16_t nameLength;
	struct db_index_entry_t** array;
	int32_t* sizes;
	int32_t page;
	int32_t slot;
	uint8_t changed;
};

struct db_index_t* db_index_create(const char* name, int32_t page, int32_t slot) {
	int16_t length = strlen(name);
	char filename[length + 5];
	sprintf(filename, "%s.idb", name);
	if (db_file_exist(filename)) {
		return 0;
	}
	struct db_index_t* self = (struct db_index_t*)calloc(1, sizeof(struct db_index_t));
	if (!self) {
		return 0;
	}
	self->sizes = (int32_t*)calloc(page, sizeof(int32_t));
	if (!self) {
		free(self);
		return 0;
	}
	self->array = (struct db_index_entry_t**)calloc(page, sizeof(struct db_index_entry_t*));
	if (!self->array) {
		free(self->sizes);
		free(self);
		return 0;
	}
	if (!(self->name = strdup(name))) {
		free(self->array);
		free(self->sizes);
		free(self);
		return 0;
	}
	self->nameLength = length;
	self->page = page;
	self->slot = slot;
	self->changed = 1;
	return self;
}

struct db_index_t* db_index_open(const char* name) {
	int16_t length = strlen(name);
	char filename[length + 5];
	sprintf(filename, "%s.idb", name);
	uint8_t* bytes = db_file_read_bytes(filename, 0), *aux = bytes;
	if (!bytes) {
		return 0;
	}
	int32_t page = db_bytes_read_uint32(aux);
	aux += 4;
	int32_t slot = db_bytes_read_uint32(aux);
	aux += 4;
	struct db_index_t* self = (struct db_index_t*)calloc(1, sizeof(struct db_index_t));
	if (!self) {
		free(bytes);
		return 0;
	}
	self->sizes = (int32_t*)calloc(page, sizeof(int32_t));
	if (!self) {
		free(bytes);
		free(self);
		return 0;
	}
	self->array = (struct db_index_entry_t**)calloc(page, sizeof(struct db_index_entry_t*));
	if (!self->array) {
		free(bytes);
		free(self->sizes);
		free(self);
		return 0;
	}
	if (!(self->name = strdup(name))) {
		free(bytes);
		free(self->array);
		free(self->sizes);
		free(self);
		return 0;
	}
	int32_t pageIndex;
	for (pageIndex = 0; pageIndex < page ; pageIndex++) {
		self->sizes[pageIndex] = db_bytes_read_uint32(aux);
		aux += 4;
	}
	free(bytes);
	self->nameLength = length;
	self->page = page;
	self->slot = slot;
	self->changed = 0;
	return self;
}

void db_index_free(struct db_index_t* self) {
	int32_t n;
	struct db_index_entry_t** pages = self->array;
	for (n = 0; n < self->page ; n++) {
		struct db_index_entry_t* entry = *pages++;
		if (entry) {
			free(entry);
		}
	}
	free(self->name);
	free(self->array);
	free(self->sizes);
	free(self);
}

static uint8_t db_index_resize(struct db_index_t* self, int32_t newPage) {
	struct db_index_entry_t** newArray = (struct db_index_entry_t**) calloc(newPage, sizeof(struct db_index_entry_t*));
	if (!newArray) {
		return 1;
	}
	int32_t* newSizes = (int32_t*)calloc(newPage, sizeof(int32_t));
	if (!newArray) {
		free(newArray);
		return 1;
	}
	struct db_index_entry_t** selfArray = self->array;
	int32_t* selfSizes = self->sizes;
	int32_t n, m, selfPage = self->page;
	for (n = 0; n < selfPage; n++) {
		struct db_index_entry_t* selfEntry = *selfArray++;
		int32_t selfSize = *selfSizes++;
		for (m = 0; m < selfSize ; m++) {
			int32_t index = selfEntry->key & (newPage - 1);
			if (newSizes[index] == self->slot) {
				selfArray = self->array;
				for (n = 0; n < selfPage ; n++) {
					struct db_index_entry_t* entry = *selfArray++;
					if (entry) {
						free(entry);
					}
				}
				free(newArray);
				free(newSizes);
				return db_index_resize(self, newPage + ((newPage >> 1) > 0 ? (newPage >> 1) : 1));
			}
			struct db_index_entry_t* entry = newArray[index];
			if (!entry) {
				entry = (struct db_index_entry_t*) malloc(self->slot * sizeof(struct db_index_entry_t));
				entry->changed = 1;
				newArray[index] = entry;
			}
			entry += newSizes[index]++;
			entry->key = selfEntry->key;
			entry->value = selfEntry->value;
			selfEntry++;
		}
	}
	selfArray = self->array;
	for (n = 0; n < selfPage ; n++) {
		struct db_index_entry_t* entry = *selfArray++;
		if (entry) {
			free(entry);
		}
	}
	free(self->array);
	free(self->sizes);
	self->array = newArray;
	self->sizes = newSizes;
	self->page = newPage;
	return 0;
}

static int32_t db_index_binary_search(struct db_index_entry_t* array, int32_t low, int32_t high, int64_t key) {
    if (low > high) {
        return -(low + 1);
    } else if (array[high].key == key) {
		return high;
	} else if (array[low].key == key) {
		return low;
	}else if (array[high].key < key) {
		return -(high + 2);
	} else if (array[low].key > key) {
		return -(low + 1);
	}
	while (low <= high) {
		int32_t mid = (low + high) >> 1;
        // TODO subtrair o key do array com a chave
		int64_t midVal = array[mid].key;
		if (midVal < key) {
			low = mid + 1;
		} else if (midVal > key) {
			high = mid - 1;
		} else {
			return mid;
		}
	}
	return -(low + 1);
}

static struct db_index_entry_t* db_index_load(struct db_index_t* self, int32_t pageIndex) {
	char filename[strlen(self->name) + 12 + 5];
	sprintf(filename, "%s_%d.idb", self->name, pageIndex);
	uint8_t* bytes = db_file_read_bytes(filename, 0), *auxBytes = bytes;
	if (!bytes) {
		return 0;
	}
	int32_t cellIndex, size = db_bytes_read_uint32(auxBytes);
	auxBytes += 4;
	struct db_index_entry_t* entry = (struct db_index_entry_t*)malloc(size * sizeof(struct db_index_entry_t)), *auxEntry = entry;
	if (!entry) {
		free(bytes);
		return 0;
	}
	for (cellIndex = 0; cellIndex < size; cellIndex++) {
		auxEntry->key = db_bytes_read_uint64_compressed(&auxBytes);
		auxEntry->value = db_bytes_read_uint32_compressed(&auxBytes);
		auxEntry->changed = 0;
		auxEntry++;
	}
	free(bytes);
	return entry;
}

int32_t db_index_get(struct db_index_t* self, int64_t key) {
	uint32_t index = key & (self->page - 1);
	uint32_t size = self->sizes[index];
	struct db_index_entry_t* entry = self->array[index];
	if (!entry) {
		if (size > 0) {
			self->array[index] = entry = db_index_load(self, index);
			if (!entry) {
				return 0;
			}
		} else {
			return 0;
		}
	}
	int32_t row = db_index_binary_search(entry, 0, size - 1, key);
	if (row < 0) {
		return 0;
	}
	return entry[row].value;
}

uint32_t db_index_search_range(struct db_index_t* self, uint64_t key, uint32_t offset, uint32_t limit, uint32_t* array) {
    uint32_t index = key & (self->page - 1);
	uint32_t size = self->sizes[index];
	struct db_index_entry_t* entry = self->array[index];
	if (!entry) {
		if (size > 0) {
			self->array[index] = entry = db_index_load(self, index);
			if (!entry) {
				return 0;
			}
		} else {
			return 0;
		}
	}
	int32_t row = db_index_binary_search(entry, 0, size - 1, key);
	if (row < 0) {
		return 0;
	}
    struct db_index_entry_t *aux = &entry[row];
    while (aux != entry && aux[-1].key == key) {
        aux--;
    }
    uint32_t n;
    for (n = 0 ; n < size && n < offset && aux->key == key ; n++) {
        aux++;
    }
    uint32_t result = 0;
    for (n = 0 ; n < size && n < limit && aux->key == key ; n++) {
        array[n] = aux->value;
        result++;
        aux++;
    }
	return result;
}

uint8_t db_index_add(struct db_index_t* self, int64_t key, uint32_t value) {
	int32_t index = key & (self->page - 1);
	int32_t size = self->sizes[index];
	struct db_index_entry_t* entry = self->array[index];
	if (!entry) {
		if (size > 0) {
			self->array[index] = entry = db_index_load(self, index);
			if (!entry) {
				return 0;
			}
		} else {
			self->array[index] = entry = (struct db_index_entry_t*)malloc(self->slot * sizeof(struct db_index_entry_t));
		}
	}
	int32_t row = db_index_binary_search(entry, 0, size - 1, key);
	if (row >= 0) {
		row++;
	} else {
        row = -row - 1;
    }
	if (size == self->slot) {
		do {
			if (db_index_resize(self, self->page * 2)) {
				return 1;
			}
			index = key & (self->page - 1);
			entry = self->array[index];
			size = self->sizes[index];
		} while (size == self->slot);
		row = db_index_binary_search(entry, 0, size - 1, key);
		row = -row - 1;
	}
	int32_t length = size - row;
	if (length > 0) {
		memcpy(entry + row + 1, entry + row, length * sizeof(struct db_index_entry_t));
	}
	self->sizes[index]++;
	entry->changed = 1;
	entry += row;
	entry->key = key;
	entry->value = value;
	self->changed = 1;
	return 0;
}

uint8_t db_index_remove(struct db_index_t* self, int64_t key) {
	int32_t index = key & (self->page - 1);
	int32_t size = self->sizes[index];
	struct db_index_entry_t* entry = self->array[index];
	if (!entry) {
		if (size > 0) {
			self->array[index] = entry = db_index_load(self, index);
			if (!entry) {
				return 0;
			}
		} else {
			return 0;
		}
	}
	int32_t row = db_index_binary_search(entry, 0, self->sizes[index] - 1, key);
	if (row < 0) {
		return 0;
	}
	row++;
	int32_t length = size - row - 1;
	if (length > 0) {
		memcpy(entry + row + 1, entry + row, length);
	}
	self->sizes[index]--;
	self->changed = 1;
	return 0;
}

uint8_t db_index_drop(struct db_index_t* self) {
	uint32_t n;
	char filename[strlen(self->name) + 12 + 5];
	sprintf(filename, "%s.idb", self->name);
	uint8_t result = db_file_exist(filename) && db_file_remove(filename);
	for (n = 0; n < self->page ; n++) {
		sprintf(filename, "%s_%d.idb", self->name, n);
        result |= db_file_exist(filename) && db_file_remove(filename);
	}
	self->changed = 1;
	return result;
}

uint8_t db_index_commit(struct db_index_t* self) {
	if (self->changed) {
		struct db_index_entry_t** array = self->array;
		int32_t* sizes = self->sizes;
		uint32_t pageIndex, pageSize = self->page;
		for (pageIndex = 0 ; pageIndex < pageSize ; pageIndex++) {
			struct db_index_entry_t* entry = *array++;
            if (entry) {
                int32_t size = *sizes++;
                uint8_t changed = entry->changed;
                if (changed) {
                    struct db_bytes_t* bytes = db_bytes_create(sizeof(int32_t) + (sizeof(uint64_t) + sizeof(uint32_t)) * size);
                    db_bytes_write_uint32(bytes, size);
                    int32_t slotIndex;
                    for (slotIndex = 0; slotIndex < size ; slotIndex++) {
                        db_bytes_write_uint64_compressed(bytes, entry->key);
                        db_bytes_write_uint32_compressed(bytes, entry->value);
                        entry->changed = 0;
                        entry++;
                    }
                    char filename[strlen(self->name) + 12];
                    sprintf(filename, "%s_%d.idb", self->name, pageIndex);
                    if (db_file_write_bytes(filename, bytes->bytes, bytes->size)) {
                        db_bytes_free(bytes);
                        return 1;
                    }
                    db_bytes_free(bytes);
                }
            }
		}
		{
			struct db_bytes_t* bytes = db_bytes_create(2 * sizeof(int32_t) + self->page * sizeof(int32_t));
            db_bytes_write_uint32(bytes, self->page);
            db_bytes_write_uint32(bytes, self->slot);
			int32_t* sizes = self->sizes;
			for (pageIndex = 0 ; pageIndex < pageSize ; pageIndex++) {
                db_bytes_write_uint32(bytes, *sizes++);
			}
			char filename[strlen(self->name) + 5];
			sprintf(filename, "%s.idb", self->name);
			if (db_file_write_bytes(filename, bytes->bytes, bytes->size)) {
                db_bytes_free(bytes);
				return 1;
			}
            db_bytes_free(bytes);
		}
	}
	return 0;
}

uint8_t db_index_rollback(struct db_index_t* self) {
	if (self->changed) {
		struct db_index_entry_t** array = self->array;
		uint32_t pageIndex, pageSize = self->page;
		for (pageIndex = 0 ; pageIndex < pageSize ; pageIndex++) {
			struct db_index_entry_t* entry = *array;
			if (entry->changed) {
				free(entry);
				*array = 0;
			}
			array++;
		}
	}
	return 0;
}

struct db_database_t {
	uint32_t version;
    struct db_table_t** tables;
    int32_t tableSize, tableMax;
    struct db_index_t** indexs;
    int32_t indexSize, indexMax;
};

void db_database_free(struct db_database_t* self) {
	uint32_t n;
	for (n = 0 ; n < self->tableSize ; n++) {
		db_table_free(self->tables[n]);
	}
	for (n = 0 ; n < self->indexSize ; n++) {
		db_index_free(self->indexs[n]);
	}
	free(self->tables);
	free(self->indexs);
    free(self);
}

uint8_t db_database_drop(struct db_database_t* self) {
	uint8_t result = db_file_remove("database.hdb");
	uint32_t n;
	for (n = 0 ; n < self->tableSize ; n++) {
		struct db_table_t* table = self->tables[n];
		result |= db_table_drop(table);
	}
	for (n = 0 ; n < self->indexSize ; n++) {
		struct db_index_t* index = self->indexs[n];
		result |= db_index_drop(index);
	}
    return result;
}

struct db_database_t* db_database_create() {
    struct db_database_t* self = (struct db_database_t*)calloc(1, sizeof(struct db_database_t));
    if (!self) {
        return 0;
    }
	self->tableMax = 16;
	if (!(self->tables = (struct db_table_t**)calloc(self->tableMax, sizeof(struct db_table_t*)))) {
		free(self);
		return 0;
	}
	self->indexMax = 16;
	if (!(self->indexs = (struct db_index_t**)calloc(self->indexMax, sizeof(struct db_index_t*)))) {
		free(self->tables);
		free(self);
		return 0;
	}
    return self;
}

struct db_database_t* db_database_open() {
	uint8_t* bytes = db_file_read_bytes("database.hdb", 0), *aux = bytes;
	if (!bytes) return 0;
    struct db_database_t* self = (struct db_database_t*)calloc(1, sizeof(struct db_database_t));
    if (!self) {
		free(bytes);
		return 0;
	}
	self->version = db_bytes_read_uint32_compressed(&aux);
	uint32_t tableSize = db_bytes_read_uint32_compressed(&aux);
	self->tableMax = tableSize == 0 ? 16 : tableSize;
	uint32_t indexSize = db_bytes_read_uint32_compressed(&aux);
	self->indexMax = indexSize == 0 ? 16 : indexSize;
	if (!(self->tables = (struct db_table_t**)calloc(self->tableMax, sizeof(struct db_table_t*)))) {
		free(bytes);
		free(self);
		return 0;
	}
	if (!(self->indexs = (struct db_index_t**)calloc(self->indexMax, sizeof(struct db_index_t*)))) {
		free(bytes);
		free(self->tables);
		free(self);
		return 0;
	}
	uint16_t len;
	uint32_t n;
	for (n = 0 ; n < tableSize ; n++) {
		char* name = db_bytes_read_ascii(&aux, &len);
		if (!name) {
			free(bytes);
			db_database_free(self);
			return 0;
		}
		struct db_table_t* table = db_table_open(name);
		free(name);
		if (!table) {
			free(bytes);
			db_database_free(self);
			return 0;
		}
		self->tables[self->tableSize++] = table;
	}
	for (n = 0 ; n < indexSize ; n++) {
		char* name = db_bytes_read_ascii(&aux, &len);
		if (!name) {
			free(bytes);
			db_database_free(self);
			return 0;
		}
		struct db_index_t* index = db_index_open(name);
		free(name);
		if (!index) {
			free(bytes);
			db_database_free(self);
			return 0;
		}
		self->indexs[self->indexSize++] = index;
	}
	free(bytes);
    return self;
}

uint8_t db_database_exist() {
    return db_file_exist("database.hdb");
}

void db_database_set_version(struct db_database_t* self, uint32_t version) {
	self->version = version;
}

uint32_t db_database_get_version(struct db_database_t* self) {
	return self->version;
}

struct db_table_t* db_database_table_create(struct db_database_t* self, const char* name, uint32_t pages, uint32_t slots) {
	if (self->tableSize == self->tableMax) {
		struct db_table_t** tables = (struct db_table_t**)realloc(self->tables, self->tableMax * 2 * sizeof(struct db_table_t*));
		if (!tables) {
			return 0;
		}
		self->tableMax *= 2;
		self->tables = tables;
	}
	struct db_table_t* table = db_table_create(name, pages, slots);
	if (!table) {
		return 0;
	}
	int32_t index = self->tableSize++;
	self->tables[index] = table;
	return table;
}

struct db_table_t* db_database_table_open(struct db_database_t* self, const char* name) {
    uint32_t n;
    size_t length = strlen(name);
    for (n = 0; n < self->tableSize ; n++) {
        struct db_table_t* table = self->tables[n];
        if (table->nameLength == length && table->name[0] == name[0] && !strcmp(table->name, name)) {
            return table;
        }
    }
	return 0;
}

struct db_index_t* db_database_index_create(struct db_database_t* self, const char* name, uint32_t pages, uint32_t slots) {
	if (self->indexSize == self->indexMax) {
		struct db_index_t** indexs = (struct db_index_t**)realloc(self->indexs, self->indexMax * 2 * sizeof(struct db_index_t*));
		if (!indexs) {
			return 0;
		}
		self->indexMax *= 2;
		self->indexs = indexs;
	}
	struct db_index_t* index = db_index_create(name, pages, slots);
	if (!index) {
		return 0;
	}
	int32_t result = self->indexSize++;
	self->indexs[result] = index;
	return index;
}

struct db_index_t* db_database_index_open(struct db_database_t* self, const char* name) {
    uint32_t n;
    size_t length = strlen(name);
    for (n = 0; n < self->indexSize ; n++) {
        struct db_index_t* index = self->indexs[n];
        if (index->nameLength == length && index->name[0] == name[0] && !strcmp(index->name, name)) {
            return index;
        }
    }
	return 0;
}

uint8_t db_database_commit(struct db_database_t* self) {
	int32_t n;
	for (n = 0; n < self->tableSize ; n++) {
		struct db_table_t* table = self->tables[n];
		if (db_table_commit(table)) {
			return 1;
		}
	}
	for (n = 0; n < self->indexSize ; n++) {
		struct db_index_t* index = self->indexs[n];
		if (db_index_commit(index)) {
			return 1;
		}
	}
	{
		size_t length = 3 * sizeof(uint32_t) + (self->tableSize + self->indexSize) * 64;
		struct db_bytes_t* bytes = db_bytes_create(length);
		db_bytes_write_uint32_compressed(bytes, self->version);
		db_bytes_write_uint32_compressed(bytes, self->tableSize);
		db_bytes_write_uint32_compressed(bytes, self->indexSize);
		uint32_t n;
		for (n = 0 ; n < self->tableSize ; n++) {
			struct db_table_t* table = self->tables[n];
			db_bytes_write_ascii(bytes, table->name, table->nameLength);
		}
		for (n = 0 ; n < self->indexSize ; n++) {
			struct db_index_t* index = self->indexs[n];
			db_bytes_write_ascii(bytes, index->name, index->nameLength);
		}
		if (db_file_write_bytes("database.hdb", db_bytes_result(bytes), db_bytes_size(bytes))) {
			db_bytes_free(bytes);
			return 1;
		}
		db_bytes_free(bytes);
	}
	return 0;
}

uint8_t db_database_rollback(struct db_database_t* self) {
	int32_t n;
	for (n = 0; n < self->tableSize ; n++) {
		struct db_table_t* table = self->tables[n];
		if (db_table_rollback(table)) {
			return 1;
		}
	}
	for (n = 0; n < self->indexSize ; n++) {
		struct db_index_t* index = self->indexs[n];
		if (db_index_rollback(index)) {
			return 1;
		}
	}
	return 0;
}

int32_t db_database_insert(struct db_database_t* self, int32_t table_index, uint8_t* bytes, size_t size) {
	if (!self || !self->tables || table_index >= self->tableSize) {
		return 0;
	}
	struct db_table_t* table = self->tables[table_index];
	int32_t id = db_table_add(table, bytes, size);
	return id;
}

uint8_t db_database_update(struct db_database_t* self, int32_t table_index, uint32_t id, uint8_t* bytes, size_t size) {
	if (!self || !self->tables || table_index >= self->tableSize) {
		return 1;
	}
	struct db_table_t* table = self->tables[table_index];
	if (db_table_set(table, id, bytes, size)) {
		return 1;
	}
	return 0;
}

uint8_t db_database_remove(struct db_database_t* self, int32_t table_index, uint32_t id) {
	if (!self || !self->tables || table_index >= self->tableSize) {
		return 1;
	}
	struct db_table_t* table = self->tables[table_index];
	if (db_table_remove(table, id)) {
		return 1;
	}
	return 0;
}

#ifdef __OBJC2__

#import "database.h"

@interface DbDatabase () {
	struct db_database_t* _database;
}

@end


@interface DbTable () {
@public
	struct db_database_t* _database;
	struct db_table_t* _table;
}

@end

@interface DbIndex () {
@public
	struct db_database_t* _database;
	struct db_index_t* _index;
}

@end

@interface DbOutData () {
@public
    struct db_bytes_t* _data;
}

@end

@interface DbInData () {
@public
    uint8_t* _bytes;
    uint8_t* _next;
    size_t _size;
}

@end

@implementation DbDatabase

+ (BOOL)exist {
	return db_database_exist();
}

- (DbDatabase*)initCreate {
	if (!(self = [super init])) return nil;
	_database = db_database_create();
	return self;
}

- (DbDatabase*)initOpen {
	if (!(self = [super init])) return nil;
	_database = db_database_open();
	if (!_database) return nil;
	return self;
}

- (DbDatabase*)drop {
	db_database_drop(_database);
	return self;
}

- (DbTable*)createTable:(NSString*)name page:(uint32_t)pages slot:(uint32_t)slots {
	const char *pname = [name cStringUsingEncoding:NSASCIIStringEncoding];
	struct db_table_t* ntable = db_database_table_create(_database, pname, pages, slots);
	if (!ntable) return nil;
	DbTable *table = [[DbTable alloc] init];
	if (!table) return nil;
	table->_database = _database;
	table->_table = ntable;
    return table;
}

- (DbTable*)openTable:(NSString*)name {
	const char *pname = [name cStringUsingEncoding:NSASCIIStringEncoding];
	struct db_table_t* ntable = db_database_table_open(_database, pname);
	if (!ntable) return nil;
	DbTable *table = [[DbTable alloc] init];
	if (!table) return nil;
	table->_database = _database;
	table->_table = ntable;
	return table;
}

- (DbIndex*)createIndex:(NSString*)name {
	const char *pname = [name cStringUsingEncoding:NSASCIIStringEncoding];
	struct db_index_t* nindex = db_database_index_create(_database, pname, 1, 16 * 1024);
	if (!nindex) return nil;
	DbIndex *index = [[DbIndex alloc] init];
	if (!index) return nil;
	index->_database = _database;
	index->_index = nindex;
    return index;
}

- (DbIndex*)openIndex:(NSString*)name {
	const char *pname = [name cStringUsingEncoding:NSASCIIStringEncoding];
	struct db_index_t* nindex = db_database_index_open(_database, pname);
	if (!nindex) return nil;
	DbIndex *index = [[DbIndex alloc] init];
	if (!index) return nil;
	index->_database = _database;
	index->_index = nindex;
    return index;
}

- (BOOL)commit {
	return db_database_commit(_database);
}

- (BOOL)rollback {
	return db_database_rollback(_database);
}

- (uint32_t)version {
    return db_database_get_version(_database);
}

- (void)setVersion:(uint32_t)version {
    db_database_set_version(_database, version);
}

- (void)dealloc {
	if (_database) {
		db_database_free(_database);
	}
}

@end

@implementation DbTable

- (DbIndex*)createIndex:(NSString*)name {
    const char *pname = [name cStringUsingEncoding:NSASCIIStringEncoding];
	struct db_index_t* nindex = db_database_index_create(_database, pname, 1, 16 * 1024);
	if (!nindex) return nil;
	DbIndex *index = [[DbIndex alloc] init];
	if (!index) return nil;
	index->_database = _database;
	index->_index = nindex;
    return index;
}

- (DbIndex*)openIndex:(NSString*)name {
    const char *pname = [name cStringUsingEncoding:NSASCIIStringEncoding];
	struct db_index_t* nindex = db_database_index_open(_database, pname);
	if (!nindex) return nil;
	DbIndex *index = [[DbIndex alloc] init];
	if (!index) return nil;
	index->_database = _database;
	index->_index = nindex;
    return index;
}

- (DbInData*)search:(uint32_t)id {
	size_t length;
	uint8_t* bytes = db_table_get(_table, id, &length);
	if (!bytes || length == 0) return nil;
    uint8_t* aux = (uint8_t*)malloc(length);
    if (!aux) return nil;
    memcpy(aux, bytes, length);
	DbInData* data = [[DbInData alloc] init];
	data->_bytes = aux;
	data->_next = aux;
	data->_size = length;
    return data;
}

- (BOOL)contain:(uint32_t)id {
    return db_table_get(_table, id, 0) != 0;
}

- (uint32_t)size {
	return db_table_size(_table);
}

- (uint32_t)insert:(DbOutData*)data {
    if (!data) return 0;
	uint8_t* bytes = db_bytes_result(data->_data);
    if (!bytes) return 0;
	size_t size = db_bytes_size(data->_data);
	db_bytes_reset(data->_data);
	return db_table_add(_table, bytes, size);
}

- (uint8_t)insert:(uint32_t)id data:(DbOutData*)data {
    if (!data) return 0;
	uint8_t* bytes = db_bytes_result(data->_data);
    if (!bytes) return 0;
	size_t size = db_bytes_size(data->_data);
	db_bytes_reset(data->_data);
	return db_table_put(_table, id, bytes, size);
}

- (BOOL)update:(uint32_t)id data:(DbOutData*)data {
    if (!data) return 0;
	uint8_t* bytes = db_bytes_result(data->_data);
	if (!bytes) return 0;
	size_t size = db_bytes_size(data->_data);
	db_bytes_reset(data->_data);
    return db_table_set(_table, id, bytes, size) == 0;
}

- (BOOL)remove:(uint32_t)id {
    if (!id) return 0;
	return db_table_remove(_table, id) == 0;
}

@end

@implementation DbIndex

- (uint32_t)search:(uint64_t)key {
	return db_index_get(_index, key);
}

- (BOOL)insert:(uint64_t)key id:(uint32_t)value {
	return db_index_add(_index, key, value) == 0;
}

- (BOOL)remove:(uint64_t)key {
    return db_index_remove(_index, key) == 0;
}

- (uint32_t)search:(uint64_t)key offset:(uint32_t)offset limit:(uint32_t)limit array:(uint32_t*)array {
    return db_index_search_range(_index, key, offset, limit, array);
}

- (void)search:(uint64_t)key callback:(void(^)(uint32_t id, BOOL* stop))callback {
    uint32_t n, m, ofs = 0, ids[16];
    BOOL stop = false;
    for (; !stop && (n = db_index_search_range(_index, key, ofs, 16, ids)) != 0 ; ofs += n) {
        for (m = 0; !stop && m < n ; m++) {
            callback(ids[m], &stop);
        }
    }
}

@end

@implementation DbInData

- (id)init {
	if (!(self = [super init])) return nil;
	return self;
}

- (uint8_t)readUByte {
    if (_next > _bytes + _size) return 0;
    uint8_t result = db_bytes_read_uint8(_next);
    _next++;
    return result;
}

- (uint16_t)readUShort {
    if (_next > _bytes + _size) return 0;
    uint16_t result = db_bytes_read_uint16(_next);
    _next += 2;
    return result;
}

- (uint32_t)readUInt {
    if (_next > _bytes + _size) return 0;
    return db_bytes_read_uint32_compressed(&_next);
}

- (uint64_t)readULong {
    if (_next > _bytes + _size) return 0;
    return db_bytes_read_uint64_compressed(&_next);
}

- (NSString*)readStringAscii {
	if (_next > _bytes + _size) return 0;
	uint16_t length;
	char* text = db_bytes_read_ascii(&_next, &length);
	if (!text) return 0;
	NSString* result = [NSString stringWithCString:text encoding:NSASCIIStringEncoding];
	free(text);
	return result;
}

- (id<NSCoding>)readCoding {
	uint16_t length = [self readUShort];
	if (length == 0) return nil;
	NSData *codedData = [NSData dataWithBytes:_next length:length];
	NSKeyedUnarchiver *unarchiver = [[NSKeyedUnarchiver alloc] initForReadingWithData:codedData];
    id<NSCoding> result = [unarchiver decodeObjectForKey:@"value"];
    [unarchiver finishDecoding];
	_next += length;
	return result;
}

- (DbInData*)reset {
    _next = _bytes;
	return self;
}

- (void)dealloc {
	free(_bytes);
}

@end


@implementation DbOutData

- (id)init {
	if (!(self = [super init])) return nil;
	_data = db_bytes_create(32);
	return self;
}

- (DbOutData*)writeUByte:(uint8_t)value {
	db_bytes_write_uint8(_data, value);
    return self;
}

- (DbOutData*)writeUShort:(uint16_t)value {
	db_bytes_write_uint16(_data, value);
    return self;
}

- (DbOutData*)writeUInt:(uint32_t)value {
	db_bytes_write_uint32_compressed(_data, value);
    return self;
}

- (DbOutData*)writeULong:(uint64_t)value {
	db_bytes_write_uint64_compressed(_data, value);
    return self;
}

- (DbOutData*)writeStringAscii:(NSString*)value {
	const char* chars = [value cStringUsingEncoding:NSASCIIStringEncoding];
	db_bytes_write_ascii(_data, chars, value.length);
    return self;
}

- (DbOutData*)writeCoding:(id<NSCoding>)value {
	NSMutableData *data = [[NSMutableData alloc] init];
	if (!data) return nil;
    NSKeyedArchiver *archiver = [[NSKeyedArchiver alloc] initForWritingWithMutableData:data];
	if (!archiver) return nil;
    [archiver encodeObject:value forKey:@"value"];
    [archiver finishEncoding];
	if (db_bytes_write_uint16(_data, data.length)) return nil;
	if (db_bytes_write_bytes(_data, data.bytes, data.length)) return nil;
	return self;
}

- (DbOutData*)reset {
    db_bytes_reset(_data);
	return self;
}

- (void)dealloc {
	db_bytes_free(_data);
}

@end

#endif