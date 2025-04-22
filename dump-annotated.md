| Offset  | Bytes                     | Annotation                                                                 |
| :------ | :------------------------ | :------------------------------------------------------------------------- |
| 0000000 | 45 52 44 49 53            | Magic String: "REDIS"                                                      |
| 0000005 | 30 30 31 30               | RDB Version Number: "0010" (Version 10)                                    |
| 0000009 | FA                        | Auxiliary field opcode                                                     |
| 000000A | 09                        | Length encoding for the key (9 bytes)                                      |
| 000000B | 72 65 64 69 73 2d 76 65 72 | Key: "redis-ver"                                                          |
| 0000014 | 06                        | Length encoding for the value (6 bytes)                                    |
| 0000015 | 37 2e 30 2e 31 2e         | Value: "7.0.1." (Redis Version)                                            |
| 000001B | FA                        | Auxiliary field opcode                                                     |
| 000001C | 0A                        | Length encoding for the key (10 bytes)                                     |
| 000001D | 72 65 64 69 73 2d 62 69 74 73 | Key: "redis-bits"                                                      |
| 0000027 | C0 73                     | Value: Integer encoded (C0 prefix means 1-byte integer). Value is 73.      |
| 0000029 | FA                        | Auxiliary field opcode                                                     |
| 000002A | 0C                        | Length encoding for the key (12 bytes)                                     |
| 000002B | 72 65 64 69 73 2d 63 74 69 6d 65 | Key: "redis-ctime"                                                  |
| 0000037 | C2 07 A3 FA 68            | Value: Integer encoded (C2 prefix means 4-byte integer). Unix timestamp.   |
|         |                           | (Little-endian: 07 A3 FA 68 -> 127,549,352)                                |
| 000003C | FA                        | Auxiliary field opcode                                                     |
| 000003D | 0E                        | Length encoding for the key (14 bytes)                                     |
| 000003E | 72 65 64 69 73 2d 75 73 65 64 2d 6d 65 6d | Key: "redis-used-mem"                                      |
| 000004C | C2 6D B4 98 00            | Value: Integer encoded (C2 prefix means 4-byte integer). Memory usage.     |
|         |                           | (Little-endian: 6D B4 98 00 -> 1,840,992,256)                              |
| 0000051 | FA                        | Auxiliary field opcode                                                     |
| 0000052 | 08                        | Length encoding for the key (8 bytes)                                      |
| 0000053 | 61 6f 66 2d 62 61 73 65   | Key: "aof-base"                                                            |
| 000005B | 00                        | Value: (Likely indicates no AOF base file)                                 |
| 000005C | FE                        | Database Selector opcode                                                   |
| 000005D | 01                        | Database Number: 1                                                         |
| 000005E | FB                        | RESIZEDB opcode                                                            |
| 000005F | 00                        | Hash table size: 0 (Length encoded integer)                                |
| 0000060 | 00                        | Expire hash table size: 0 (Length encoded integer)                         |
| 0000061 | 05                        | Value Type: 5 (String encoding) - Start of a key-value pair.               |
| 0000062 | 05                        | Length encoding for the key (5 bytes)                                      |
| 0000063 | 6b 65 79 79 79            | Key: "keyyy" (ASCII for 6b 65 79 79 79)                                    |
| 0000068 | 05                        | Length encoding for the value (5 bytes)                                    |
| 0000069 | 76 61 6c 75 65            | Value: "value" (ASCII for 76 61 6c 75 65)                                  |
| 000006E | FF                        | End of RDB file opcode                                                     |
| 000006F | 9A 10 F9 B2 A0 0C 00 04   | CRC64 Checksum (8 bytes)                                                   |
