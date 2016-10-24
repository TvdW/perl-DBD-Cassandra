#!perl
use 5.010;
use strict;
use warnings;
use Test::More;
use Cassandra::Client::Protocol qw/:constants pack_int/;
use Cassandra::Client::Encoder 'make_encoder';
use Cassandra::Client::Decoder 'make_decoder';

sub check_encdec {
    my ($rowspec, $row, $expected)= @_;
    eval {
        $expected= $row unless defined $expected;

        my $encoder= make_encoder($rowspec);
        ok($encoder) or diag('No encoder');
        my $decoder= make_decoder($rowspec);
        ok($decoder) or diag('No decoder');
        my $encoded= $encoder->($row);
        ok(length $encoded) or diag('Encoding failed');
        HACK: { # Turns the encoded parameter list into something our decoders understand
            substr($encoded, 0, 2, '');
            $encoded= pack_int(1).$encoded;
        }
        my $decoded= $decoder->($encoded);
        is_deeply($decoded->[0], $expected);

        1;
    } or do {
        my $error= $@ || '??';
        ok(0) or diag $error;
    };
}

sub check_simple {
    my ($coltype, $row, $expected)= @_;
    my $meta= { columns => [] };
    my $i= ord('a');
    for my $field (@$row) {
        push @{$meta->{columns}}, [ 'schema', 'table', chr($i++), $coltype ];
    }
    check_encdec($meta, $row, $expected);
}

sub check_enc {
    my ($coltype, $col, $expected)= @_;
    my $meta= {
        columns => [
            [ 'schema', 'table', 'a', $coltype ]
        ]
    };
    eval {
        my $encoder= make_encoder($meta);
        ok($encoder) or diag('No encoder');
        my $encoded= $encoder->([ $col ]);
        my $rowcount= unpack('n', substr($encoded, 0, 2, ''));
        ok($rowcount == 1);
        my $col_length= unpack('l>', substr($encoded, 0, 4, ''));
        ok($col_length >= 0);
        is(unpack('H*', $encoded), unpack('H*', $expected));
        1;
    } or do {
        my $error= $@ || "??";
        ok(0) or diag $error;
    };
}

# Custom
check_simple([TYPE_CUSTOM, 'java.lang.String'], [ 'abc', 'def', undef ]);
check_enc([TYPE_CUSTOM, 'java.lang.String'], 'abc', 'abc');

# ASCII
check_simple([TYPE_ASCII], [ 'test', 'abcdef', '', undef, 0 ]);
check_enc([TYPE_ASCII], 'test', 'test');

# Bigint
check_simple([TYPE_BIGINT], [0, 1, -2, 3, -4, 5, "1000000000000", undef]);
check_enc([TYPE_BIGINT], 0, "\0\0\0\0\0\0\0\0");
check_enc([TYPE_BIGINT], "1000000000000", "\0\0\0\xe8\xd4\xa5\x10\0");
check_enc([TYPE_BIGINT], -1, "\xff\xff\xff\xff\xff\xff\xff\xff");

# Blob
check_simple([TYPE_BLOB], [ 'test', undef, chr(rand()%256), '0', '' ]);
check_enc([TYPE_BLOB], "\0\1\2abc", "\0\1\2abc");

# Boolean
check_simple([TYPE_BOOLEAN], [ !0, !1, undef, 1, '' ]);
check_simple([TYPE_BOOLEAN], [ 2, 3, -1, 'a' ],
                             [ !0, !0, !0, !0 ]);
check_enc([TYPE_BOOLEAN], 1, "\1");
check_enc([TYPE_BOOLEAN], 0, "\0");

# Counter
check_simple([TYPE_COUNTER], [ undef, 0, 1, 2, 3, 4 ]);
check_enc([TYPE_COUNTER], 0, "\0\0\0\0\0\0\0\0");
check_enc([TYPE_COUNTER], "1000000000000", "\0\0\0\xe8\xd4\xa5\x10\0");
check_enc([TYPE_COUNTER], -1, "\xff\xff\xff\xff\xff\xff\xff\xff");

# Decimal
check_simple([TYPE_DECIMAL], [ undef, 0, 1, 100, '1e+100', 1E100 ]);
check_simple([TYPE_DECIMAL], [ "10000000000000001.123456789123456789E-1000" ], [ "10000000000000001123456789123456789e-1018" ]);
check_enc([TYPE_DECIMAL], 0, "\0\0\0\0\0");
check_enc([TYPE_DECIMAL], 1, "\0\0\0\0\1");
check_enc([TYPE_DECIMAL], 0.1, "\0\0\0\1\1");

# Double
check_simple([TYPE_DOUBLE], [ 0.5, 2, -3, undef, 0 ]);
check_enc([TYPE_DOUBLE], 0.1, "\x3f\xb9\x99\x99\x99\x99\x99\x9a");

# Float
check_simple([TYPE_FLOAT], [ 0.5, 2, -3, undef, 0 ]);
check_enc([TYPE_FLOAT], 0.1, "\x3d\xcc\xcc\xcd");

# Int
check_simple([TYPE_INT], [ 1, 2, -3, undef, 0 ]);
check_enc([TYPE_INT], 1, "\0\0\0\1");
check_enc([TYPE_INT], -1, "\xff\xff\xff\xff");

# Timestamp
check_simple([TYPE_TIMESTAMP], [ time()*1000, undef, 0 ]);
check_enc([TYPE_TIMESTAMP], 0, "\0\0\0\0\0\0\0\0");
check_enc([TYPE_TIMESTAMP], "1000000000000", "\0\0\0\xe8\xd4\xa5\x10\0");
check_enc([TYPE_TIMESTAMP], -1, "\xff\xff\xff\xff\xff\xff\xff\xff");

# Uuid
check_simple([TYPE_UUID], [ '00000000-0000-0000-0000-000000000000', 'ffffffff-ffff-ffff-ffff-ffffffffffff', undef ]);
check_enc([TYPE_UUID], '00000000-0000-0000-0000-000000000000', "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0");

# Varchar
check_simple([TYPE_VARCHAR], [ 'test', 'a', '', undef, '0' ]);
check_enc([TYPE_VARCHAR], 'test', 'test');

# Varint
check_simple([TYPE_VARINT], [ 1, 1000, 1000000, 1000000000, -1, -1000, -1000000, -1000000000, "100000000000", "1000000000000000000000000", "-1000000000000000000000000", "1000000000000000000000000000000000000000000000000000000000000000000"]);
check_enc([TYPE_VARINT], 0, "\0");
check_enc([TYPE_VARINT], 1, "\1");
check_enc([TYPE_VARINT], 127, "\x7f");
check_enc([TYPE_VARINT], 128, "\0\x80");
check_enc([TYPE_VARINT], 129, "\0\x81");
check_enc([TYPE_VARINT], -1, "\xff");
check_enc([TYPE_VARINT], -128, "\x80");
check_enc([TYPE_VARINT], -129, "\xff\x7f");
check_enc([TYPE_VARINT], "1000000000000000000000000000000000000000000000000000000000000000000", "\x09\x7e\xdd\x87\x1c\xfd\xa3\xa5\x69\x77\x58\xbf\x0e\x3c\xbb\x5a\xc5\x74\x1c\x64\0\0\0\0\0\0\0\0");

# Timeuuid
check_simple([TYPE_TIMEUUID], [ '568ef050-5aca-11e5-9c6b-eb15c19b7bc8', undef ]);
check_enc([TYPE_TIMEUUID], '568ef050-5aca-11e5-9c6b-eb15c19b7bc8', "\x56\x8e\xf0\x50\x5a\xca\x11\xe5\x9c\x6b\xeb\x15\xc1\x9b\x7b\xc8");

# Inet
check_simple([TYPE_INET], [undef, qw/
                                     2001:1af8:4300:a031:1::
                                     1234::1234
                                     1::1
                                     127.0.0.1
                                     1.2.3.4
                                     ::123
                                     123::123
                                     1::1
                                     1::
                                     ::1
                                    /]);
check_enc([TYPE_INET], "1.2.3.4", "\1\2\3\4");
check_enc([TYPE_INET], "::1", "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\1");

# List
check_simple([TYPE_LIST, [ TYPE_INT ] ], [ [1, 2, 3], [4, 5, 6], undef, [4, undef, 6] ]);
check_enc([TYPE_LIST, [ TYPE_INT ] ], [ 1, 2, 3 ], "\0\0\0\3\0\0\0\4\0\0\0\1\0\0\0\4\0\0\0\2\0\0\0\4\0\0\0\3");

# Map
check_simple([TYPE_MAP, [ TYPE_INT ], [ TYPE_BOOLEAN ] ], [
                                                            { 1 => !1, 2 => !0 },
                                                          ]);
check_enc([TYPE_MAP, [ TYPE_INT ], [ TYPE_BOOLEAN ] ], { 1 => !1, 2 => !0 }, "\0\0\0\2\0\0\0\4\0\0\0\1\0\0\0\1\0\0\0\0\4\0\0\0\2\0\0\0\1\1");

# Set
check_simple([TYPE_SET, [ TYPE_INT ]], [
                                        [ 1, 2, 3 ]
                                       ]);
check_enc([TYPE_SET, [ TYPE_INT ]], [ 1, 2, 3 ], "\0\0\0\3\0\0\0\4\0\0\0\1\0\0\0\4\0\0\0\2\0\0\0\4\0\0\0\3");

# UDT

# Tuple

# list<frozen<map<int,bool>>>
check_simple([TYPE_LIST, [TYPE_MAP, [TYPE_INT], [TYPE_BOOLEAN]]], [
                                                                    [ { 1 => !1, 2 => !0 } ]
                                                                  ]);
check_enc([TYPE_LIST, [TYPE_MAP, [TYPE_INT], [TYPE_BOOLEAN]]],
    [ { 1 => !1, 2 => !0} ],
    pack("H*", '000000010000001e000000020000000400000001000000010000000004000000020000000101')
);

# set<frozen<map<int,boolean>>>
check_simple([TYPE_SET, [TYPE_MAP, [TYPE_INT], [TYPE_BOOLEAN]]], [
                                                                    [ { 1 => !1, 2 => !0 } ],
                                                                 ]);
check_enc([TYPE_SET, [TYPE_MAP, [TYPE_INT], [TYPE_BOOLEAN]]],
    [ { 1 => !1, 2 => !0 } ],
    pack('H*', '000000010000001e000000020000000400000001000000010000000004000000020000000101')
);

# map<int,frozen<list<int>>>
check_simple([TYPE_MAP, [TYPE_INT], [TYPE_LIST, [TYPE_INT]]], [
                                                                { 1 => [2], 2 => [3] }
                                                              ]);
check_enc([TYPE_MAP, [TYPE_INT], [TYPE_LIST, [TYPE_INT]]],
    { 1 => [2], 2 => [3] },
    pack('H*', '0000000200000004000000010000000c00000001000000040000000200000004000000020000000c000000010000000400000003')
);


done_testing;
