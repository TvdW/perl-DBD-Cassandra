#!perl
use 5.008;
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

# Custom
check_simple([TYPE_CUSTOM, 'java.lang.String'], [ 'abc', 'def', undef ]);

# ASCII
check_simple([TYPE_ASCII], [ 'test', 'abcdef', '', undef, 0 ]);

# Bigint
check_simple([TYPE_BIGINT], [0, 1, -2, 3, -4, 5, undef]);

# Blob
check_simple([TYPE_BLOB], [ 'test', undef, chr(rand()%256), '0', '' ]);

# Boolean
check_simple([TYPE_BOOLEAN], [ !0, !1, undef, 1, '' ]);
check_simple([TYPE_BOOLEAN], [ 2, 3, -1, 'a' ],
                             [ !0, !0, !0, !0 ]);

# Counter
check_simple([TYPE_COUNTER], [ undef, 0, 1, 2, 3, 4 ]);

# Decimal
## Not implemented.

# Double
check_simple([TYPE_DOUBLE], [ 0.5, 2, -3, undef, 0 ]);

# Float
check_simple([TYPE_FLOAT], [ 0.5, 2, -3, undef, 0 ]);

# Int
check_simple([TYPE_INT], [ 1, 2, -3, undef, 0 ]);

# Timestamp
check_simple([TYPE_TIMESTAMP], [ time()*1000, undef, 0 ]);

# Uuid
check_simple([TYPE_UUID], [ '00000000-0000-0000-0000-000000000000', 'ffffffff-ffff-ffff-ffff-ffffffffffff', undef ]);

# Varchar
check_simple([TYPE_VARCHAR], [ 'test', 'a', '', undef, '0' ]);

# Varint
check_simple([TYPE_VARINT], [ 1, 1000, 1000000, 1000000000, -1, -1000, -1000000, -1000000000, "100000000000", "1000000000000000000000000", "-1000000000000000000000000", "1000000000000000000000000000000000000000000000000000000000000000000"]);

# Timeuuid
check_simple([TYPE_TIMEUUID], [ '568ef050-5aca-11e5-9c6b-eb15c19b7bc8', undef ]);

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

# List
check_simple([TYPE_LIST, [ TYPE_INT ] ], [ [1, 2, 3], [4, 5, 6], undef, [4, undef, 6] ]);

# Map
check_simple([TYPE_MAP, [ TYPE_INT ], [ TYPE_BOOLEAN ] ], [
                                                            { 1 => !1, 2 => !0 },
                                                          ]);

# Set
check_simple([TYPE_SET, [ TYPE_INT ]], [
                                        [ 1, 2, 3 ]
                                       ]);

# UDT

# Tuple


done_testing;
