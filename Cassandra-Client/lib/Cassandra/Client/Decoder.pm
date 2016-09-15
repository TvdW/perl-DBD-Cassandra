package Cassandra::Client::Decoder;
use 5.008;
use strict;
use warnings;

use Exporter 'import';
our @EXPORT_OK= qw/make_decoder/;

use Cassandra::Client::Protocol qw/:constants unpack_long BIGINT_SUPPORTED/;
use vars qw/@ROW/;

my $bigint_dec= BIGINT_SUPPORTED ? 'q>' : \&d_bigint_slow;

my %type_lookup= (
    TYPE_CUSTOM     ,=> [ \&d_passthru ],
    TYPE_ASCII      ,=> [ \&d_passthru ],
    TYPE_BIGINT     ,=> [ $bigint_dec  ],
    TYPE_BLOB       ,=> [ \&d_passthru ],
    TYPE_BOOLEAN    ,=> [ \&d_bool     ],
    TYPE_COUNTER    ,=> [ $bigint_dec  ],
    #TYPE_DECIMAL    ,=> decimal
    TYPE_DOUBLE     ,=> [ 'd>'         ],
    TYPE_FLOAT      ,=> [ 'f>'         ],
    TYPE_INT        ,=> [ 'l>'         ],
    TYPE_TEXT       ,=> [ \&d_string   ],
    #TYPE_VARINT     ,=> varint
    TYPE_TIMESTAMP  ,=> [ $bigint_dec  ],
    TYPE_UUID       ,=> [ \&d_uuid     ],
    TYPE_VARCHAR    ,=> [ \&d_string   ],
    TYPE_TIMEUUID   ,=> [ \&d_uuid     ],
    TYPE_INET       ,=> [ \&d_inet     ],
    TYPE_LIST       ,=> [ \&d_list     ],
    TYPE_MAP        ,=> [ \&d_map      ],
    TYPE_SET        ,=> [ \&d_set      ],
);

sub make_decoder {
    my ($metadata)= @_;
    return undef unless $metadata->{columns};

    my $column_count= 0+@{$metadata->{columns}};

    my $decoder= <<EOC;

    my \$false= !1;
    my \$true= !\$false;
    sub {
        my (\@output, \$byte_count, \$tmp_val);
        my \$row_count= unpack('l>', substr(\$_[0], 0, 4, ''));
        \$#output= \$row_count-1;
        for (0..(\$row_count-1)) {
            \$output[\$_]= [(undef)x$column_count];
            local *ROW= \$output[\$_];
@{[ map { my $col= $_; my $column= $metadata->{columns}[$col]; <<EOP } 0..($column_count-1)

            # Field $col, $column->[0].$column->[1].$column->[2]
            \$byte_count= unpack('l>', substr(\$_[0], 0, 4, ''));
            if (\$byte_count >= 0) {
                \$tmp_val= substr(\$_[0], 0, \$byte_count, '');
                @{[ make_value_decoder($column->[3], 4, '$tmp_val', "\$ROW[$_]", '$byte_count') ]}
            } # default: else { \$row[$col]= undef; }

EOP
]}
        }

        return \\\@output;
    }

EOC

    return ( eval($decoder) or die $@ );
}

sub make_value_decoder {
    my ($type, $indent, $tmp_val, $dest, $input_length)= @_;

    my $val_decoder= "# Decoder for type $type->[0]\n";
    my $lookedup= $type_lookup{$type->[0]};

    if (!$lookedup) {
        warn 'Type '.$type->[0].' not implemented, returning undef';
    } elsif (!ref $lookedup->[0]) { # decode pack format
        $val_decoder .= "$dest= unpack('$lookedup->[0]', $tmp_val);\n";
    } elsif (ref $lookedup->[0] eq 'CODE') {
        $val_decoder .= $lookedup->[0]->($type, $tmp_val, $dest, $input_length);
    }

    # Indent it for readability
    $val_decoder= "\n".(join "\n", map { ("    " x $indent).$_ } split /\n/, $val_decoder)."\n";

    return $val_decoder;
}

sub d_string {
    my ($type, $tmp_val, $dest, $input_length)= @_;

    return "$dest= $tmp_val;
utf8::decode $dest;";
}

sub d_map {
    my ($type, $tmp_val, $dest, $input_length)= @_;

    my $val_decoder= <<EOC;
{
    my \$map_entries= unpack('l>', substr($tmp_val, 0, 4, ''));
    my (\$map_byte_count, \$map_bytes, \%map);
    for (1..\$map_entries) {
        my (\$key, \$value);
        \$map_byte_count= unpack('l>', substr($tmp_val, 0, 4, ''));
        if (\$map_byte_count >= 0) {
            \$map_bytes= substr($tmp_val, 0, \$map_byte_count, '');
            @{[ make_value_decoder($type->[1], 3, '$map_bytes', '$key', '$map_byte_count') ]}
        }
        \$map_byte_count= unpack('l>', substr($tmp_val, 0, 4, ''));
        if (\$map_byte_count >= 0) {
            \$map_bytes= substr($tmp_val, 0, \$map_byte_count, '');
            @{[ make_value_decoder($type->[2], 3, '$map_bytes', '$value', '$map_byte_count') ]}
        }
        \$map{\$key}= \$value;
    }
    $dest= \\\%map;
}
EOC
}

sub d_set {
    my ($type, $tmp_val, $dest, $input_length)= @_;

    my $val_decoder= <<EOC;
{
    my \$set_entries= unpack('l>', substr($tmp_val, 0, 4, ''));
    my (\$set_byte_count, \$set_bytes, \@set);
    for (1..\$set_entries) {
        my \$item;
        \$set_byte_count= unpack('l>', substr($tmp_val, 0, 4, ''));
        if (\$set_byte_count >= 0) {
            \$set_bytes= substr($tmp_val, 0, \$set_byte_count, '');
            @{[ make_value_decoder($type->[1], 3, '$set_bytes', '$item', '$set_byte_count') ]}
        }
        push \@set, \$item;
    }
    $dest= \\\@set;
}
EOC
}

sub d_list {
    my ($type, $tmp_val, $dest, $input_length)= @_;

    my $val_decoder= <<EOC;
{
    my \$list_entries= unpack('l>', substr($tmp_val, 0, 4, ''));
    my (\$list_byte_count, \$list_bytes, \@list);
    \$#list= \$list_entries - 1;
    for my \$list_i (0..(\$list_entries-1)) {
        \$list_byte_count= unpack('l>', substr($tmp_val, 0, 4, ''));
        if (\$list_byte_count >= 0) {
            \$list_bytes= substr($tmp_val, 0, \$list_byte_count, '');
            @{[ make_value_decoder($type->[1], 3, '$list_bytes', '$list[$list_i]', '$list_byte_count') ]}
        }
    }
    $dest= \\\@list;
}
EOC
}

sub d_passthru {
    my ($type, $tmp_val, $dest, $input_length)= @_;
    return "$dest= $tmp_val;";
}

sub d_uuid {
    my ($type, $tmp_val, $dest, $input_length)= @_;
    return "$dest= unpack('H[32]', $tmp_val); $dest =~ ".'s/\A(\w{8})(\w{4})(\w{4})(\w{4})(\w{12})\z/$1-$2-$3-$4-$5/'.";";
}

sub d_inet {
    my ($type, $tmp_val, $dest, $input_length)= @_;
    return <<EOC;
if ($input_length == 4) {
    $dest= join('.', unpack('CCCC', $tmp_val));
} else {
    $dest= join(':', unpack('(H4)[8]', $tmp_val));
    # Simplify the V6 address
    $dest =~ s/\\b0+(\\d+)\\b/\$1/g;
    $dest =~ s/\\b0(:0)+\\b/:/;
    $dest =~ s/:::/::/;
}
EOC
}

sub d_bool {
    my ($type, $tmp_val, $dest, $input_length)= @_;
    return <<EOC;
$dest= unpack('c', $tmp_val) ? \$true : \$false;
EOC
}

sub d_bigint_slow {
    my ($type, $tmp_val, $dest, $input_length)= @_;
    return <<EOC;
$dest= unpack_long($tmp_val);
EOC
}

1;
