package DBD::Cassandra::Type;
use v5.14;
use warnings;
use vars qw/@INPUT @OUTPUT/;

require Exporter;
our @ISA= 'Exporter';

my %lookup= (
    0  => [\&p2c_string, \&c2p_string,      'TYPE_CUSTOM'],
    1  => [\&p2c_string, \&c2p_string,      'TYPE_ASCII'],
    2  => [\&p2c_bigint, \&c2p_bigint,      'TYPE_BIGINT'],
    3  => [\&p2c_string, \&c2p_string,      'TYPE_BLOB'],
    4  => [\&p2c_bool,   \&c2p_bool,        'TYPE_BOOLEAN'],
    5  => [\&p2c_bigint, \&c2p_bigint,      'TYPE_COUNTER'],
    6  => [\&not_impl,   \&not_impl,        'TYPE_DECIMAL'],
    7  => [\&p2c_double, \&c2p_double,      'TYPE_DOUBLE'],
    8  => [\&p2c_float,  \&c2p_float,       'TYPE_FLOAT'],
    9  => [\&p2c_int,    \&c2p_int,         'TYPE_INT'],
    10 => [\&p2c_string, \&c2p_utf8string,  'TYPE_TEXT'],
    11 => [\&p2c_time,   \&c2p_time,        'TYPE_TIMESTAMP'],
    12 => [\&p2c_uuid,   \&c2p_uuid,        'TYPE_UUID'],
    13 => [\&p2c_string, \&c2p_utf8string,  'TYPE_VARCHAR'],
    14 => [\&not_impl,   \&not_impl,        'TYPE_VARINT'],
    15 => [\&p2c_uuid,   \&c2p_uuid,        'TYPE_TIMEUUID'],
    16 => [\&not_impl,   \&not_impl,        'TYPE_INET'],
    32 => [\&p2c_list,   \&c2p_list,        'TYPE_LIST'],
    33 => [\&p2c_map,    \&c2p_map,         'TYPE_MAP'],
    34 => [\&p2c_list,   \&c2p_list,        'TYPE_SET'],
);

sub not_impl { ... }
sub _pack {
    my ($p, $l, $m, $i)= @_;
    $m //= '';
    return "pack('l> $p', $l, ($i $m))";
}
sub _unpack {
    my ($p, $l, $m, $v)= @_;
    $m //= '';
    return "(unpack('$p', $v) $m)";
}

sub p2c_string {
    my ($i)= @_;
    return ("pack('l>/a', $i)", "utf8::is_utf8($i) && utf8::encode($i)");
}
sub c2p_string { return shift }
sub c2p_utf8string { my $var= shift; return ($var, "utf8::decode $var") }
sub p2c_bigint { return   _pack('q>', 8, undef, @_) }
sub c2p_bigint { return _unpack('q>', 8, undef, @_) }
sub p2c_time { return   _pack('q>', 8, undef, @_) }
sub c2p_time { return _unpack('q>', 8, undef, @_) }
sub p2c_int { return   _pack('l>', 4, undef, @_) }
sub c2p_int { return _unpack('l>', 4, undef, @_) }
sub p2c_bool { return   _pack('C', 1, ' ? 1 : 0', @_) }
sub c2p_bool { return _unpack('C', 1, undef, @_) }
sub p2c_float { return   _pack('f>', 4, undef, @_) }
sub c2p_float { return _unpack('f>', 4, undef, @_) }
sub p2c_double { return   _pack('d>', 8, undef, @_) }
sub c2p_double { return _unpack('d>', 8, undef, @_) }
sub p2c_uuid { return   _pack('H[32]', 16, ' =~ s/\W//rg', @_) }
sub c2p_uuid { return _unpack('H[32]', 16, ' =~ s/\A(\w{8})(\w{4})(\w{4})(\w{4})(\w{12})\z/$1-$2-$3-$4-$5/r', @_) }
#sub p2c_ { return   _pack('', , undef, @_) }
#sub c2p_ { return _unpack('', , undef, @_) }

sub p2c_list {
    my ($i, $type)= @_;

    my $t= $lookup{$type->[0]} or die "Unknown type $type->[0]";
    my ($c, $prep)= $t->[0]('$copied_value', $type->[1]);
    $prep //= '';

    return
        "pack('l>/a', (join '', pack('l>', 0+\@{$i}), map { my \$copied_value= \$_; $prep; defined \$copied_value ? ($c) : \$null } \@{$i}))",
    ;
}

sub c2p_list {
    my ($i, $type)= @_;

    my $t= $lookup{$type->[0]} or die "Unknown type $type->[0]";
    my ($c, $prep)= $t->[1]('$temp_val', $type->[1]);
    $prep //= '';

    return "do {
                my \$rowcount= unpack('l>', substr(($i), 0, 4, ''));
                my \@list;
                for (1..\$rowcount) {
                    my \$byte_count= unpack('l>', substr(($i), 0, 4, ''));
                    if (\$byte_count >= 0) {
                        my \$temp_val= substr(($i), 0, \$byte_count, '');
                        $prep;
                        push \@list, ($c);
                    } else {
                        push \@list, undef;
                    }
                }
                \\\@list;
            }";
}

sub p2c_map {
    my ($i, $types)= @_;
    my ($kt, $vt)= ($lookup{$types->[0][0]}, $lookup{$types->[1][0]});
    ($kt && $vt) or die "Unknown type in map<>";

    my ($c1, $prep1)= $kt->[0]('$copied_key', $types->[0][1]);
    my ($c2, $prep2)= $vt->[0]('$copied_value', $types->[1][1]);
    $prep1 //= '';
    $prep2 //= '';

    return
        "pack('l>/a', (join '', pack('l>', 0+keys \%{$i}), map { my \$copied_key= \$_; my \$copied_value= ($i)->{\$_}; $prep1; $prep2; (($c1),(defined \$copied_value ? ($c2) : \$null)) } keys \%{$i}))",
    ;

    return "";
}

sub c2p_map {
    my ($i, $types)= @_;

    my ($kt, $vt)= ($lookup{$types->[0][0]}, $lookup{$types->[1][0]});
    ($kt && $vt) or die "Unknown type in map<>";

    my ($c1, $prep1)= $kt->[1]('$key_bytes', $types->[0][1]);
    my ($c2, $prep2)= $vt->[1]('$value_bytes', $types->[1][1]);
    $prep1 //= '';
    $prep2 //= '';

    return "do {
                my \$entrycount= unpack('l>', substr(($i), 0, 4, ''));
                my \%hash;
                for (1..\$entrycount) {
                    my (\$key, \$value);

                    my \$key_byte_count= unpack('l>', substr(($i), 0, 4, ''));
                    if (\$key_byte_count >= 0) {
                        my \$key_bytes= substr(($i), 0, \$key_byte_count, '');
                        $prep1;
                        \$key= ($c1);
                    }
                    my \$value_byte_count= unpack('l>', substr(($i), 0, 4, ''));
                    if (\$value_byte_count >= 0) {
                        my \$value_bytes= substr(($i), 0, \$value_byte_count, '');
                        $prep2;
                        \$value= ($c2);
                    }
                    \$hash{\$key}= \$value;
                }
                \\\%hash;
            }";
}

our @EXPORT_OK= qw( build_row_encoder build_row_decoder );

sub build_row_encoder {
    my ($types)= @_;

    return sub{"\0\0"} unless @$types;

    my $count= 0+@$types;

    my $code= "my \$null= pack('l>', -1);\nmy \$length_bits= pack('n', $count);\nsub {\n    local *INPUT= \$_[0];\n";
    my $i= 0;

    my $result;
    for my $type (@$types) {
        if (ref $type eq 'HASH') { $type= $type->{type}; }
        my $t= $lookup{$type->[0]} or die "Unknown type $type->[0]";
        my ($c, $prep)= $t->[0]("\$INPUT[$i]", $type->[1]);

        $code .= "    $prep if defined \$INPUT[$i];\n" if $prep;
        $result .= "        (defined \$INPUT[$i] ? ($c) : \$null) .\n";
        $i++;
    }
    $code = $code  . "    return\n        \$length_bits .\n" . substr($result, 0, -3). "\n    ;\n}";
    return(eval($code) or die $@);
}

sub build_row_decoder {
    my ($types)= @_;

    # $_ = [count, body, dest_rows]
    my $code= "sub {\n    local *OUTPUT= \$_[2];\n    my (\$byte_count, \$tmp_val);\n    for my \$row_id (1..\$_[0]) {\n        my \@row;\n";

    my $i= 0;
    for my $type (@$types) {
        if (ref $type eq 'HASH') { $type= $type->{type}; }
        my $t= $lookup{$type->[0]} or die "Unknown type $type->[0]";
        my ($c, $prep)= $t->[1]('$tmp_val', $type->[1]);

        $code .= '        $byte_count= unpack("l>", substr $_[1], 0, 4, "");'."\n";
        $code .= '        if ($byte_count >= 0) {'."\n";
        $code .= '            $tmp_val= substr $_[1], 0, $byte_count, "";'."\n";
        $code .= '            '.$prep.';'."\n" if $prep;
        $code .= '            push @row, ('.$c.');'."\n";
        $code .= '        } else { push @row, undef; }'."\n";
        $i++;
    }

    $code .= "        push \@OUTPUT, \\\@row;\n    }\n}";
    return(eval($code) or die $@);
}

1;
