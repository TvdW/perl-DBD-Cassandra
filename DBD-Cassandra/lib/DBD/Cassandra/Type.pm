package DBD::Cassandra::Type;
use v5.14;
use warnings;

require Exporter;
our @ISA= 'Exporter';

my %lookup= (
    0  => [\&p2c_string, \&c2p_string, 'TYPE_CUSTOM'],
    1  => [\&p2c_string, \&c2p_string, 'TYPE_ASCII'],
    2  => [\&p2c_bigint, \&c2p_bigint, 'TYPE_BIGINT'],
    3  => [\&p2c_string, \&c2p_string, 'TYPE_BLOB'],
    4  => [\&p2c_bool,   \&c2p_bool,   'TYPE_BOOLEAN'],
    5  => [\&not_impl,   \&not_impl,   'TYPE_COUNTER'],
    6  => [\&not_impl,   \&not_impl,   'TYPE_DECIMAL'],
    7  => [\&p2c_double, \&c2p_double, 'TYPE_DOUBLE'],
    8  => [\&p2c_float,  \&c2p_float,  'TYPE_FLOAT'],
    9  => [\&p2c_int,    \&c2p_int,    'TYPE_INT'],
    10 => [\&p2c_string, \&c2p_string, 'TYPE_TEXT'],
    11 => [\&not_impl,   \&not_impl,   'TYPE_TIMESTAMP'],
    12 => [\&not_impl,   \&not_impl,   'TYPE_UUID'],
    13 => [\&p2c_string, \&c2p_string, 'TYPE_VARCHAR'],
    14 => [\&not_impl,   \&not_impl,   'TYPE_VARINT'],
    15 => [\&not_impl,   \&not_impl,   'TYPE_TIMEUUID'],
    16 => [\&not_impl,   \&not_impl,   'TYPE_INET'],
);

sub not_impl { ... }
sub _pack {
    my ($p, $l, $i)= @_;
    return "pack('l> $p', $l, \$_[$i])";
}
sub _unpack {
    my ($p, $l, $v)= @_;
    return "unpack('$p', $v)";
}

sub p2c_string {
    my ($i)= @_;
    return "pack('l>/a', \$_[$i])";
}
sub c2p_string { return shift }
sub p2c_bigint { return   _pack('q>', 8, @_) }
sub c2p_bigint { return _unpack('q>', 8, @_) }
sub p2c_int { return   _pack('l>', 4, @_) }
sub c2p_int { return _unpack('l>', 4, @_) }
sub p2c_bool { return   _pack('C', 1, @_) }
sub c2p_bool { return _unpack('C', 1, @_) }
sub p2c_float { return   _pack('f', 4, @_) }
sub c2p_float { return _unpack('f', 4, @_) }
sub p2c_double { return   _pack('d', 8, @_) }
sub c2p_double { return _unpack('d', 8, @_) }
#sub p2c_ { return   _pack('', , @_) }
#sub c2p_ { return _unpack('', , @_) }

our @EXPORT_OK= qw( build_row_encoder build_row_decoder );

sub build_row_encoder {
    my ($types)= @_;

    return sub{''} unless @$types;

    my $count= 0+@$types;

    my $code= "my \$null= pack('l>', -1);\nsub {\n    return\n";
    my $i= 0;
    for my $type (@$types) {
        if (ref $type) { $type= $type->{type}; }
        my $t= $lookup{$type} or die "Unknown type $type";
        $code .= "        (defined \$_[$i] ? (".$t->[0]($i).") : \$null) .\n";
        $i++;
    }
    $code = substr($code, 0, -3). "\n    ;\n}";
    return eval($code);
}

sub build_row_decoder {
    my ($types)= @_;
    my $count= 0+@$types;

    my $code= "sub {\n    my (\@row, \$a, \$b);\n";

    my $i= 0;
    for my $type (@$types) {
        if (ref $type) { $type= $type->{type}; }
        my $t= $lookup{$type} or die "Unknown type $type";
        $code .= '    $a= unpack("l>", substr $_[0], 0, 4, "");'."\n";
        $code .= '    if ($a >= 0) {'."\n";
        $code .= '        $b= substr $_[0], 0, $a, "";'."\n";
        $code .= '        push @row, ('.$t->[1]('$b').');'."\n";
        $code .= '    } else { push @row, undef; }'."\n";
        $i++;
    }

    $code .= "    return \\\@row;\n}";
    return eval($code);
}

1;
