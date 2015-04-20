package DBD::Cassandra::db;
use v5.14;
use warnings;

use DBD::Cassandra::Protocol qw/:all/;
use DBD::Cassandra::Type qw/build_row_encoder build_row_decoder/;

# This cargocult comes straight from DBI::DBD docs. No idea what it does.
$DBD::Cassandra::db::imp_data_size = 0;

sub prepare {
    my ($dbh, $statement, $attribs)= @_;

    prune_prepare_cache($dbh->{cass_prepare_cache});

    my $prepared= ($dbh->{cass_prepare_cache}{$statement} //= do {
        my ($opcode, $body);
        eval {
            ($opcode, $body)= $dbh->{cass_connection}->request(
                OPCODE_PREPARE,
                pack_longstring($statement)
            );
            1;
        } or do {
            my $error= $@ || "unknown error";
            return $dbh->set_err($DBI::stderr, "prepare failed: $error");
        };

        if ($opcode != OPCODE_RESULT) {
            return $dbh->set_err($DBI::stderr, "Unknown response from server");
        }

        my $kind= unpack('N', substr $body, 0, 4, '');
        if ($kind != RESULT_PREPARED) {
            return $dbh->set_err($DBI::stderr, "Server returned an unknown response");
        }

        my $prepared_id= unpack_shortbytes($body);
        my $metadata= unpack_metadata($body);
        my $result_metadata= unpack_metadata($body);
        my $paramcount= 0+ @{ $metadata->{columns} };
        my @names= map { $_->{name} } @{$result_metadata->{columns}};

        [time(), {
            cass_prepared_metadata => $metadata,
            cass_prepared_result_metadata => $result_metadata,
            cass_prepared_id => $prepared_id,
            cass_row_encoder => build_row_encoder($metadata->{columns}),
            cass_row_decoder => build_row_decoder($result_metadata->{columns}),
            NAME => \@names,
            NUM_OF_FIELDS => 0+@names,
            NUM_OF_PARAMS => $paramcount,
        }]
    });
    $prepared->[0]= time();

    my ($outer, $sth)= DBI::_new_sth($dbh, { Statement => $statement });
    for my $key (keys %{$prepared->[1]}) {
        $key =~ /^NUM_/
            ? $sth->STORE($key, $prepared->[1]{$key})
            : ($sth->{$key}= $prepared->[1]{$key});
    }
    $sth->{cass_params}= [];
    $sth->{cass_consistency}= $attribs->{consistency} // $attribs->{Consistency} // $dbh->{cass_consistency} // 'one';
    return $outer;
}

sub prune_prepare_cache {
    my ($cache)= @_;
    my $expiration= time() - 60;

    %$cache = map { $_ => $cache->{$_} } grep { $cache->{$_} && $cache->{$_}[0] >= $expiration } keys %$cache;
}

sub commit {
    my ($dbh)= @_;
    if ($dbh->FETCH('Warn')) {
        warn "Commit ineffective while AutoCommit is on";
    }
    0;
}

sub rollback {
    my ($dbh)= @_;
    if ($dbh->FETCH('Warn')) {
        warn "Rollback ineffective while AutoCommit is on";
    }
    0;
}

sub STORE {
    my ($dbh, $attr, $val)= @_;
    if ($attr eq 'AutoCommit') {
        if (!$val) { die "DBD::Cassandra does not yet support transactions"; }
        return 1;
    }
    if ($attr =~ m/^cass_/) {
        $dbh->{$attr}= $val;
        return 1;
    }
    return $dbh->SUPER::STORE($attr, $val);
}

sub FETCH {
    my ($dbh, $attr)= @_;
    return 1 if $attr eq 'AutoCommit';
    return $dbh->{$attr} if $attr =~ m/^cass_/;
    if ($attr eq 'Active') {
        return $dbh->SUPER::FETCH($attr) && $dbh->{cass_connection} && $dbh->{cass_connection}{Active};
    }
    return $dbh->SUPER::FETCH($attr);
}

sub disconnect {
    my ($dbh)= @_;
    $dbh->STORE('Active', 0);

    $dbh->{cass_connection}->close;
}

1;
