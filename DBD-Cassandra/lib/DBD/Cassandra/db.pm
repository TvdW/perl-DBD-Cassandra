package DBD::Cassandra::db;
use v5.14;
use warnings;

use DBD::Cassandra::Protocol qw/:all/;
use DBD::Cassandra::Type qw/build_row_encoder build_row_decoder/;

# This cargocult comes straight from DBI::DBD docs. No idea what it does.
$DBD::Cassandra::db::imp_data_size = 0;

sub prepare {
    my ($dbh, $statement, $attribs)= @_;

    my $now= time();
    my $cache= ($dbh->{cass_prepare_cache} //= {});
    my $prepared= $cache->{$statement};
    undef $prepared if $prepared && $prepared->[0] < $now - 60;

    $prepared //= ($cache->{$statement}= do {
        # Might as well clean the prepare cache now, by expiring the old things
        delete $cache->{$_} for grep { $cache->{$_}[0] < $now - 60 } keys %$cache;

        # Actually prepare the query
        my ($opcode, $body);
        eval {
            ($opcode, $body)= $dbh->{cass_connection}->request(
                OPCODE_PREPARE,
                pack_longstring($statement),
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

        if ($body) {
            die "Failed to parse Cassandra's prepare() output";
        }

        [$now, {
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
    $prepared->[0]= $now;

    my ($outer, $sth)= DBI::_new_sth($dbh, { Statement => $statement });

    # Copy our prepared statement
    my $tmp_attr= $prepared->[1];
    @$sth{qw/cass_prepared_metadata cass_prepared_result_metadata cass_prepared_id cass_row_encoder cass_row_decoder NAME/}=
        @$tmp_attr{qw/cass_prepared_metadata cass_prepared_result_metadata cass_prepared_id cass_row_encoder cass_row_decoder NAME/};
    $sth->STORE("NUM_OF_FIELDS", $tmp_attr->{NUM_OF_FIELDS});
    $sth->STORE("NUM_OF_PARAMS", $tmp_attr->{NUM_OF_PARAMS});

    $sth->{cass_params}= [];
    $sth->{cass_consistency}= $attribs->{consistency} // $attribs->{Consistency} // $dbh->{cass_consistency} // 'one';
    $sth->{cass_async}= $attribs->{async};
    $sth->{cass_paging}= $attribs->{perpage} // $attribs->{PerPage} // $attribs->{per_page};
    return $outer;
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
    if ($attr =~ m/\Acass_/) {
        $dbh->{$attr}= $val;
        return 1;
    }
    return $dbh->SUPER::STORE($attr, $val);
}

sub FETCH {
    my ($dbh, $attr)= @_;
    return 1 if $attr eq 'AutoCommit';
    return $dbh->{$attr} if $attr =~ m/\Acass_/;

    # Sort of a workaround for unrecoverable errors in st.pm
    if ($attr eq 'Active') {
        if ($dbh->SUPER::FETCH($attr)) {
            if (!$dbh->{cass_connection} || !$dbh->{cass_connection}{Active}) {
                $dbh->disconnect;
                return 0;
            } else {
                return 1;
            }
        }
    }
    return $dbh->SUPER::FETCH($attr);
}

sub disconnect {
    my ($dbh)= @_;
    $dbh->STORE('Active', 0);

    $dbh->{cass_connection}->close;
}

sub ping {
    my ($dbh)= @_;
    return 0 unless $dbh->FETCH('Active');

    eval {
        my $conn= $dbh->{cass_connection};
        my ($opcode)= $conn->request(OPCODE_OPTIONS, '');
        die unless $opcode == OPCODE_SUPPORTED;
        1;
    } or do {
        0
    };
}

1;
