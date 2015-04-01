package DBD::Cassandra::db;
use v5.14;
use warnings;

use DBD::Cassandra::Protocol qw/:all/;
use DBD::Cassandra::Type qw/build_row_encoder build_row_decoder/;

# This cargocult comes straight from DBI::DBD docs. No idea what it does.
$DBD::Cassandra::db::imp_data_size = 0;

sub prepare {
    my ($dbh, $statement, @attribs)= @_;

    {
        my $body= pack_longstring($statement);
        send_frame( $dbh->{cass_connection}, 2, 0, 1, Protocol::CassandraCQL::OPCODE_PREPARE, $body )
            or return $dbh->set_err($DBI::stderr, "Failed to send frame: $!");
    }

    my ($prepared_id, $metadata, $result_metadata, @names, $paramcount);

    {
        my ($version, $flags, $streamid, $opcode, $body)= recv_frame($dbh->{cass_connection});
        if ($opcode == OPCODE_ERROR) {
            my ($code, $message)= unpack('Nn/a', $body);
            return $dbh->set_err($DBI::stderr, "$code: $message");

        } elsif ($opcode == OPCODE_RESULT) {
            my $kind= unpack('N', substr $body, 0, 4, '');
            if ($kind != RESULT_PREPARED) {
                return $dbh->set_err($DBI::stderr, "Server returned an unknown response");
            }

            $prepared_id= unpack_shortbytes($body);
            $metadata= unpack_metadata($body);
            $result_metadata= unpack_metadata($body);
            $paramcount= 0+ @{ $metadata->{columns} };
            @names= map { $_->{name} } @{$result_metadata->{columns}};

        } else {
            return $dbh->set_err($DBI::stderr, "Unknown response from server");
        }
    }

    my ($outer, $sth)= DBI::_new_sth($dbh, { Statement => $statement });
    $sth->STORE('NUM_OF_PARAMS', $paramcount);
    $sth->STORE('NUM_OF_FIELDS', 0+@names);
    $sth->{NAME}= \@names;

    $sth->{cass_params}= [];
    $sth->{cass_prepared_metadata}= $metadata;
    $sth->{cass_prepared_result_metadata}= $result_metadata;
    $sth->{cass_prepared_id}= $prepared_id;
    $sth->{cass_row_encoder}= build_row_encoder($metadata->{columns});
    $sth->{cass_row_decoder}= build_row_decoder($result_metadata->{columns});
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
    return $dbh->SUPER::FETCH($attr);
}

sub disconnect {
    my ($dbh)= @_;
    $dbh->STORE('Active', 0);

    close $dbh->{cass_connection};
}

1;
