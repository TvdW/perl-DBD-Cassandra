package DBD::Cassandra::db;
use v5.14;
use warnings;

use DBD::Cassandra::Protocol qw/:all/;
use DBD::Cassandra::Type qw/build_row_encoder build_row_decoder/;

# This cargocult comes straight from DBI::DBD docs. No idea what it does.
$DBD::Cassandra::db::imp_data_size = 0;

sub prepare {
    my ($dbh, $statement, $attribs)= @_;

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
    $sth->{cass_consistency}= $attribs->{consistency} // $attribs->{Consistency} // $dbh->{cass_consistency} // 'one';
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

    $dbh->{cass_connection}->close;
}

1;
