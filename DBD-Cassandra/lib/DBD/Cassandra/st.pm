package DBD::Cassandra::st;
use v5.14;
use warnings;

use DBD::Cassandra::Protocol qw/:all/;

# Documentation-driven cargocult
$DBD::Cassandra::st::imp_data_size = 0;

sub bind_param {
    my ($sth, $pNum, $val, $attr)= @_;
    my $params= $sth->{cass_params};
    $params->[$pNum-1] = $val;
    1;
}

sub execute {
    my ($sth, @bind_values)= @_;

    my $dbh= $sth->{Database};

    $sth->finish if $sth->FETCH('Active');

    my $params= @bind_values ? \@bind_values : $sth->{cass_params};
    my $param_count = $sth->FETCH('NUM_OF_PARAMS');
    return $sth->set_err($DBI::stderr, "Wrong number of parameters")
        if @$params != $param_count;

    my $data;
    eval {
        $data= _cass_execute($sth, $params);
        1;
    } or do {
        my $err= $@ || "unknown error";
        return $sth->set_err($DBI::stderr, "error in execute: $err");
    };

    $sth->{cass_data}= $data;
    $sth->{cass_rows}= 0+@$data;

    $sth->STORE('Active', 1);
    @$data || '0E0'; # Something true
}

sub _cass_execute {
    my ($sth, $params)= @_;

    my $prepared_id= $sth->{cass_prepared_id};

    my $dbh= $sth->{Database};
    my $conn= $dbh->{cass_connection};

    my $values= pack('n', 0+@$params). ($sth->{cass_row_encoder}->(@$params));
    my $request_body= pack_shortbytes($prepared_id).pack_parameters({
        values => $values,
        consistency => $sth->{cass_consistency},
    });

    my ($opcode, $body)= $conn->request(
        OPCODE_EXECUTE,
        $request_body
    );

    if ($opcode == OPCODE_ERROR) {
        my ($code, $message)= unpack('Nn/a', $body);
        die "Code $code: $message";
    } elsif ($opcode != OPCODE_RESULT) {
        die "Strange answer from server during execute";
    }

    my $kind= unpack 'N', substr $body, 0, 4, '';
    if ($kind == RESULT_VOID || $kind == RESULT_SET_KEYSPACE || $kind == RESULT_SCHEMA_CHANGE) {
        return [];
    } elsif ($kind != RESULT_ROWS) {
        die 'Unsupported response from server';
    }


    my $metadata= unpack_metadata($body);
    my $decoder= $sth->{cass_row_decoder};
    my $rows_count= unpack('N', substr $body, 0, 4, '');

    my @rows;
    for my $row (1..$rows_count) {
        push @rows, $decoder->($body);
    }
    return \@rows;
}

sub execute_for_fetch {
    ... #TODO
}

sub bind_param_array {
    ... #TODO
}

sub fetchrow_arrayref {
    my ($sth)= @_;
    my $data= $sth->{cass_data};
    my $row= shift @$data;
    if (!$row) {
        $sth->STORE('Active', 0);
        return undef;
    }
    if ($sth->FETCH('ChopBlanks')) {
        map { $_ =~ s/\s+$//; } @$row;
    }
    return $sth->_set_fbav($row);
}

*fetch = \&fetchrow_arrayref; # DBI requires this. Probably historical reasons.

sub rows { shift->{cass_rows} }

sub DESTROY {
    my ($sth)= @_;
    $sth->finish if $sth->FETCH('Active');
}

1;
