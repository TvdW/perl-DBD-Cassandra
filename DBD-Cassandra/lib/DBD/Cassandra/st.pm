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

    return $sth->set_err($DBI::stderr, sprintf "Wrong number of parameters. Expected %d, got %d", $param_count, 0+@$params)
        if @$params != $param_count;

    $sth->{cass_paging_state}= undef;
    $sth->{cass_params_real}= $params;

    unless (my $success= cass_post($sth)) {
        return $success;
    }

    if ($sth->{cass_async}) {
        return '0E0'; # We don't really know whether it worked or not...
    } else {
        return cass_read($sth);
    }
}

sub cass_post {
    my ($sth)= @_;

    # Make sure we don't have an existing open request
    finish_async($sth);

    eval {
        my $params= $sth->{cass_params_real};
        my $prepared_id= $sth->{cass_prepared_id};

        my $dbh= $sth->{Database};
        my $conn= $dbh->{cass_connection};

        my $request_body= pack_parameters({
            prepare_id       => $sth->{cass_prepared_id},
            values           => $sth->{cass_row_encoder}->($params),
            consistency      => $sth->{cass_consistency},
            result_page_size => $sth->{cass_paging},
            paging_state     => $sth->{cass_paging_state},
        });

        my ($stream_id)= $conn->post_request(
            OPCODE_EXECUTE,
            $request_body,
        );

        $sth->{cass_pending_stream_id}= $stream_id;
        1;
    } or do {
        my $err= $@ || "unknown error";
        return $sth->set_err($DBI::stderr, "post error in execute: $err");
    };

    $sth->{cass_data}= undef;
    $sth->{cass_rows}= undef;

    $sth->STORE('Active', 1);
    1;
}

sub cass_read {
    my ($sth)= @_;

    my $data;
    eval {
        my $dbh= $sth->{Database};
        my $conn= $dbh->{cass_connection};
        my ($opcode, $body)= $conn->read_request(delete $sth->{cass_pending_stream_id});

        if ($opcode != OPCODE_RESULT) {
            die "Strange answer from server during execute";
        }

        my $kind= unpack 'N', substr $body, 0, 4, '';
        if ($kind == RESULT_VOID || $kind == RESULT_SET_KEYSPACE || $kind == RESULT_SCHEMA_CHANGE) {
            $data= [];
            $sth->STORE('Active', 0);
            return 1;
        } elsif ($kind != RESULT_ROWS) {
            die 'Unsupported response from server';
        }

        my $metadata= unpack_metadata($body);
        $sth->{cass_paging_state}= $metadata->{paging_state};
        my $decoder= $sth->{cass_row_decoder};
        my $rows_count= unpack('N', substr $body, 0, 4, '');

        # No rows, no paging, that means we're done
        if (!$rows_count && !$sth->{cass_paging_state}) {
            $sth->STORE('Active', 0);
        }

        $sth->{cass_row_decoder}->($rows_count, $body, ($data = []));
        1;

    } or do {
        my $err= $@ || "unknown error";
        $sth->STORE('Active', 0);
        return $sth->set_err($DBI::stderr, "read error in execute: $err");
    };

    $sth->{cass_data}= $data;
    $sth->{cass_rows}= 0+@$data;

    if ($sth->{cass_paging} || $sth->{cass_async}) {
        return '0E0'; # We don't know how many rows will be returned.
    } else {
        return (@$data || '0E0'); # Something true
    }
}

sub finish_async {
    my ($sth)= @_;

    if ($sth->{cass_pending_stream_id}) {
        if ($sth->{cass_async}) {
            return cass_read($sth);
        } else {
            return $sth->set_err($DBI::stderr, "DBD::Cassandra BUG: pending stream, but no async?")
        }
    }

    '0E0';
}
*x_finish_async= \&finish_async;

sub execute_for_fetch {
    ... #TODO
}

sub bind_param_array {
    ... #TODO
}

sub fetchrow_arrayref {
    my ($sth)= @_;
    finish_async($sth) or return undef;

    my $cass_data= $sth->{cass_data};
    my $row= shift @$cass_data;
    if (!$row) {
        if ($sth->{cass_paging_state}) {
            # Fetch some more rows
            cass_post($sth) or return undef;
            cass_read($sth) or return undef;
            $cass_data= $sth->{cass_data};
            $row= shift @$cass_data;
        }
    }
    if (!$row) {
        $sth->STORE('Active', 0);
        return undef;
    }
    if (!@$cass_data && !$sth->{cass_paging_state}) { # This is our last row
        $sth->STORE('Active', 0);
    }
    if ($sth->FETCH('ChopBlanks')) {
        map { $_ =~ s/\s+$//; } @$row;
    }
    return $sth->_set_fbav($row);
}

*fetch = \&fetchrow_arrayref; # DBI requires this. Probably historical reasons.

sub rows {
    my $sth= shift;
    if ($sth->{cass_paging}) {
        return '0E0';
    } else {
        return undef unless finish_async($sth);
        return $sth->{cass_rows};
    }
}

sub DESTROY {
    my ($sth)= @_;
    finish_async($sth);

    # This fixes an issue where DBI throws a warning for an 'insert into .. if not exists update ..',
    # which (interestingly) returns rows
    $sth->finish if $sth->FETCH('Active') && !$sth->FETCH('NUM_OF_FIELDS');
}

1;
