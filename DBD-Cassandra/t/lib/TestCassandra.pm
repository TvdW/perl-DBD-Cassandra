package TestCassandra;
use 5.010;
use strict;
use warnings;

use DBI;

sub is_ok {
    return !!$ENV{CASSANDRA_HOST};
}

sub get {
    my ($class, $connstr, %options)= @_;
    my $tls= $ENV{CASSANDRA_TLS} || '';
    my $port= $ENV{CASSANDRA_PORT} ? ";port=$ENV{CASSANDRA_PORT}" : "";
    my $dbh= DBI->connect(
        "dbi:Cassandra:host=$ENV{CASSANDRA_HOST};tls=$tls$port".($connstr ? $connstr : ''),
        $ENV{CASSANDRA_USER},
        $ENV{CASSANDRA_AUTH},
        {
            RaiseError => 1,
            %options,
        });
    return $dbh;
}

1;
