use 5.008;
use warnings;
use strict;
use DBI;
use Test::More;

unless ($ENV{CASSANDRA_HOST}) {
    plan skip_all => "CASSANDRA_HOST not set";
}

plan tests => 2;

my $keyspace= "dbd_cassandra_tests";

{
    my $dbh= DBI->connect("dbi:Cassandra:host=$ENV{CASSANDRA_HOST}", $ENV{CASSANDRA_USER}, $ENV{CASSANDRA_AUTH}, {RaiseError => 1});
    ok($dbh);
    $dbh->disconnect;
}
{
    eval {
        my $dbh= DBI->connect("dbi:Cassandra:host=$ENV{CASSANDRA_HOST};cql_version=1.2.3", $ENV{CASSANDRA_USER}, $ENV{CASSANDRA_AUTH}, {RaiseError => 1});
        ok(0);
    } or do {
        ok(1);
    };
}
