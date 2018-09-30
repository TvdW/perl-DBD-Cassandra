use 5.010;
use warnings;
use strict;
use File::Basename qw//; use lib File::Basename::dirname(__FILE__).'/lib';
use TestCassandra;
use Test::More;

plan skip_all => "Missing Cassandra test environment" unless TestCassandra->is_ok;
plan tests => 2;

my $keyspace= "dbd_cassandra_tests";

{
    my $dbh= TestCassandra->get;
    ok($dbh);
    $dbh->disconnect;
}
{
    eval {
        my $dbh= TestCassandra->get(";cql_version=1.2.3");
        ok(0);
    } or do {
        ok(1);
    };
}
