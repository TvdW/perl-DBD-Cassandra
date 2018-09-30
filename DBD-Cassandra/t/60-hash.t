use 5.010;
use warnings;
use strict;
use File::Basename qw//; use lib File::Basename::dirname(__FILE__).'/lib';
use TestCassandra;
use Test::More;

plan skip_all => "Missing Cassandra test environment" unless TestCassandra->is_ok;
plan tests => 4;

my $dbh= TestCassandra->get(";consistency=quorum");
ok($dbh);

my $keyspace= "dbd_cassandra_tests";

ok(!eval {
    # Invalid: can't use prepared statements here
    $dbh->do('drop keyspace if exists ?', undef, $keyspace, {Warn=>0});
});

$dbh->do("drop keyspace if exists $keyspace");
$dbh->do("create keyspace $keyspace with replication={'class': 'SimpleStrategy', 'replication_factor': 1}");
$dbh->do("create table $keyspace.test_int (id bigint primary key, val text, id2 uuid)");
$dbh->do("insert into $keyspace.test_int (id, val, id2) values (?, ?, ?)", undef, 1, "test", "12345678-1234-1234-1234-123412341234");
my $row= $dbh->selectall_arrayref("select id, val, id2 from $keyspace.test_int where id=?", {Slice=>{},async=>1}, 1)->[0];
is_deeply($row, {
    id => 1,
    val => "test",
    id2 => "12345678-1234-1234-1234-123412341234",
});

SKIP: {
    skip "Authentication not configured", 1 unless $ENV{CASSANDRA_USER};
    ok($dbh->selectall_arrayref("list authorize permission on keyspace system of $ENV{CASSANDRA_USER}", {Slice=>{}}));
}

$dbh->disconnect;
