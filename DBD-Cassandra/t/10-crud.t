use 5.010;
use warnings;
use strict;
use File::Basename qw//; use lib File::Basename::dirname(__FILE__).'/lib';
use TestCassandra;
use Test::More;

plan skip_all => "Missing Cassandra test environment" unless TestCassandra->is_ok;
plan tests => 9;

my $dbh= TestCassandra->get(undef, Warn => 1, PrintWarn => 0, PrintError => 0);
ok($dbh);

my $keyspace= "dbd_cassandra_tests";

ok(!eval {
    # Invalid: can't use prepared statements here
    $dbh->do('drop keyspace if exists ?', undef, $keyspace);
});

$dbh->do("drop keyspace if exists $keyspace");
$dbh->do("create keyspace $keyspace with replication={'class': 'SimpleStrategy', 'replication_factor': 1}");

ok(!eval {
    # Invalid: no keyspace selected
    $dbh->do("create table test_int (id bigint primary key)");
});

$dbh->do("create table $keyspace.test_int (id bigint primary key)");

my $last_res;
for (1..5) {
    $last_res= $dbh->do("insert into $keyspace.test_int (id) values (?)", undef, $_);
}
is($last_res, '0E0');

for my $row (@{ $dbh->selectall_arrayref("select count(*) from $keyspace.test_int") }) {
    is($row->[0], 5);
}

ok(!eval {
    # Can't have a string in an integer
    $dbh->do("insert into $keyspace.test_int (id) values 'test'");
});

$dbh->do("delete from $keyspace.test_int where id in (?,?,3)", undef, 1, 2);
for my $row (@{ $dbh->selectall_arrayref("select count(*) from $keyspace.test_int") }) {
    is($row->[0], 2);
}

ok($dbh->do("insert into $keyspace.test_int (id) values (6)", { Consistency => 'any' }));
ok(!eval {
    # We have replication_factor=1, 'two' shouldn't work
    $dbh->do("insert into $keyspace.test_int (id) values (5)", { Consistency => 'two' });
});

$dbh->disconnect;
