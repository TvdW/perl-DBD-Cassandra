use 5.010;
use warnings;
use strict;
use File::Basename qw//; use lib File::Basename::dirname(__FILE__).'/lib';
use TestCassandra;
use Test::More;

plan skip_all => "Missing Cassandra test environment" unless TestCassandra->is_ok;
plan tests => 2;

my $dbh= TestCassandra->get(";keyspace=dbd_cassandra_tests");
ok($dbh);

$dbh->do('drop table if exists test_batch');
ok($dbh->do("create table test_batch (
    pk int,
    value int,
    primary key ( pk )
)"));

SCOPED: {
    local $SIG{__WARN__}= sub { ok(0); print STDERR @_; };

    my $sth= $dbh->prepare("BEGIN BATCH
        INSERT INTO test_batch (pk, value)
        VALUES (?, ?)
        IF NOT EXISTS
        UPDATE test_batch
        SET value = 5
        WHERE pk = ?
        APPLY BATCH;",
    );
    $sth->execute(1, 2, 1);
}

$dbh->disconnect;
