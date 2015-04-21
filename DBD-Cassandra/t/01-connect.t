use v5.14;
use Test::More;

unless ($ENV{CASSANDRA_HOST}) {
    plan skip_all => "CASSANDRA_HOST not set";
}

plan tests => 3;

use DBI;
my $dbh= DBI->connect("dbi:Cassandra:host=$ENV{CASSANDRA_HOST}", undef, undef, {RaiseError => 1});
ok($dbh);

is($dbh->ping(), 1);
$dbh->disconnect;

is($dbh->ping(), 0);
