use 5.010;
use warnings;
use strict;
use File::Basename qw//; use lib File::Basename::dirname(__FILE__).'/lib';
use Test::More;
use TestCassandra;

# Fake running under 'perl -l'
$\= "\n";

plan skip_all => "Missing Cassandra test environment" unless TestCassandra->is_ok;
plan tests => 3;

my $dbh= TestCassandra->get;
ok($dbh);

is($dbh->ping(), 1);
$dbh->disconnect;

is($dbh->ping(), 0);
