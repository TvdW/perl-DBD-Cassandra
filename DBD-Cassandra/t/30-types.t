use v5.14;
use DBI;
use Test::More;
use utf8;

my $input= "__as__input";
my $warn= "__warn";

my $type_table= [
    # Type name, test input, test output (undef for error, $input for copying the input, $warn if we expect a perl warning)
    ['ascii',       'asd',  $input],
    ['ascii',       '∫∫',   undef],
    ['bigint',      5,      $input],
    ['bigint',      'asd',  $warn],
    ['blob',        'asd',  $input],
    ['boolean',     1,      $input],
    ['boolean',     0,      $input],
    ['boolean',     2,      1],
    ['boolean',     'asd',  1],
    ['double',      0.15,   $input],
    ['float',       0.2,    0.200000002980232], # Yeah.
    ['int',         5,      $input],
    ['text',        '∫∫',   $input],
    ['timestamp',   time(), $input],
    ['varchar',     '∫∫',   $input],
];

unless ($ENV{CASSANDRA_HOST}) {
    plan skip_all => "CASSANDRA_HOST not set";
}

plan tests => 1+@$type_table;

my $dbh= DBI->connect("dbi:Cassandra:host=$ENV{CASSANDRA_HOST};keyspace=dbd_cassandra_tests", undef, undef, {RaiseError => 1});
ok($dbh);

for my $type (@$type_table) {
    my ($typename, $test_val, $output_val)= @$type;
    $dbh->do("create table if not exists test_type_$typename (id bigint primary key, test $typename)");
    my $random_id= sprintf '%.f', rand(10000);
    eval {
        my $did_warn;
        local $SIG{__WARN__}= sub { $did_warn= 1; };

        $dbh->do("insert into test_type_$typename (id, test) values (?, ?)", undef, $random_id, $test_val);
        my $row= $dbh->selectrow_arrayref("select test from test_type_$typename where id=$random_id");
        if (!defined $output_val) {
            ok(0);
        } elsif ($output_val eq $warn) {
            ok($did_warn);
        } elsif ($output_val eq $input) {
            is($row->[0], $test_val, "input match $typename");
        } else {
            is($row->[0], $output_val, "perfect match $typename");
        }
        1;
    } or do {
        ok(!defined $output_val, "$typename raise error");
    };
}
$dbh->disconnect;
