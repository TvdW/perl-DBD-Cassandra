package DBD::Cassandra;
use v5.14;
use warnings;

use DBD::Cassandra::dr;
use DBD::Cassandra::db;
use DBD::Cassandra::st;

our $VERSION= '0.15';
our $drh= undef;

sub driver {
    return $drh if $drh;

    my ($class, $attr)= @_;
    $drh = DBI::_new_drh($class."::dr", {
            'Name' => 'Cassandra',
            'Version' => $VERSION,
            'Attribution' => 'DBD::Cassandra by Tom van der Woerdt',
        }) or return undef;

    return $drh;
}

sub CLONE {
    undef $drh;
}

1;

__END__

=pod

=encoding utf8

=head1 NAME

DBD::Cassandra - Database driver for Cassandra's CQL3

=head1 EXAMPLE

    use DBI;

    my $dbh = DBI->connect("dbi:Cassandra:host=localhost;keyspace=test", $user, $password, { RaiseError => 1 });
    my $rows = $dbh->selectall_arrayref("SELECT id, field_one, field_two FROM some_table");

    for my $row (@$rows) {
        # Do something with your row
    }

    $dbh->do("INSERT INTO some_table (id, field_one, field_two) VALUES (?, ?, ?)",
        { Consistency => "quorum" },
        1, "String value", 38962986
    );

    $dbh->disconnect;

=head1 DESCRIPTION

B<DBD::Cassandra> is a Perl5 Database Interface driver for Cassandra,
using the CQL3 query language.

=head2 Configuration

=over

=item Database handles

    use DBI;

    $dsn = "dbi:Cassandra:database=$database";
    $dsn = "dbi:Cassandra:keyspace=$keyspace;host=$hostname;port=$port";
    $dsn = "dbi:Cassandra:keyspace=$keyspace;consistency=local_quorum";

    my $dbh = DBI->connect($dsn, $username, $password);

=over

=item keyspace

=item database

=item db

Optionally, a keyspace to use by default. If this is not specified,
all queries must include the keyspace name.

=item hostname

Hostname to connect to. Defaults to C<localhost>

=item port

Port number to connect to. Defaults to C<9042>

=item compression

The compression method we should use for the connection. Currently
Cassandra allows C<lz4> and C<snappy>. Defaults to the algorithm with
the best compression ratio, if the server supports it. Compression can
be disabled by setting C<compression=none>.

Only used for data frames longer than 512 bytes, smaller frames get
sent uncompressed.

=item cql_version

There are several versions of the CQL language and this option lets you
pick one. Defaults to the highest available version. Consult your
Cassandra manual to see which versions your database supports.

=item consistency

=item connect_timeout

=item read_timeout

=item write_timeout

=back

=item Statement handles

    my $sth= $dbh->prepare('SELECT "id", "field1", "field2" FROM table_name WHERE id=?', { Consistency => 'one' });

=over

=item consistency

See "consistency levels".

=item per_page

Cassandra supports pagination through result sets, to avoid having the entire
result set in memory.

    my $sth = $dbh->prepare('SELECT id FROM tablename', { PerPage => 1000 });
    $sth->execute;
    while (my $row = $sth->fetchrow_arrayref()) {
        print "$row->[0]\n";
    }

It is important to keep in mind that this mode can cause errors while fetching
rows, as extra queries may be executed by the driver internally.

=item retries

Allows specifying how many times to retry queries that failed because of a
timeout. Defaults to C<0> (no retrying).

=back

=back

=head1 CONSISTENCY LEVELS

    $dbh->do("INSERT INTO some_table (id, field_name) VALUES (?, ?)",
        { Consistency => "quorum" },
        @values
    );

B<DBD::Cassandra> accepts a I<Consistency> attribute for statements.
Supported consistency levels are C<any>, C<one>, C<two>, C<three>,
C<quorum>, C<all>, C<local_quorum>, C<each_quorum>, C<serial>,
C<local_serial> and C<local_one>.

This attribute is ignored on statements that do not support it, such
as C<CREATE>.

A global consistency level can be defined as part of the DSN.

=head1 CAVEATS, BUGS, TODO

=over

=item *

There is currently no support for transactions. C<begin_work> will die
if you try to use it.

=item *

Thread support is untested. Use at your own risk.

=item *

There is currently no support for asynchronous queries, and there are
no plans to implement it. If you need to run a lot of queries in
parallel, consider using C<fork> to manage the parallel work.

=item *

If the table structure changes, prepared queries are not invalidated correctly.
This is a serious issue and will be fixed in a future release.

=item *

Not all Cassandra data types are supported. These are currently
supported:

=over

=item * ascii

=item * bigint

=item * blob

=item * boolean

=item * custom

=item * double

=item * float

=item * int

=item * text

=item * timestamp

=item * varchar

=back

=item *

Cassandra/CQL3 is strict about the queries you write. When switching
from other databases, such as MySQL, this may come as a surprise. This
module supports C<quote(..)>, but try to use prepared statements
instead. They will save you a lot of trouble.

=back

=head1 LICENSE

This module is released under the same license as Perl itself.

=head1 AUTHORS

Tom van der Woerdt, L<tvdw@cpan.org|mailto:tvdw@cpan.org>
