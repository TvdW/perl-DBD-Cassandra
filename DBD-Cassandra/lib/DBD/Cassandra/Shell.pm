package DBD::Cassandra::Shell;
use v5.14;
use warnings;

use Term::ReadLine;
use List::Util qw/max/;
use Time::HiRes qw//;

sub run {
    my ($class, $dbh, %args)= @_;

    delete $args{dbh}; # Just making sure
    my $self= bless { dbh => $dbh, %args }, $class;
    $self->_run;
}

sub _run {
    my ($self)= @_;

    my $hist_file= $self->{hist_file} // "$ENV{HOME}/.plcqlsh";

    my $term= Term::ReadLine->new('plcqlsh');
    my $out= $term->OUT || \*STDOUT;

    {
        open my $hist_fh, "<", $hist_file or next;
        $term->addhistory($_ =~ s/\A\s*(.*?)\s*\z/$1/r) for <$hist_fh>;
        close $hist_fh;
    }

    my $no_history;
    open my $history_fh, '>>', $hist_file or do { $no_history= 1; warn $! };

    QUERY: while (defined (my $query= $term->readline($self->{prompt} // "plcqlsh> "))) {
        if ($query =~ /\S/) {
            $term->addhistory($query);
            print $history_fh "$query\n";
        } else {
            next;
        }

        my ($result, $sth, $time);
        eval {
            local $SIG{__DIE__};
            my $t0= -Time::HiRes::time();
            $sth= $self->{dbh}->prepare($query, { per_page => 100 });
            $result= $sth->execute;
            $time= Time::HiRes::time() + $t0;
            1;
        } or do {
            #printf $out "Error: $@\n";
            next QUERY;
        };

        my @names= @{ $sth->{NAME} };
        my $max_name_length= max map { length $_ } @names;
        my $count= 0;
        while (my $row= $sth->fetchrow_arrayref()) {
            for my $i (0..$#names) {
                printf $out "%${max_name_length}s : %s\n", $names[$i], ($row->[$i] // 'NULL');
            }
            printf $out "\n\n";
            $count++;
        }

        printf $out "Query OK, %d results in %.2f seconds\n\n", $count, $time;
    }

    close $history_fh;
}

1;
