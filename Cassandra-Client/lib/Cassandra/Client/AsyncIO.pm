package Cassandra::Client::AsyncIO;
use 5.010;
use strict;
use warnings;

use Time::HiRes qw();
use vars qw/@TIMEOUTS/;

my %callback_mutex;

sub new {
    my ($class, %args)= @_;

    my $options= $args{options};

    return bless {
        timer_granularity => ($options->{timer_granularity} || 0.1),
        fh_vec_read => '',
        fh_vec_write => '',
        fh_to_obj => {},
        timeouts => [],
    }, $class;
}

sub register {
    my ($self, $fh, $connection)= @_;
    $self->{fh_to_obj}{$fh}= $connection;
    return;
}

sub unregister {
    my ($self, $fh)= @_;
    delete $self->{fh_to_obj}{$fh};
    @{$self->{timeouts}}= grep { $_->[1] != $fh } @{$self->{timeouts}} if $self->{timeouts};
    return;
}

sub register_read {
    my ($self, $fh)= @_;
    my $connection= $self->{fh_to_obj}{$fh} or die;
    vec($self->{fh_vec_read}, $fh, 1)= 1;

    return;
}

sub register_write {
    my ($self, $fh)= @_;
    my $connection= $self->{fh_to_obj}{$fh} or die;
    vec($self->{fh_vec_write}, $fh, 1)= 1;

    return;
}

sub unregister_read {
    my ($self, $fh)= @_;
    vec($self->{fh_vec_read}, $fh, 1)= 0;

    return;
}

sub unregister_write {
    my ($self, $fh)= @_;
    vec($self->{fh_vec_write}, $fh, 1)= 0;

    return;
}

sub deadline {
    my ($self, $fh, $id, $timeout)= @_;
    local *TIMEOUTS= $self->{timeouts};

    my $curtime= Time::HiRes::time;
    my $deadline= $curtime + $timeout;
    my $additem= [ $deadline, $fh, $id, 0 ];

    if (@TIMEOUTS && $TIMEOUTS[-1][0] > $deadline) {
        # Grumble... that's slow
        push @TIMEOUTS, $additem;
        @TIMEOUTS= sort { $a->[0] <=> $b->[0] } @TIMEOUTS;
    } else {
        # Common case
        push @TIMEOUTS, $additem;
    }

    return \($additem->[3]);
}

sub handle_timeouts {
    my ($self, $curtime)= @_;
    local *TIMEOUTS= $self->{timeouts};

    while (@TIMEOUTS && $curtime >= $TIMEOUTS[0][0]) {
        my $item= shift @TIMEOUTS;
        if (!$item->[3]) { # If it timed out
            my ($deadline, $fh, $id, $timedout)= @$item;
            my $obj= $self->{fh_to_obj}{$fh};
            $obj->can_timeout($id);
        }
    }

    return;
}

# $something->($async->wait(my $w)); my ($error, $result)= $w->();
sub wait {
    my ($self)= @_;
    my $output= \$_[1];

    my $done= 0;
    my @output;
    my $callback= sub {
        $done= 1;
        @output= @_;
    };

    $$output= sub {
        die 'Refusing to process Cassandra IO: already processing (bad recursion?)' if $callback_mutex{mutex};
        local $callback_mutex{mutex}= 1;

        while (!$done) {
            my ($read, $write, $error);

            my $next_timeout= @{$self->{timeouts}} ? $self->{timeouts}[0][0] : undef;
            my $next_timeout_in= $next_timeout ? ($next_timeout - Time::HiRes::time()) : undef;
            $next_timeout_in = ($next_timeout_in && $next_timeout_in > $self->{timer_granularity}) ? $next_timeout_in : $self->{timer_granularity};

            my $fh_vec_err= $self->{fh_vec_read} | $self->{fh_vec_write};
            my ($nfound)= select(
                $read= $self->{fh_vec_read},
                $write= $self->{fh_vec_write},
                $error= $fh_vec_err,
                $next_timeout_in,
            );

            if ($nfound) {
                my $lookup= $self->{fh_to_obj};
                for my $fh (keys %$lookup) {
                    if (vec($read, $fh, 1)) {
                        next unless my $obj= $lookup->{$fh};
                        $obj->can_read;
                    }
                    if (vec($write, $fh, 1)) {
                        next unless my $obj= $lookup->{$fh};
                        $obj->can_write;
                    }
                    if (vec($error, $fh, 1)) {
                        next unless my $obj= $lookup->{$fh};
                        $obj->can_read; # Good enough to trigger an error
                    }
                }
            }

            my $curtime= Time::HiRes::time;
            if ($curtime >= $next_timeout) {
                $self->handle_timeouts($curtime);
            }
        }

        return @output;
    };

    return $callback;
}

1;
