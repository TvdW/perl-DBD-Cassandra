package Cassandra::Client::AsyncAnyEvent;
use 5.010;
use strict;
use warnings;

use Time::HiRes qw();
use AnyEvent;

sub new {
    my ($class, %args)= @_;

    my $options= $args{options};

    return bless {
        timer_granularity => ($options->{timer_granularity} || 0.1),
        ae_read => {},
        ae_write => {},
        fh_to_obj => {},
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
    return;
}

sub register_read {
    my ($self, $fh)= @_;
    my $connection= $self->{fh_to_obj}{$fh} or die;

    $self->{ae_read}{$fh}= AnyEvent->io(
        poll => 'r',
        fh => $fh,
        cb => sub {
            $connection->can_read;
        },
    );

    return;
}

sub register_write {
    my ($self, $fh)= @_;
    my $connection= $self->{fh_to_obj}{$fh} or die;

    $self->{ae_write}{$fh}= AnyEvent->io(
        poll => 'w',
        fh => $fh,
        cb => sub {
            $connection->can_write;
        },
    );

    return;
}

sub unregister_read {
    my ($self, $fh)= @_;
    undef $self->{ae_read}{$fh};

    return;
}

sub unregister_write {
    my ($self, $fh)= @_;
    undef $self->{ae_write}{$fh};

    return;
}

sub deadline {
    my ($self, $fh, $id, $timeout)= @_;

    return \AE::timer( $timeout, 0, sub {
        $self->{fh_to_obj}{$fh}->can_timeout($id);
    } );
}

# $something->($async->wait(my $w)); my ($error, $result)= $w->();
sub wait {
    my ($self)= @_;
    my $output= \$_[1];

    my $cv= AE::cv;
    my @output;
    my $callback= sub {
        @output= @_;
        $cv->send;
    };

    $$output= sub {
        $cv->recv;
        return @output;
    };

    return $callback;
}

1;
